use crate::backend::ByteStreamSource;
use crate::error::AppError;
use crate::models::EodagResponse;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::{BoxStream, StreamExt, Stream};
use object_store::{ObjectStore, ObjectStoreExt, PutMultipartOptions, PutPayload, path::Path};
use object_store::aws::AmazonS3Builder;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// ── S3 client pool ──────────────────────────────────────────────────────

/// Fingerprint of the credentials currently held by a pooled client.
/// When credentials change (e.g. new STS token), we recreate the client.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CredentialFingerprint {
    hash: u64,
}

impl CredentialFingerprint {
    fn from_parts(
        access_key: Option<&str>,
        secret_key: Option<&str>,
        token: Option<&str>,
        anon: bool,
    ) -> Self {
        let mut hasher = DefaultHasher::new();
        access_key.hash(&mut hasher);
        secret_key.hash(&mut hasher);
        token.hash(&mut hasher);
        anon.hash(&mut hasher);
        Self {
            hash: hasher.finish(),
        }
    }
}

/// A connection pool of `object_store` S3 clients.
///
/// Clients are keyed by `(endpoint_url, bucket)`.  When the credentials
/// supplied by EODAG change for an existing key, the client is replaced
/// transparently.
///
/// Eviction of unused clients is handled by `moka` (TTL + capacity).
pub struct S3ClientPool {
    /// Fast concurrent cache with TTL + LRU eviction.
    clients: moka::future::Cache<(String, String), Arc<dyn ObjectStore>>,
    /// Tracks the credential fingerprint for each cached client so we know
    /// when to replace it.
    fingerprints: RwLock<HashMap<(String, String), CredentialFingerprint>>,
}

impl S3ClientPool {
    /// Create a new pool with the given TTL and maximum capacity.
    pub fn new(ttl: Duration, max_capacity: u64) -> Self {
        let clients = moka::future::Cache::builder()
            .max_capacity(max_capacity)
            .time_to_idle(ttl)
            .build();
        Self {
            clients,
            fingerprints: RwLock::new(HashMap::new()),
        }
    }

    /// Return an `ObjectStore` client for the given S3 backend parameters.
    ///
    /// If a client already exists for the same `(endpoint_url, bucket)` and
    /// the credentials have not changed, it is returned as-is.  Otherwise a
    /// new client is created (and replaces the old one).
    pub async fn get_or_create(
        &self,
        endpoint_url: &str,
        bucket: &str,
        access_key: Option<&str>,
        secret_key: Option<&str>,
        token: Option<&str>,
        anon: bool,
        requester_pays: bool,
    ) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
        let cache_key = (endpoint_url.to_string(), bucket.to_string());
        let new_fp = CredentialFingerprint::from_parts(access_key, secret_key, token, anon);

        // Fast path: check if a client exists with matching credentials.
        {
            let fps = self.fingerprints.read().await;
            if let Some(existing_fp) = fps.get(&cache_key) {
                if *existing_fp == new_fp {
                    if let Some(client) = self.clients.get(&cache_key).await {
                        return Ok(client);
                    }
                }
            }
        }

        // Slow path: create a new client.
        let client = build_s3_client(
            endpoint_url,
            bucket,
            access_key,
            secret_key,
            token,
            anon,
            requester_pays,
        )?;
        let client: Arc<dyn ObjectStore> = Arc::new(client);

        self.clients.insert(cache_key.clone(), client.clone()).await;

        {
            let mut fps = self.fingerprints.write().await;
            fps.insert(cache_key, new_fp);
        }

        Ok(client)
    }

    /// Number of clients currently in the pool (for diagnostics / tests).
    pub async fn len(&self) -> u64 {
        self.clients.entry_count()
    }
}

/// Build an `AmazonS3` object store from explicit parameters.
fn build_s3_client(
    endpoint_url: &str,
    bucket: &str,
    access_key: Option<&str>,
    secret_key: Option<&str>,
    token: Option<&str>,
    anon: bool,
    _requester_pays: bool,
) -> Result<impl ObjectStore, object_store::Error> {
    // Validate the endpoint is a proper URL before the builder silently
    // accepts it and then panics at request time.
    validate_endpoint(endpoint_url).map_err(|msg| {
        object_store::Error::Generic {
            store: "AmazonS3",
            source: msg.into(),
        }
    })?;

    let mut builder = AmazonS3Builder::new()
        .with_endpoint(endpoint_url)
        .with_bucket_name(bucket)
        .with_allow_http(true);

    if anon {
        builder = builder.with_skip_signature(true);
    } else if let (Some(ak), Some(sk)) = (access_key, secret_key) {
        builder = builder.with_access_key_id(ak).with_secret_access_key(sk);
        if let Some(t) = token {
            builder = builder.with_token(t);
        }
    }

    builder.build()
}

/// Reject endpoints that are missing a scheme or otherwise malformed,
/// since `object_store` panics at request time for invalid URIs.
fn validate_endpoint(endpoint: &str) -> Result<(), String> {
    if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
        return Err(format!(
            "invalid S3 endpoint \"{endpoint}\": must start with http:// or https://"
        ));
    }
    endpoint
        .parse::<axum::http::Uri>()
        .map_err(|e| format!("invalid S3 endpoint \"{endpoint}\": {e}"))?;
    Ok(())
}

// ── S3 backend source ───────────────────────────────────────────────────

/// S3 backend source for streaming objects from S3.
pub struct S3BackendSource {
    s3_pool: Arc<S3ClientPool>,
    endpoint: String,
    bucket: String,
    prefix: String,
    access_key: Option<String>,
    secret_key: Option<String>,
    token: Option<String>,
    anon: bool,
    requester_pays: bool,
}

impl S3BackendSource {
    pub fn new(
        endpoint: &str,
        bucket: &str,
        prefix: Option<&str>,
        access_key: Option<&str>,
        secret_key: Option<&str>,
        token: Option<&str>,
        anon: bool,
        requester_pays: bool,
        s3_pool: Arc<S3ClientPool>,
    ) -> Self {
        Self {
            s3_pool,
            endpoint: endpoint.to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.unwrap_or("").to_string(),
            access_key: access_key.map(|s| s.to_string()),
            secret_key: secret_key.map(|s| s.to_string()),
            token: token.map(|s| s.to_string()),
            anon,
            requester_pays,
        }
    }

    /// Create from EODAG S3 response
    pub async fn from_eodag_response(
        s3_pool: Arc<S3ClientPool>,
        response: &EodagResponse,
    ) -> Result<Self, AppError> {
        match response {
            EodagResponse::S3 {
                endpoint_url,
                path,
                key,
                secret,
                token,
                anon,
                requester_pays,
            } => {
                let path = path
                    .strip_prefix("s3://")
                    .unwrap_or(path)
                    .trim_start_matches('/');
                let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
                Ok(Self {
                    s3_pool,
                    endpoint: endpoint_url.clone(),
                    bucket: bucket.to_string(),
                    prefix: prefix.to_string(),
                    access_key: key.clone(),
                    secret_key: secret.clone(),
                    token: token.clone(),
                    anon: *anon,
                    requester_pays: *requester_pays,
                })
            }
            _ => Err(AppError::Internal(
                "S3BackendSource created from non-S3 response".to_string(),
            )),
        }
    }

    async fn store(&self) -> Result<Arc<dyn ObjectStore>, AppError> {
        self.s3_pool
            .get_or_create(
                &self.endpoint,
                &self.bucket,
                self.access_key.as_deref(),
                self.secret_key.as_deref(),
                self.token.as_deref(),
                self.anon,
                self.requester_pays,
            )
            .await
            .map_err(|e| {
                AppError::BackendError(format!("failed to create S3 client: {e}"))
            })
    }

    /// Verify the bucket is reachable by listing with a limit of 1.
    /// Returns `Ok(())` if the endpoint and bucket are accessible,
    /// or an error if the bucket does not exist or the endpoint is
    /// unreachable.
    pub async fn check_bucket(&self) -> Result<(), AppError> {
        let store = self.store().await?;
        // list_with_offset with a prefix of "" and collect just 1 result.
        // If the bucket doesn't exist, this will error out.
        use futures::stream::StreamExt;
        let mut listing = store.list(Some(&Path::from("")));
        // We only need to pull one item (or confirm the stream ends).
        match listing.next().await {
            Some(Err(e)) => Err(AppError::BackendError(format!(
                "S3 bucket check failed: {e}"
            ))),
            _ => Ok(()), // empty bucket or has objects — either way bucket exists
        }
    }

    fn object_path(&self, key: Option<&str>) -> Path {
        let mut full = self.prefix.clone();
        if let Some(k) = key {
            if !full.is_empty() {
                full.push('/');
            }
            full.push_str(k);
        }
        Path::from(full)
    }

    pub async fn put_streaming<S>(
        &self,
        key: &str,
        stream: S,
        content_length: Option<u64>,
    ) -> Result<(), AppError>
    where
        S: Stream<Item = Result<Bytes, AppError>> + Send + 'static,
    {
        let store = self.store().await?;
        let path = self.object_path(Some(key));

        // If we know the object is small (<5 MiB), do a single PUT
        const MIN_MULTIPART_SIZE: u64 = 5 * 1024 * 1024;

        if let Some(len) = content_length {
            if len < MIN_MULTIPART_SIZE {
                // collect into memory and do a single put
                let mut all_bytes = Vec::with_capacity(len as usize);
                tokio::pin!(stream);
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    all_bytes.extend_from_slice(&chunk);
                }
                tracing::debug!(key = %key, size = all_bytes.len(), "uploading small object to S3 in one PUT");
                store
                    .put(&path, PutPayload::from_bytes(Bytes::from(all_bytes)))
                    .await
                    .map_err(|e| AppError::BackendError(format!("S3 put failed: {e}")))?;
                return Ok(());
            }
        }

        tracing::debug!(key = %key, "uploading large object to S3 using multipart upload");

        // Initiate multipart upload and get a writer
        let mut multipart = store
            .put_multipart_opts(&path, PutMultipartOptions::default())
            .await
            .map_err(|e| AppError::BackendError(format!("S3 put_multipart init failed: {e}")))?;

        let mut buffer = BytesMut::new();
        let mut stream = Box::pin(stream);

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.extend_from_slice(&chunk);

            if buffer.len() >= MIN_MULTIPART_SIZE as usize {
                multipart
                    .put_part(PutPayload::from_bytes(buffer.split().freeze()))
                    .await
                    .map_err(|e| AppError::BackendError(format!("S3 put_multipart write failed: {e}")))?;
            }
        }

        // Last part
        if !buffer.is_empty() {
            multipart
                .put_part(PutPayload::from_bytes(buffer.freeze()))
                .await
                .map_err(|e| AppError::BackendError(format!("S3 put_multipart write failed: {e}")))?;
        }

        // Complete the multipart upload
        multipart
            .complete()
            .await
            .map_err(|e| AppError::BackendError(format!("S3 put_multipart finish failed: {e}")))?;

        Ok(())
    }
}

#[async_trait]
impl ByteStreamSource for S3BackendSource {
    async fn exists(&self, key: Option<&str>) -> Result<bool, AppError> {
        let store = self.store().await?;

        let path = self.object_path(key);
        match store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(AppError::BackendError(format!("S3 head failed: {e}"))),
        }
    }

    async fn stream(
        &self,
        key: Option<&str>,
    ) -> Result<
        (
            BoxStream<'static, Result<Bytes, AppError>>,
            Option<String>,
            Option<u64>,
        ),
        AppError,
    > {
        let store = self.store().await?;

        let path = self.object_path(key);

        // Head to get Content-Length
        let meta = store.head(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                AppError::NotFound(format!("object not found: {}/{}", self.bucket, path))
            }
            other => AppError::BackendError(format!("S3 head failed: {other}")),
        })?;

        let content_length = Some(meta.size as u64);

        // Stream the object
        let stream = store
            .get(&path)
            .await
            .map_err(|e| AppError::BackendError(format!("S3 get failed: {e}")))?
            .into_stream()
            .map(|r| r.map_err(|e| AppError::BackendError(format!("stream error: {e}"))));

        // Guess content type from key
        let content_type = guess_content_type_from_key(meta.location.as_ref());

        Ok((Box::pin(stream), content_type, content_length))
    }
}

pub fn guess_content_type_from_key(key: &str) -> Option<String> {
    if let Some(ext) = key.rsplit('.').next() {
        match ext.to_lowercase().as_str() {
            "zarr" => return Some("application/vnd.zarr".to_string()),
            "nc" => return Some("application/x-netcdf".to_string()),
            // add more overrides here if needed
            _ => {}
        }
    }

    mime_guess::MimeGuess::from_path(key)
        .first_raw()
        .map(String::from)
        .or(Some("application/octet-stream".to_string()))
}
