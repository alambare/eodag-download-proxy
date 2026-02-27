use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Key that identifies a unique S3 backend (endpoint + bucket).
/// Note: in practice we use `(String, String)` tuples as keys in the moka cache.
/// This struct is kept for documentation purposes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(dead_code)]
struct S3ClientKey {
    endpoint_url: String,
    bucket: String,
}

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
    let mut builder = AmazonS3Builder::new()
        .with_endpoint(endpoint_url)
        .with_bucket_name(bucket)
        .with_allow_http(true); // dev convenience; restrict later

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
