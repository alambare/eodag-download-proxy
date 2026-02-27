use crate::backend::ByteStreamSource;
use crate::client_pool::S3ClientPool;
use crate::error::AppError;
use crate::models::EodagResponse;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use std::sync::Arc;

/// S3 backend source for streaming objects from S3.
pub struct S3BackendSource {
    s3_pool: Arc<S3ClientPool>,
    endpoint: String,
    bucket: String,
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
}

#[async_trait]
impl ByteStreamSource for S3BackendSource {
    async fn exists(&self, key: &str) -> Result<bool, AppError> {
        let store = self
            .s3_pool
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
            .map_err(|e| AppError::BackendError(format!("failed to create S3 client: {e}")))?;

        let path = Path::from(key);
        match store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(AppError::BackendError(format!("S3 head failed: {e}"))),
        }
    }

    async fn stream(
        &self,
        key: &str,
    ) -> Result<
        (
            BoxStream<'static, Result<Bytes, AppError>>,
            Option<String>,
            Option<u64>,
        ),
        AppError,
    > {
        let store = self
            .s3_pool
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
            .map_err(|e| AppError::BackendError(format!("failed to create S3 client: {e}")))?;

        let path = Path::from(key);

        // Head to get Content-Length
        let meta = store.head(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                AppError::NotFound(format!("object not found: {}/{}", self.bucket, key))
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
