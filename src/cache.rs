use crate::backend::ByteStreamSource;
use crate::backend::s3::S3BackendSource;
use crate::config::AppConfig;
use crate::error::AppError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, Stream};
use std::sync::Arc;

// Cache store wrapping a backend source.
pub struct S3CacheStore {
    inner: S3BackendSource,
}

#[async_trait]
impl ByteStreamSource for S3CacheStore {
    async fn exists(&self, key: Option<&str>) -> Result<bool, AppError> {
        let key = key.ok_or_else(|| AppError::Internal("Cache key required".to_string()))?;
        self.inner.exists(Some(key)).await
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
        let key = key.ok_or_else(|| AppError::Internal("Cache key required".to_string()))?;
        self.inner.stream(Some(key)).await
    }
}

impl S3CacheStore {
    /// Build a cache source from the config.
    ///
    /// Returns `None` if no cache config or missing credentials.
    pub fn build_from_config(
        config: &AppConfig,
        s3_pool: Arc<crate::client_pool::S3ClientPool>,
    ) -> Option<Arc<S3CacheStore>> {
        let cache_cfg = match config.cache.as_ref() {
            Some(cfg) => cfg,
            None => {
                tracing::info!("cache disabled (no cache config)");
                return None;
            }
        };

        let (access_key, secret_key) = match (&cache_cfg.access_key, &cache_cfg.secret_key) {
            (Some(a), Some(s)) => (a, s),
            _ => {
                tracing::info!(
                    "cache disabled (missing access_key or secret_key for bucket={})",
                    cache_cfg.bucket
                );
                return None;
            }
        };

        tracing::info!(
            "initializing S3 cache: endpoint={} bucket={}",
            cache_cfg.endpoint,
            cache_cfg.bucket
        );

        let s3_backend = S3BackendSource::new(
            &cache_cfg.endpoint,
            &cache_cfg.bucket,
            None,
            Some(access_key),
            Some(secret_key),
            None,
            false,
            false,
            s3_pool,
        );

        Some(Arc::new(S3CacheStore {
            inner: s3_backend,
        }))
    }

    pub async fn put_streaming<S>(&self, key: &str, stream: S, content_length: Option<u64>) -> Result<(), AppError>
    where
        S: Stream<Item = Result<Bytes, AppError>> + Send + 'static,
    {
        self.inner.put_streaming(key, stream, content_length).await
    }
}
