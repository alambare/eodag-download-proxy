use crate::backend::ByteStreamSource;
use crate::backend::s3::S3BackendSource;
use crate::config::AppConfig;
use crate::error::AppError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use std::sync::Arc;

// Cache store wrapping a backend source.
pub struct S3CacheStore {
    inner: Arc<dyn ByteStreamSource>,
}

#[async_trait]
impl ByteStreamSource for S3CacheStore {
    async fn exists(&self, key: &str) -> Result<bool, AppError> {
        self.inner.exists(key).await
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
        self.inner.stream(key).await
    }
}

impl S3CacheStore {
    /// Build a cache source from the config.
    ///
    /// Returns `None` if no cache config or missing credentials.
    pub fn build_from_config(
        config: &AppConfig,
        s3_pool: Arc<crate::client_pool::S3ClientPool>,
    ) -> Option<Arc<dyn ByteStreamSource>> {
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
            Some(access_key),
            Some(secret_key),
            None,
            false,
            false,
            s3_pool,
        );

        Some(Arc::new(S3CacheStore {
            inner: Arc::new(s3_backend),
        }) as Arc<dyn ByteStreamSource>)
    }
}
