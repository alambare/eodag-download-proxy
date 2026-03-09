use crate::backend::ByteStreamSource;
use crate::backend::s3::{S3BackendSource, S3ClientPool};
use crate::config::{AppConfig, CacheConfig};
use crate::error::AppError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use std::sync::Arc;
use std::time::Duration;

/// Trait abstracting cache operations so handlers don't depend on a
/// concrete S3 implementation and tests can supply lightweight mocks.
#[async_trait]
pub trait CacheStore: Send + Sync {
    async fn exists(&self, key: &str) -> Result<bool, AppError>;

    async fn get_stream(
        &self,
        key: &str,
    ) -> Result<
        (
            BoxStream<'static, Result<Bytes, AppError>>,
            Option<String>,
            Option<u64>,
        ),
        AppError,
    >;

    async fn upload(
        &self,
        key: &str,
        stream: BoxStream<'static, Result<Bytes, AppError>>,
        content_length: Option<u64>,
    ) -> Result<(), AppError>;

    /// Try to serve from cache. Returns cached data on hit, `None` on miss.
    /// Errors are logged and treated as misses.
    async fn try_get(
        &self,
        key: &str,
    ) -> Option<(BoxStream<'static, Result<Bytes, AppError>>, Option<String>, Option<u64>)> {
        match self.exists(key).await {
            Ok(true) => match self.get_stream(key).await {
                Ok(result) => {
                    tracing::info!(cache_key = %key, "cache hit");
                    Some(result)
                }
                Err(e) => {
                    tracing::warn!(error = ?e, cache_key = %key, "cache read failed, falling through to backend");
                    None
                }
            },
            Ok(false) => None,
            Err(e) => {
                tracing::warn!(error = ?e, cache_key = %key, "cache lookup failed, falling through to backend");
                None
            }
        }
    }
}

/// Build a cache store from the application config.
///
/// Returns `Ok(None)` when cache is not configured or credentials are
/// missing.  Returns `Err` when cache is configured but the endpoint is
/// unreachable so the caller can abort startup instead of silently
/// degrading.
pub async fn build_from_config(config: &AppConfig) -> Result<Option<Arc<dyn CacheStore>>, AppError> {
    S3CacheStore::build_from_config(config).await
}

// ── Path-based cache resolution ─────────────────────────────────────────

/// Return the cache store when caching is enabled *and* `request_path`
/// matches the configured `cache_path_prefixes` (empty list = match all).
pub fn resolve_for_path(
    cache_store: &Option<Arc<dyn CacheStore>>,
    cache_cfg: Option<&CacheConfig>,
    request_path: &str,
) -> Option<Arc<dyn CacheStore>> {
    let cache = cache_store.as_ref()?;

    if let Some(cfg) = cache_cfg {
        if !cfg.cache_path_prefixes.is_empty() {
            let matched = cfg
                .cache_path_prefixes
                .iter()
                .filter_map(|p| normalize_prefix(p))
                .any(|prefix| path_matches_prefix(request_path, &prefix));
            if !matched {
                tracing::debug!(
                    path = %request_path,
                    prefixes = ?cfg.cache_path_prefixes,
                    "cache skipped (path not in cache_path_prefixes)"
                );
                return None;
            }
        }
    }

    Some(Arc::clone(cache))
}

fn normalize_prefix(prefix: &str) -> Option<String> {
    let trimmed = prefix.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('/') {
        Some(trimmed.to_string())
    } else {
        Some(format!("/{trimmed}"))
    }
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    if prefix.ends_with('/') {
        return path.starts_with(prefix);
    }
    path == prefix || path.starts_with(&format!("{prefix}/"))
}

// ── S3-backed implementation ────────────────────────────────────────────

pub struct S3CacheStore {
    inner: S3BackendSource,
}

#[async_trait]
impl CacheStore for S3CacheStore {
    async fn exists(&self, key: &str) -> Result<bool, AppError> {
        self.inner.exists(Some(key)).await
    }

    async fn get_stream(
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
        self.inner.stream(Some(key)).await
    }

    async fn upload(
        &self,
        key: &str,
        stream: BoxStream<'static, Result<Bytes, AppError>>,
        content_length: Option<u64>,
    ) -> Result<(), AppError> {
        self.inner.put_streaming(key, stream, content_length).await
    }
}

impl S3CacheStore {
    async fn build_from_config(
        config: &AppConfig,
    ) -> Result<Option<Arc<dyn CacheStore>>, AppError> {
        let cache_cfg = match config.cache.as_ref() {
            Some(cfg) => cfg,
            None => {
                tracing::info!("cache disabled (no cache config)");
                return Ok(None);
            }
        };

        let (access_key, secret_key) = match (&cache_cfg.access_key, &cache_cfg.secret_key) {
            (Some(a), Some(s)) => (a, s),
            _ => {
                tracing::info!(
                    "cache disabled (missing access_key or secret_key for bucket={})",
                    cache_cfg.bucket
                );
                return Ok(None);
            }
        };

        tracing::info!(
            "initializing S3 cache: endpoint={} bucket={}",
            cache_cfg.endpoint,
            cache_cfg.bucket
        );

        let s3_pool = Arc::new(S3ClientPool::new(
            Duration::from_secs(config.pool.s3_pool_ttl_secs),
            1, // cache talks to a single endpoint/bucket
        ));

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

        let store = S3CacheStore { inner: s3_backend };

        // Verify the endpoint is reachable AND the bucket exists by
        // listing objects (a HEAD on a missing key can hide bucket errors).
        match store.inner.check_bucket().await {
            Ok(()) => {
                tracing::info!(
                    "S3 cache ready: endpoint={} bucket={}",
                    cache_cfg.endpoint,
                    cache_cfg.bucket
                );
            }
            Err(e) => {
                return Err(AppError::Internal(format!(
                    "S3 cache is not usable (endpoint={} bucket={}): {e}",
                    cache_cfg.endpoint, cache_cfg.bucket
                )));
            }
        }

        Ok(Some(Arc::new(store)))
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_prefix, path_matches_prefix};

    #[test]
    fn prefix_matching_handles_trailing_slash_and_boundaries() {
        assert!(path_matches_prefix("/data/a/b", "/data/a"));
        assert!(path_matches_prefix("/data/a/b", "/data/a/"));
        assert!(path_matches_prefix("/data/a", "/data/a"));
        assert!(!path_matches_prefix("/data/ab", "/data/a"));
        assert!(!path_matches_prefix("/other/a", "/data/a"));
    }

    #[test]
    fn prefix_normalization_trims_and_adds_leading_slash() {
        assert_eq!(normalize_prefix(" /data/x "), Some("/data/x".to_string()));
        assert_eq!(normalize_prefix("data/x/"), Some("/data/x/".to_string()));
        assert_eq!(normalize_prefix("   "), None);
    }
}
