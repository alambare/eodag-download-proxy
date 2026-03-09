use crate::cache::CacheStore;
use crate::backend::s3::S3ClientPool;
use crate::config::AppConfig;
use crate::eodag::EodagClient;
use std::sync::Arc;

/// Shared application state injected into every Axum handler via
/// `axum::extract::State`.
#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub http_client: reqwest::Client,
    pub cache_store: Option<Arc<dyn CacheStore>>,
    pub s3_pool: Arc<S3ClientPool>,
    pub eodag_client: Arc<dyn EodagClient + Send + Sync>,
}
