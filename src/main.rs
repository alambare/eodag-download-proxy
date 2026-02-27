use eodag_download_proxy::backend::ByteStreamSource;
use eodag_download_proxy::cache::S3CacheStore;
use eodag_download_proxy::client_pool::S3ClientPool;
use eodag_download_proxy::config::AppConfig;
use eodag_download_proxy::eodag::MockEodagClient;
use eodag_download_proxy::state::AppState;
use eodag_download_proxy::{logging, router};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // ── Logging ─────────────────────────────────────────────────────────
    logging::init();

    // ── Configuration ───────────────────────────────────────────────────
    let config = AppConfig::load().unwrap();

    // ── HTTP client (shared, connection-pooled) ─────────────────────────
    let http_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(config.pool.http_pool_idle_timeout_secs))
        .build()
        .expect("failed to build HTTP client");

    // ── S3 client pool ──────────────────────────────────────────────────
    let s3_pool = Arc::new(S3ClientPool::new(
        Duration::from_secs(config.pool.s3_pool_ttl_secs),
        config.pool.s3_pool_max_capacity,
    ));

    // ── EODAG client (mock for now) ─────────────────────────────────────
    let eodag_client: Arc<dyn eodag_download_proxy::eodag::EodagClient> =
        Arc::new(MockEodagClient::from_mode(&config.mock_mode));

    let cache_store: Option<Arc<dyn ByteStreamSource>> =
        S3CacheStore::build_from_config(&config, s3_pool.clone());

    // ── Application state ───────────────────────────────────────────────
    let state = AppState {
        config: config.clone(),
        http_client,
        cache_store,
        s3_pool,
        eodag_client,
    };

    // ── Router ──────────────────────────────────────────────────────────
    let app = router::build(state);

    // ── Serve ───────────────────────────────────────────────────────────
    let listener = tokio::net::TcpListener::bind(&config.listen_addr)
        .await
        .expect("failed to bind");

    tracing::info!("listening on {}", config.listen_addr);

    axum::serve(listener, app).await.expect("server error");
}
