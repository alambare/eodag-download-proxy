use eodag_data_proxy::eodag::EodagClient;
use eodag_data_proxy::cache;
use eodag_data_proxy::backend::s3::S3ClientPool;
use eodag_data_proxy::config::AppConfig;
use eodag_data_proxy::eodag::HttpEodagClient;
use eodag_data_proxy::state::AppState;
use eodag_data_proxy::{logging, router};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    logging::init();

    let config = AppConfig::load().unwrap();

    let http_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(config.pool.http_pool_idle_timeout_secs))
        .build()
        .expect("failed to build HTTP client");

    let s3_pool = Arc::new(S3ClientPool::new(
        Duration::from_secs(config.pool.s3_pool_ttl_secs),
        config.pool.s3_pool_max_capacity,
    ));

    let eodag_client: Arc<dyn EodagClient + Send + Sync> = Arc::new(
        HttpEodagClient::new(config.eodag_url.clone(), http_client.clone()),
    );

    let cache_store = cache::build_from_config(&config)
        .await
        .expect("cache initialization failed");

    let state = AppState {
        config: config.clone(),
        http_client,
        cache_store,
        s3_pool,
        eodag_client,
    };
    let app = router::build(state);

    let listener = tokio::net::TcpListener::bind(&config.listen_addr)
        .await
        .expect("failed to bind");

    tracing::info!("listening on {}", config.listen_addr);

    axum::serve(listener, app).await.expect("server error");
}
