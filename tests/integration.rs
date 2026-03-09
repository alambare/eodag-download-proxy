use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use bytes::Bytes;
use http_body_util::BodyExt;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use eodag_download_proxy::cache::CacheStore;
use futures::stream::BoxStream;

/// No-op cache mock – always reports a miss and discards writes.
struct MockCacheStore;

#[async_trait::async_trait]
impl CacheStore for MockCacheStore {
    async fn exists(&self, _key: &str) -> Result<bool, eodag_download_proxy::error::AppError> {
        Ok(false)
    }

    async fn get_stream(
        &self,
        _key: &str,
    ) -> Result<
        (
            BoxStream<'static, Result<Bytes, eodag_download_proxy::error::AppError>>,
            Option<String>,
            Option<u64>,
        ),
        eodag_download_proxy::error::AppError,
    > {
        Err(eodag_download_proxy::error::AppError::Internal(
            "MockCacheStore has no data".to_string(),
        ))
    }

    async fn upload(
        &self,
        _key: &str,
        _stream: BoxStream<'static, Result<Bytes, eodag_download_proxy::error::AppError>>,
        _content_length: Option<u64>,
    ) -> Result<(), eodag_download_proxy::error::AppError> {
        Ok(())
    }
}
use eodag_download_proxy::backend::s3::S3ClientPool;
use eodag_download_proxy::config::{AppConfig, PoolConfig};
use eodag_download_proxy::eodag::{EodagClient, MockEodagClient};
use eodag_download_proxy::models::EodagResponse;
use eodag_download_proxy::state::AppState;

// ── Helpers ─────────────────────────────────────────────────────────────

/// Build an `AppState` with the given EODAG mock and optional cache mock.
fn build_state(eodag: Arc<dyn EodagClient>, cache: Option<Arc<dyn CacheStore>>) -> AppState {
    let config = AppConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        eodag_url: "http://unused".to_string(),
        cache: None,
        auth: None,
        opa: None,
        pool: PoolConfig::default(),
        mock_mode: "http".to_string(),
    };

    let http_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    let s3_pool = Arc::new(S3ClientPool::new(Duration::from_secs(60), 10));

    AppState {
        config,
        http_client,
        cache_store: cache,
        s3_pool,
        eodag_client: eodag,
    }
}

/// Send a GET request through the router and return the response.
async fn get(app: axum::Router, uri: &str) -> axum::http::Response<Body> {
    app.oneshot(
        axum::http::Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
}

/// Collect the full response body as bytes.
async fn body_bytes(resp: axum::http::Response<Body>) -> Bytes {
    resp.into_body().collect().await.unwrap().to_bytes()
}

// ── Tests ───────────────────────────────────────────────────────────────

/// Happy path: EODAG returns an HTTP backend, mock HTTP backend replies
/// with data, server streams it back.
#[tokio::test]
async fn test_http_backend_happy_path() {
    // 1. Start a mock HTTP server acting as the remote data backend.
    let backend = MockServer::start().await;
    let payload = b"hello-earth-observation-data";

    Mock::given(method("GET"))
        .and(path("/some/data.tif"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(payload.to_vec())
                .insert_header("content-type", "image/tiff"),
        )
        .mount(&backend)
        .await;

    // 2. Configure the mock EODAG client to return an HTTP response
    //    pointing to our mock backend.
    let eodag_response = EodagResponse::Http {
        path: format!("{}/some/data.tif", backend.uri()),
        headers: Default::default(),
    };
    let eodag: Arc<dyn EodagClient> = Arc::new(MockEodagClient::new(eodag_response));

    // 3. Build the app and make a request.
    let state = build_state(eodag, Some(Arc::new(MockCacheStore)));
    let app = eodag_download_proxy::router::build(state);

    let resp = get(app, "/data/test_provider/test_collection/item1/B04.tif").await;

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "image/tiff"
    );

    let body = body_bytes(resp).await;
    assert_eq!(body.as_ref(), payload);
}

/// Happy path with a subpath (Zarr chunk scenario).
#[tokio::test]
async fn test_http_backend_with_subpath() {
    let backend = MockServer::start().await;
    let payload = b"zarr-chunk-data";

    Mock::given(method("GET"))
        .and(path("/some/data.zarr/0/0/0"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(payload.to_vec())
                .insert_header("content-type", "application/octet-stream"),
        )
        .mount(&backend)
        .await;

    let eodag_response = EodagResponse::Http {
        path: format!("{}/some/data.zarr", backend.uri()),
        headers: Default::default(),
    };
    let eodag: Arc<dyn EodagClient> = Arc::new(MockEodagClient::new(eodag_response));

    let state = build_state(eodag, Some(Arc::new(MockCacheStore)));
    let app = eodag_download_proxy::router::build(state);

    let resp = get(
        app,
        "/data/test_provider/test_collection/item1/data.zarr/0/0/0",
    )
    .await;

    assert_eq!(resp.status(), 200);
    let body = body_bytes(resp).await;
    assert_eq!(body.as_ref(), payload);
}

/// HTTP backend returns an error → server should return 502.
#[tokio::test]
async fn test_http_backend_upstream_error() {
    let backend = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/bad"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&backend)
        .await;

    let eodag_response = EodagResponse::Http {
        path: format!("{}/bad", backend.uri()),
        headers: Default::default(),
    };
    let eodag: Arc<dyn EodagClient> = Arc::new(MockEodagClient::new(eodag_response));

    let state = build_state(eodag, Some(Arc::new(MockCacheStore)));
    let app = eodag_download_proxy::router::build(state);

    let resp = get(app, "/data/prov/col/item/asset").await;
    assert_eq!(resp.status(), 502);
}

/// Request with headers forwarded to the backend.
#[tokio::test]
async fn test_http_backend_with_headers() {
    let backend = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/data.tif"))
        .and(wiremock::matchers::header(
            "Authorization",
            "Bearer secret-token",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok".to_vec()))
        .mount(&backend)
        .await;

    let mut headers = std::collections::HashMap::new();
    headers.insert(
        "Authorization".to_string(),
        "Bearer secret-token".to_string(),
    );

    let eodag_response = EodagResponse::Http {
        path: format!("{}/data.tif", backend.uri()),
        headers,
    };
    let eodag: Arc<dyn EodagClient> = Arc::new(MockEodagClient::new(eodag_response));

    let state = build_state(eodag, Some(Arc::new(MockCacheStore)));
    let app = eodag_download_proxy::router::build(state);

    let resp = get(app, "/data/prov/col/item/asset").await;
    assert_eq!(resp.status(), 200);
    let body = body_bytes(resp).await;
    assert_eq!(body.as_ref(), b"ok");
}

/// Without cache configured (cache_store = None), should still work.
#[tokio::test]
async fn test_no_cache_configured() {
    let backend = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"data".to_vec()))
        .mount(&backend)
        .await;

    let eodag_response = EodagResponse::Http {
        path: format!("{}/data", backend.uri()),
        headers: Default::default(),
    };
    let eodag: Arc<dyn EodagClient> = Arc::new(MockEodagClient::new(eodag_response));

    // No cache at all.
    let state = build_state(eodag, None);
    let app = eodag_download_proxy::router::build(state);

    let resp = get(app, "/data/prov/col/item/asset").await;
    assert_eq!(resp.status(), 200);
}

/// 404 for a path that doesn't match the route pattern.
#[tokio::test]
async fn test_bad_route_returns_404() {
    let eodag: Arc<dyn EodagClient> = Arc::new(MockEodagClient::http_mock("http://unused"));

    let state = build_state(eodag, None);
    let app = eodag_download_proxy::router::build(state);

    let resp = get(app, "/data/only_two_segments").await;
    assert_eq!(resp.status(), 404);
}

/// Cache hit scenario: mock cache returns data, EODAG should NOT be called.
#[tokio::test]
async fn test_cache_hit() {
    use async_trait::async_trait;
    use futures::stream::BoxStream;

    /// A cache mock that always reports a hit and streams back fixed data.
    struct HitCache;

    #[async_trait]
    impl CacheStore for HitCache {
        async fn exists(&self, _key: &str) -> Result<bool, eodag_download_proxy::error::AppError> {
            Ok(true)
        }

        async fn get_stream(
            &self,
            _key: &str,
        ) -> Result<
            (
                BoxStream<'static, Result<Bytes, eodag_download_proxy::error::AppError>>,
                Option<String>,
                Option<u64>,
            ),
            eodag_download_proxy::error::AppError,
        > {
            let data = Bytes::from_static(b"cached-data");
            let stream = futures::stream::once(async { Ok(data) });
            Ok((
                Box::pin(stream),
                Some("application/octet-stream".to_string()),
                Some(11),
            ))
        }

        async fn upload(
            &self,
            _key: &str,
            _stream: BoxStream<'static, Result<Bytes, eodag_download_proxy::error::AppError>>,
            _content_length: Option<u64>,
        ) -> Result<(), eodag_download_proxy::error::AppError> {
            Ok(())
        }
    }

    // EODAG mock that panics if called (should never happen on cache hit).
    struct PanicEodag;

    #[async_trait]
    impl EodagClient for PanicEodag {
        async fn resolve(
            &self,
            _req: &eodag_download_proxy::models::EodagResolveRequest,
        ) -> Result<EodagResponse, eodag_download_proxy::error::AppError> {
            panic!("EODAG should not be called on cache hit");
        }
    }

    let eodag: Arc<dyn EodagClient> = Arc::new(PanicEodag);
    let cache: Arc<dyn CacheStore> = Arc::new(HitCache);

    let state = build_state(eodag, Some(cache));
    let app = eodag_download_proxy::router::build(state);

    let resp = get(app, "/data/prov/col/item/asset.tif").await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/octet-stream"
    );

    let body = body_bytes(resp).await;
    assert_eq!(body.as_ref(), b"cached-data");
}

/// EODAG response deserialization: HTTP variant.
#[tokio::test]
async fn test_eodag_response_deser_http() {
    let json =
        r#"{"path": "https://example.com/data.tif", "headers": {"Authorization": "Bearer x"}}"#;
    let resp: EodagResponse = serde_json::from_str(json).unwrap();
    match resp {
        EodagResponse::Http { path, headers } => {
            assert_eq!(path, "https://example.com/data.tif");
            assert_eq!(headers.get("Authorization").unwrap(), "Bearer x");
        }
        _ => panic!("expected Http variant"),
    }
}

/// EODAG response deserialization: S3 variant.
#[tokio::test]
async fn test_eodag_response_deser_s3() {
    let json = r#"{
        "endpoint_url": "https://s3.eu-west-1.amazonaws.com",
        "path": "my-bucket/some/prefix/file.tif",
        "key": "AKIA...",
        "secret": "wJalr...",
        "anon": false
    }"#;
    let resp: EodagResponse = serde_json::from_str(json).unwrap();
    match resp {
        EodagResponse::S3 {
            endpoint_url,
            path,
            key,
            anon,
            ..
        } => {
            assert_eq!(endpoint_url, "https://s3.eu-west-1.amazonaws.com");
            assert_eq!(path, "my-bucket/some/prefix/file.tif");
            assert_eq!(key.unwrap(), "AKIA...");
            assert!(!anon);
        }
        _ => panic!("expected S3 variant"),
    }
}
