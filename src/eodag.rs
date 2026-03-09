use crate::error::AppError;
use crate::models::{EodagResolveRequest, EodagResponse};
use async_trait::async_trait;

/// Trait abstracting the EODAG resolution service.
///
/// Implementations must be `Send + Sync` so they can be shared across
/// Tokio tasks via `Arc<dyn EodagClient>`.
#[async_trait]
pub trait EodagClient: Send + Sync {
    /// Ask EODAG how to fetch the asset identified by `request`.
    ///
    /// Returns the download instructions (HTTP or S3).
    async fn resolve(&self, request: &EodagResolveRequest) -> Result<EodagResponse, AppError>;
}

// ── Real implementation ─────────────────────────────────────────────────

/// Production EODAG client backed by an HTTP call.
pub struct HttpEodagClient {
    base_url: String,
    http: reqwest::Client,
}

impl HttpEodagClient {
    pub fn new(base_url: String, http: reqwest::Client) -> Self {
        tracing::info!(eodag_url = %base_url, "EODAG control plane initialized");
        Self { base_url, http }
    }
}

#[async_trait]
impl EodagClient for HttpEodagClient {
    async fn resolve(&self, request: &EodagResolveRequest) -> Result<EodagResponse, AppError> {
        let mut url = url::Url::parse(&format!(
            "{}/resolve/eodag",
            self.base_url.trim_end_matches('/'),
        ))
        .map_err(|e| AppError::EodagError(format!("invalid EODAG base URL: {e}")))?;

        url.query_pairs_mut()
            .append_pair("provider", &request.provider)
            .append_pair("collection_id", &request.collection_id)
            .append_pair("item_id", &request.item_id)
            .append_pair("asset_key", &request.asset_key);

        tracing::debug!(
            eodag_url = %url,
            provider = %request.provider,
            collection_id = %request.collection_id,
            item_id = %request.item_id,
            asset_key = %request.asset_key,
            "sending resolve request to EODAG"
        );

        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| AppError::EodagError(format!("request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            return Err(AppError::EodagError(format!(
                "EODAG returned {status}: {body}"
            )));
        }

        // Note: we intentionally do NOT log the response body because it
        // may contain credentials (access keys, tokens).
        let eodag_response: EodagResponse = resp
            .json()
            .await
            .map_err(|e| AppError::EodagError(format!("failed to parse response: {e}")))?;

        Ok(eodag_response)
    }
}

// ── Mock implementation ─────────────────────────────────────────────────

/// Mock EODAG client that returns a pre-configured response.
///
/// Useful for tests and local development without a running EODAG service.
pub struct MockEodagClient {
    response: EodagResponse,
}

impl MockEodagClient {
    /// Creates a mock that always returns `response`.
    pub fn new(response: EodagResponse) -> Self {
        Self { response }
    }

    /// Convenience: returns a mock that yields an HTTP-type response.
    pub fn http_mock(url: &str) -> Self {
        Self {
            response: EodagResponse::Http {
                path: url.to_string(),
                headers: Default::default(),
            },
        }
    }

    /// Build a mock from config `mock_mode`.
    ///
    /// Supported modes:
    /// - `"http"` — simulates a Copernicus-like HTTP backend
    /// - `"s3"`  — simulates a public S3 bucket (anonymous access)
    pub fn from_mode(mode: &str) -> Self {
        match mode {
            "s3" => {
                tracing::info!("mock EODAG: using S3 preset (public bucket, anonymous)");
                Self {
                    response: EodagResponse::S3 {
                        endpoint_url: "https://eodata.cloudferro.com".to_string(),
                        path: "s3://eodata/Sentinel-2/MSI/L2A_N0500/2015/07/04/S2A_MSIL2A_20150704T101006_N0500_R022_T32TMN_20231012T100650.SAFE/GRANULE/L2A_T32TMN_A000162_20150704T101337/IMG_DATA/R10m/T32TMN_20150704T101006_AOT_10m.jp2".to_string(),
                        key: Some("xxx".to_string()),
                        secret: Some("xxx".to_string()),
                        token: None,
                        anon: false,
                        requester_pays: false,
                    },
                }
            }
            // Default: HTTP mock
            _ => {
                tracing::info!(mode = %mode, "mock EODAG: using HTTP preset (Copernicus WEkEO download)");
                Self {
                    response: EodagResponse::Http {
                        path: "https://download.dataspace.copernicus.eu/odata/v1/Products(1f71078c-1f67-578b-a18b-1c0e68acf7ad)/$value".to_string(),
                        headers: {
                            let mut h = std::collections::HashMap::new();
                            h.insert("Accept".to_string(), "application/octet-stream".to_string());
                            h
                        },
                    },
                }
            }
        }
    }
}

#[async_trait]
impl EodagClient for MockEodagClient {
    async fn resolve(&self, request: &EodagResolveRequest) -> Result<EodagResponse, AppError> {
        tracing::info!(
            provider = %request.provider,
            collection = %request.collection_id,
            item = %request.item_id,
            asset = %request.asset_key,
            "mock EODAG: returning pre-configured response"
        );
        Ok(self.response.clone())
    }
}
