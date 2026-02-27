use serde::Deserialize;
use std::collections::HashMap;

/// Response returned by the EODAG resolution endpoint.
///
/// The variant is determined by the shape of the JSON payload
/// (`#[serde(untagged)]`):
/// - If `endpoint_url` is present → [`EodagResponse::S3`]
/// - Otherwise (`path` + `headers`) → [`EodagResponse::Http`]
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum EodagResponse {
    /// S3-backed asset.  `path` contains `"bucket/prefix"`.
    S3 {
        /// S3-compatible endpoint URL (e.g. `"https://s3.amazonaws.com"`).
        endpoint_url: String,

        /// `"bucket/key/prefix"` – the bucket is the first segment.
        path: String,

        /// Static access key (optional – omit or set `anon: true` for public).
        key: Option<String>,

        /// Static secret key.
        secret: Option<String>,

        /// Session token (STS temporary credentials).
        token: Option<String>,

        /// Whether the bucket allows anonymous access.
        #[serde(default)]
        anon: bool,

        /// Whether requester-pays is enabled on the bucket.
        #[serde(default)]
        requester_pays: bool,
    },

    /// HTTP-backed asset.
    Http {
        /// Full URL to GET.
        path: String,

        /// Extra headers to include in the request (e.g. `Authorization`).
        #[serde(default)]
        headers: HashMap<String, String>,
    },
}

impl EodagResponse {
    /// Human-readable label for logging (avoids leaking credentials).
    pub fn log_summary(&self) -> String {
        match self {
            EodagResponse::S3 {
                endpoint_url, path, ..
            } => format!("S3 backend: endpoint={endpoint_url} path={path}"),
            EodagResponse::Http { path, .. } => format!("HTTP backend: url={path}"),
        }
    }

    pub fn get_path(&self) -> &str {
        match self {
            EodagResponse::S3 { path, .. } => path,
            EodagResponse::Http { path, .. } => path,
        }
    }
}

/// Request sent to EODAG for resolution.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EodagResolveRequest {
    pub provider: String,
    pub collection_id: String,
    pub item_id: String,
    pub asset_key: String,
}
