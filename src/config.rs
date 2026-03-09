use serde::Deserialize;
use serde::Deserializer;

/// Top-level application configuration.
///
/// Loaded from `config.toml` at the working directory, with overrides
/// from environment variables prefixed with `EODAG_DL_` (double underscore
/// separates nested keys, e.g. `EODAG_DL_CACHE__BUCKET`).
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    /// Address to listen on (default: "0.0.0.0:8080").
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    /// Base URL of the EODAG resolution endpoint.
    /// Example: "http://localhost:5000/resolve"
    pub eodag_url: String,

    /// Optional S3 cache configuration.
    pub cache: Option<CacheConfig>,

    /// Optional authentication (OIDC) configuration – placeholder.
    pub auth: Option<AuthConfig>,

    /// Optional authorization (OPA) configuration – placeholder.
    pub opa: Option<OpaConfig>,

    /// Client pool tuning.
    #[serde(default)]
    pub pool: PoolConfig,

    /// Mock mode for development: "http" or "s3" (default: "http").
    /// Only used when mock EODAG client is active.
    #[serde(default = "default_mock_mode")]
    pub mock_mode: String,
}

/// S3 cache bucket configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    /// S3 bucket name used for caching.
    pub bucket: String,

    /// S3-compatible endpoint URL.
    pub endpoint: String,

    /// AWS region (default: "us-east-1").
    #[serde(default = "default_region")]
    pub region: String,

    /// Access key for the cache bucket.
    pub access_key: Option<String>,

    /// Secret key for the cache bucket.
    pub secret_key: Option<String>,

    /// Path prefixes for which we generate pre-signed URLs instead of
    /// streaming through the server.  Empty means always stream.
    #[serde(default, deserialize_with = "deserialize_comma_separated")]
    pub presign_prefixes: Vec<String>,

    /// Request path prefixes that are allowed to use cache.
    ///
    /// Example: ["/data/sentinel-2/", "/data/landsat/"]
    /// Empty means cache is enabled for all paths.
    #[serde(default, deserialize_with = "deserialize_comma_separated")]
    pub cache_path_prefixes: Vec<String>,
}

/// Placeholder for future OIDC authentication configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    /// OIDC issuer URL.
    pub issuer_url: Option<String>,
    /// Expected audience.
    pub audience: Option<String>,
}

/// Placeholder for future OPA authorization configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct OpaConfig {
    /// OPA policy evaluation URL.
    pub url: String,
}

/// Client pool tuning knobs.
#[derive(Debug, Clone, Deserialize)]
pub struct PoolConfig {
    /// Time-to-live (in seconds) for cached S3 clients (default: 900 = 15 min).
    #[serde(default = "default_s3_pool_ttl")]
    pub s3_pool_ttl_secs: u64,

    /// Maximum number of cached S3 clients (default: 100).
    #[serde(default = "default_s3_pool_max")]
    pub s3_pool_max_capacity: u64,

    /// Idle timeout (in seconds) for the shared HTTP connection pool
    /// managed by reqwest (default: 90).
    #[serde(default = "default_http_idle_timeout")]
    pub http_pool_idle_timeout_secs: u64,
}

// ── Defaults ────────────────────────────────────────────────────────────

fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_mock_mode() -> String {
    "http".to_string()
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_s3_pool_ttl() -> u64 {
    900
}

fn default_s3_pool_max() -> u64 {
    100
}

fn default_http_idle_timeout() -> u64 {
    90
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            s3_pool_ttl_secs: default_s3_pool_ttl(),
            s3_pool_max_capacity: default_s3_pool_max(),
            http_pool_idle_timeout_secs: default_http_idle_timeout(),
        }
    }
}

// ── Loading ─────────────────────────────────────────────────────────────

impl AppConfig {
    /// Build the configuration by merging (in order):
    /// 1. `config.toml` in the current directory (optional)
    /// 2. Environment variables prefixed with `EODAG_DL_`
    pub fn load() -> Result<Self, config::ConfigError> {
        let builder = config::Config::builder()
            .add_source(config::File::with_name("config").required(false))
            .add_source(
                config::Environment::with_prefix("EODAG_DL")
                    .separator("__")
                    .try_parsing(true),
            );

        builder.build()?.try_deserialize()
    }
}

/// Accept either a TOML array `["a", "b"]` or a comma-separated env-var
/// string `"a,b"`.  Blank entries are silently dropped.
fn deserialize_comma_separated<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de;

    struct StringOrVec;

    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Vec<String>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a string or a sequence of strings")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect())
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut v = Vec::new();
            while let Some(s) = seq.next_element::<String>()? {
                let s = s.trim().to_string();
                if !s.is_empty() {
                    v.push(s);
                }
            }
            Ok(v)
        }
    }

    deserializer.deserialize_any(StringOrVec)
}
