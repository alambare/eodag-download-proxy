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
        Self { base_url, http }
    }
}

#[async_trait]
impl EodagClient for HttpEodagClient {
    async fn resolve(&self, request: &EodagResolveRequest) -> Result<EodagResponse, AppError> {
        let url = format!(
            "{}/resolve/{}/{}/{}/{}",
            self.base_url.trim_end_matches('/'),
            request.provider,
            request.collection_id,
            request.item_id,
            request.asset_key,
        );

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
            .get(&url)
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
                        
                        // s3://eodata/ auxdata/CopDEM/COP-DEM_GLO-30-DGED                     /   DEM1_SAR_DGE_30_20101212T040432_20130429T040610_ADS_000000_wfpc                                              .DEM         /Copernicus_DSM_10_S04_00_E023_00/DEM/Copernicus_DSM_10_S04_00_E023_00_DEM.tif
                        // s3://eodata/ Sentinel-5P  /   TROPOMI /  L1B_RA_BD4  / 2018/04/30   /   S5P _ RPRO_L1B_RA_BD4 _   20180430T020120 _ 20180430T034250 _ 02819_03_020100_20220630T164531.nc
                        // s3://eodata/ Sentinel-3   /   SYNERGY /  SY_2_SYN___ / 2018/10/07   /   S3A _ SY_2_SYN___     _   20181007T053450 _ 20181007T053552 _ 20181010T060805_0061_036_290_4320_LN2_O_NT_002 .SEN3        /Syn_Oa07_reflectance.nc
                        // s3://eodata/ Sentinel-3   /   OLCI    /  OL_2_WRR___ / 2016/04/25   /   S3A _ OL_2_WRR___     _   20160425T114036 _ 20160425T114236 _ 20210510T133650_0119_003_237______MAR_R_NT_003 .SEN3        /Oa03_reflectance.nc
                        // s3://eodata/ Sentinel-3   /   OLCI    /  OL_1_ERR___ / 2016/04/06   /   S3A _ OL_1_ERR___     _   20160406T080551 _ 20160406T084949 _ 20241003T130601_2638_002_349______MAR_R_NT_004 .SEN3        /Oa02_radiance.nc
                        // s3://eodata/ Sentinel-3   /   SRAL    /  SR_2_LAN    / 2016/03/01   /   S3A _ SR_2_LAN___     _   20160301T143301 _ 20160301T152330 _ 20180518T211020_3029_001_224______LR1_R_NT_003 .SEN3        /enhanced_measurement.nc
                        // s3://eodata/ Sentinel-1   /   SAR     /  GRD         / 2014/10/03   /   S1A _ IW_GRDH_1SDV    _   20141003T165234 _ 20141003T165259 _ 002668_002F8B_4584.SAFE                                     /measurement/s1a-iw-grd-vv-20141003t165234-20141003t165259-002668-002f8b-001.tiff
                        // s3://eodata/ Sentinel-2   /   MSI     /  L2A_N0500   / 2015/07/04   /   S2A _ MSIL2A          _   20150704T101006 _ N0500_R022_T32TMN_20231012T100650                                .SAFE        /GRANULE/L2A_T32TMN _ A000162 _ 20150704T101337 /IMG_DATA/R10m/T32TMN_20150704T101006_AOT_10m.jp2


                        // For all the format /data / provider / collection / item / asset_name works but for  S1 and S2
                        // For S1: we need a template per asset because the asset are in a subpath. That should be the generic behavior probably anyway
                        // For S2: the problem is the granule is in the path and specific to the item and we cannot infer it from the item id or the asset name. Because of the absolute orbit present only in the granule...
                        // The solution would be to:
                        // 1. EODAG returns a list of templates (1 per asset)
                        // 2. we accept additional query parameters in the /data request to fill the template (e.g. granule name)


                        // => from watching EODAG it looks complicated to be able to export a download URL template. We will need to request EODAG for the URLs before finding the right solution.
                        //
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
