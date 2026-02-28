use crate::backend::ByteStreamSource;
use crate::error::AppError;
use crate::models::EodagResponse;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use reqwest::Client;
use std::collections::HashMap;

/// HTTP backend source for streaming arbitrary files over HTTP/HTTPS.
pub struct HTTPBackendSource {
    client: Client,
    base_url: String,
    headers: HashMap<String, String>,
}

impl HTTPBackendSource {
    /// Construct from an HTTP Eodag response.
    pub fn from_eodag_response(
        client: &Client,
        response: &EodagResponse,
    ) -> Result<Self, AppError> {
        match response {
            EodagResponse::Http { path, headers } => Ok(Self {
                client: client.clone(),
                base_url: path.clone(),
                headers: headers.clone(),
            }),
            _ => Err(AppError::Internal(
                "HTTPBackendSource created from non-HTTP EODAG response".to_string(),
            )),
        }
    }

    fn get_url(&self, subpath: Option<&str>) -> String {
        match subpath {
            Some(sp) if !sp.is_empty() => format!(
                "{}/{}",
                self.base_url.trim_end_matches('/'),
                sp.trim_start_matches('/')
            ),
            _ => self.base_url.clone(),
        }
    }
}

#[async_trait]
impl ByteStreamSource for HTTPBackendSource {
    async fn exists(&self, subpath: Option<&str>) -> Result<bool, AppError> {
        let url = self.get_url(subpath);

        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
        let mut header_map = HeaderMap::new();
        for (k, v) in &self.headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::try_from(k.as_str()),
                HeaderValue::try_from(v.as_str()),
            ) {
                header_map.insert(name, value);
            }
        }
        let resp = self
            .client
            .head(&url)
            .headers(header_map)
            .send()
            .await
            .map_err(|e| AppError::BackendError(format!("HTTP HEAD failed: {e}")))?;

        Ok(resp.status().is_success())
    }

    async fn stream(
        &self,
        subpath: Option<&str>,
    ) -> Result<
        (
            BoxStream<'static, Result<Bytes, AppError>>,
            Option<String>,
            Option<u64>,
        ),
        AppError,
    > {
        let url = self.get_url(subpath);

        tracing::info!(backend = "http", url = %url, "streaming from HTTP backend");

  
        let upstream = self.headers.iter().fold(
            self.client.get(&url),
            |req, (k, v)| req.header(k, v),
        )
        .send()
        .await
        .map_err(|e| AppError::BackendError(format!("HTTP request failed: {e}")))?;

        if !upstream.status().is_success() {
            let status = upstream.status();
            let body = upstream
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            return Err(AppError::BackendError(format!(
                "upstream returned {status}: {body}"
            )));
        }

        let content_type = upstream
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let content_length = upstream
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        let stream = upstream
            .bytes_stream()
            .map(|r| r.map_err(|e| AppError::BackendError(format!("stream error: {e}"))));

        Ok((Box::pin(stream), content_type, content_length))
    }
}
