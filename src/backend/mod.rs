pub mod http;
pub mod s3;
use crate::error::AppError;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Response, StatusCode, header};
use bytes::Bytes;
use futures::stream::BoxStream;

/// Common trait for any byte stream source (cache, S3, HTTP).
#[async_trait]
pub trait ByteStreamSource: Send + Sync {
    /// Check if key exists
    async fn exists(&self, key: &str) -> Result<bool, AppError>;

    /// Stream the key, returning a `Response<Body>`.
    async fn stream(
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
}

/// Assemble a streaming `Response<Body>` with the given metadata headers.
fn build_streaming_response(
    body: Body,
    content_type: Option<String>,
    content_length: Option<u64>,
) -> Result<Response<Body>, AppError> {
    let mut builder = Response::builder().status(StatusCode::OK);

    if let Some(ct) = content_type {
        builder = builder.header(header::CONTENT_TYPE, ct);
    }
    if let Some(cl) = content_length {
        builder = builder.header(header::CONTENT_LENGTH, cl);
    }

    builder
        .body(body)
        .map_err(|e| AppError::Internal(format!("failed to build response: {e}")))
}
