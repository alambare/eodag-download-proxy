pub mod http;
pub mod s3;
use crate::error::AppError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

/// Common trait for any byte stream source (cache, S3, HTTP).
#[async_trait]
pub trait ByteStreamSource: Send + Sync {
    /// Check if key exists
    async fn exists(&self, key: Option<&str>) -> Result<bool, AppError>;

    /// Stream the key, returning a `Response<Body>`.
    async fn stream(
        &self,
        key: Option<&str>,
    ) -> Result<
        (
            BoxStream<'static, Result<Bytes, AppError>>,
            Option<String>,
            Option<u64>,
        ),
        AppError,
    >;
}