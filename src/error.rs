use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

/// Application-level error type.
///
/// Each variant maps to an HTTP status code and a human-readable message
/// returned to the client.  Internal details are logged but never leaked.
#[derive(Debug, Error)]
pub enum AppError {
    #[error("authentication required")]
    AuthError(String),

    #[error("forbidden")]
    AuthzError(String),

    #[error("resource not found: {0}")]
    NotFound(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("eodag resolution failed: {0}")]
    EodagError(String),

    #[error("backend request failed: {0}")]
    BackendError(String),

    #[error("cache error: {0}")]
    CacheError(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::AuthError(_) => (StatusCode::UNAUTHORIZED, self.to_string()),
            AppError::AuthzError(_) => (StatusCode::FORBIDDEN, self.to_string()),
            AppError::NotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            AppError::BadRequest(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            AppError::EodagError(_) => (StatusCode::BAD_GATEWAY, self.to_string()),
            AppError::BackendError(_) => (StatusCode::BAD_GATEWAY, self.to_string()),
            AppError::CacheError(_) => {
                tracing::error!("cache error: {self}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
            AppError::Internal(_) => {
                tracing::error!("internal error: {self}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };

        (status, message).into_response()
    }
}
