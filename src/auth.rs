use crate::config::AuthConfig;
use crate::error::AppError;
use axum::http::request::Parts;

/// Outcome of the authentication check.
///
/// Currently a stub that always succeeds.  Will carry user identity
/// claims once OIDC is wired in.
#[derive(Debug, Clone)]
pub struct AuthInfo {
    /// Authenticated subject (e.g. "anonymous" while auth is stubbed).
    pub subject: String,
}

/// Validate authentication for the incoming request.
///
/// When `auth_config` is `None` (auth disabled), the call always succeeds
/// with an anonymous identity.  When it is `Some(…)`, the OIDC validation
/// logic will be implemented here in the future.
pub async fn check_auth(
    _parts: &Parts,
    _auth_config: Option<&AuthConfig>,
) -> Result<AuthInfo, AppError> {
    // Stub: always allow.
    Ok(AuthInfo {
        subject: "anonymous".to_string(),
    })
}
