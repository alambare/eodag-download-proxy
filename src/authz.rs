use crate::auth::AuthInfo;
use crate::config::OpaConfig;
use crate::error::AppError;
use axum::http::request::Parts;

/// Outcome of the authorization check.
#[derive(Debug, Clone)]
pub struct AuthzDecision {
    pub allowed: bool,
}

/// Evaluate authorization for the given request + identity.
///
/// When `opa_config` is `None` (authz disabled), the call always allows.
/// When it is `Some(…)`, the OPA query will be implemented here.
pub async fn check_authz(
    _parts: &Parts,
    _auth_info: &AuthInfo,
    _opa_config: Option<&OpaConfig>,
) -> Result<AuthzDecision, AppError> {
    // Stub: always allow.
    Ok(AuthzDecision { allowed: true })
}
