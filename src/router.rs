use crate::handlers;
use crate::state::AppState;
use axum::Router;
use axum::routing::get;
use tower_http::trace::TraceLayer;

/// Build the Axum router with all routes and middleware.
pub fn build(state: AppState) -> Router {
    Router::new()
        // Route with optional trailing subpath (Zarr chunks / nested assets).
        .route(
            "/data/{provider}/{collection_id}/{item_id}/{asset_key}/{*subpath}",
            get(handlers::handle_data),
        )
        // Route without subpath.
        .route(
            "/data/{provider}/{collection_id}/{item_id}/{asset_key}",
            get(handlers::handle_data_no_subpath),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
