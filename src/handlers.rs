use crate::backend::ByteStreamSource;
use crate::error::AppError;
use crate::models::{EodagResolveRequest, EodagResponse};
use crate::state::AppState;
use axum::body::Body;
use axum::extract::State;
use axum::http::Response;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

#[derive(Debug, serde::Deserialize)]
pub struct DataPathParams {
    pub provider: String,
    pub collection_id: String,
    pub item_id: String,
    pub asset_key: String,
    pub subpath: Option<String>,
}

/// Handler with subpath
pub async fn handle_data(
    State(state): State<AppState>,
    params: axum::extract::Path<DataPathParams>,
) -> Result<Response<Body>, AppError> {
    handle_request(state, &params).await
}

/// Handler without subpath (legacy support)
pub async fn handle_data_no_subpath(
    State(state): State<AppState>,
    params: axum::extract::Path<(String, String, String, String)>,
) -> Result<Response<Body>, AppError> {
    let (provider, collection_id, item_id, asset_key) = params.0;
    let params = DataPathParams {
        provider,
        collection_id,
        item_id,
        asset_key,
        subpath: None,
    };
    handle_request(state, &params).await
}

/// Main request handler
async fn handle_request(
    state: AppState,
    params: &DataPathParams,
) -> Result<Response<Body>, AppError> {
    let cache_key = build_cache_key(params, params.subpath.as_deref());

    if let Some(cache) = &state.cache_store {
        if cache.exists(&cache_key).await? {
            let (stream, ct, cl) = cache.stream(&cache_key).await?;
            return Ok(build_response(stream, ct, cl));
        }
    }

    let req = EodagResolveRequest {
        provider: params.provider.clone(),
        collection_id: params.collection_id.clone(),
        item_id: params.item_id.clone(),
        asset_key: params.asset_key.clone(),
    };

    let eodag_response = state.eodag_client.resolve(&req).await?;

    let upstream_path = if let Some(subpath) = &params.subpath {
        format!(
            "{}/{}",
            eodag_response.get_path(),
            subpath.trim_start_matches('/')
        )
    } else {
        eodag_response.get_path().to_string()
    };

    let backend_source: Arc<dyn ByteStreamSource> = match &eodag_response {
        EodagResponse::S3 { .. } => Arc::new(
            crate::backend::s3::S3BackendSource::from_eodag_response(
                state.s3_pool.clone(),
                &eodag_response,
            )
            .await?,
        ),
        EodagResponse::Http { .. } => Arc::new(
            crate::backend::http::HTTPBackendSource::from_eodag_response(
                &state.http_client,
                &eodag_response,
            )?,
        ),
    };

    let (stream, content_type, content_length) = backend_source.stream(&upstream_path).await?;
    Ok(build_response(stream, content_type, content_length))
}

/// Build cache key based on data path + optional subpath
fn build_cache_key(params: &DataPathParams, subpath: Option<&str>) -> String {
    let mut key = format!(
        "{}/{}/{}/{}",
        params.provider, params.collection_id, params.item_id, params.asset_key
    );
    if let Some(sp) = subpath {
        key.push('/');
        key.push_str(sp);
    }
    key
}

/// Build HTTP response with streamed body and optional headers
fn build_response(
    stream: impl futures::Stream<Item = Result<Bytes, AppError>> + Send + 'static,
    content_type: Option<String>,
    content_length: Option<u64>,
) -> Response<Body> {
    let body_stream = stream
        .map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
    let mut builder = Response::builder().status(200);
    if let Some(ct) = content_type {
        builder = builder.header("content-type", ct);
    }
    if let Some(cl) = content_length {
        builder = builder.header("content-length", cl);
    }
    builder.body(Body::from_stream(body_stream)).unwrap()
}
