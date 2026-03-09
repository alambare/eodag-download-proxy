use crate::backend::ByteStreamSource;
use crate::cache::CacheStore;
use crate::error::AppError;
use crate::models::{EodagResolveRequest, EodagResponse};
use crate::state::AppState;
use axum::body::Body;
use axum::extract::State;
use axum::http::Response;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::sync::Arc;
use crate::backend::s3::S3BackendSource;
use crate::backend::http::HTTPBackendSource;
use async_stream::stream;

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
    // Resolve cache store for this request (None when disabled or path not matched).
    let resource_path = build_resource_path(params, params.subpath.as_deref());
    let cache = crate::cache::resolve_for_path(
        &state.cache_store,
        state.config.cache.as_ref(),
        &resource_path,
    );

    // Check cache
    if let Some(c) = &cache {
        if let Some((stream, content_type, content_length)) = c.try_get(&resource_path).await {
            return Ok(build_response(stream, content_type, content_length));
        }
    }

    // Cache miss: resolve backend via EODAG
    let req = EodagResolveRequest {
        provider: params.provider.clone(),
        collection_id: params.collection_id.clone(),
        item_id: params.item_id.clone(),
        asset_key: params.asset_key.clone(),
    };

    let eodag_response = state.eodag_client.resolve(&req).await?;
    tracing::info!(eodag_response = %eodag_response.log_summary(), "EODAG resolution successful");

    let backend_source = create_backend_source(&eodag_response, &state).await?;

    // Stream from backend
    let (backend_stream, content_type, content_length) = backend_source
        .stream(params.subpath.as_deref())
        .await?;

    // If cache is active for this path, write-through while streaming to client
    let stream_for_client: BoxStream<Result<Bytes, AppError>> = match cache {
        Some(c) => write_through_stream(c, resource_path, backend_stream, content_length),
        None => backend_stream,
    };

    Ok(build_response(stream_for_client, content_type, content_length))
}

/// Wrap the backend stream so each chunk is also forwarded to the cache.
fn write_through_stream(
    cache: Arc<dyn CacheStore>,
    cache_key: String,
    backend_stream: BoxStream<'static, Result<Bytes, AppError>>,
    content_length: Option<u64>,
) -> BoxStream<'static, Result<Bytes, AppError>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(4);

    tokio::spawn(async move {
        let mut rx = rx;
        let s: BoxStream<Result<Bytes, AppError>> = stream! {
            while let Some(chunk) = rx.recv().await {
                yield Ok(chunk);
            }
        }
        .boxed();
        if let Err(e) = cache.upload(&cache_key, s, content_length).await {
            tracing::warn!(error = ?e, "cache write failed");
        }
    });

    backend_stream
        .inspect(move |res| {
            if let Ok(chunk) = res {
                let _ = tx.try_send(chunk.clone());
            }
        })
        .boxed()
}

/// Build the resource path used for cache keys and EODAG resolution (e.g. `/sentinel-2/collection/item/asset[/subpath]`).
fn build_resource_path(params: &DataPathParams, subpath: Option<&str>) -> String {
    let mut path = format!(
        "/{}/{}/{}/{}",
        params.provider, params.collection_id, params.item_id, params.asset_key
    );
    if let Some(sp) = subpath {
        path.push('/');
        path.push_str(sp);
    }
    path
}

async fn create_backend_source(
    eodag_response: &EodagResponse,
    state: &AppState,
) -> Result<Arc<dyn ByteStreamSource + Send + Sync>, AppError> {
    match eodag_response {
        EodagResponse::S3 { .. } => {
            let s3_backend = S3BackendSource::from_eodag_response(
                state.s3_pool.clone(),
                eodag_response,
            )
            .await?;
            Ok(Arc::from(Box::new(s3_backend) as Box<dyn ByteStreamSource + Send + Sync>))
        }
        EodagResponse::Http { .. } => {
            let http_backend = HTTPBackendSource::from_eodag_response(
                &state.http_client,
                eodag_response,
            )?;
            Ok(Arc::from(Box::new(http_backend) as Box<dyn ByteStreamSource + Send + Sync>))
        }
    }
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


