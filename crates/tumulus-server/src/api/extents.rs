use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, head, post, put},
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio_util::io::StreamReader;

use crate::api::AppState;
use crate::storage::{Storage, StorageError};

pub fn router<S: Storage>() -> Router<AppState<S>> {
    Router::new()
        .route("/{id}", get(get_extent))
        .route("/{id}", put(put_extent))
        .route("/{id}", head(head_extent))
        .route("/check", post(check_extents))
}

/// GET /extents/:id - Download extent data (streamed)
async fn get_extent<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<Response, StorageError> {
    let id = parse_id(&id)?;

    // Get metadata first for Content-Length
    let meta = state.storage.extent_meta(&id).await?;

    // Get the stream
    let stream = state.storage.get_extent(&id).await?;

    // Convert our stream to an axum Body
    let body = Body::from_stream(stream);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, meta.size)
        .body(body)
        .unwrap())
}

/// PUT /extents/:id - Upload extent data (streamed)
async fn put_extent<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
    request: axum::extract::Request,
) -> Result<impl IntoResponse, StorageError> {
    let id = parse_id(&id)?;

    // Get Content-Length header for size hint
    let size_hint = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    // Convert the request body to an AsyncRead
    let body = request.into_body();
    let stream = body.into_data_stream();
    let stream = stream.map_err(std::io::Error::other);
    let reader = StreamReader::new(stream);

    let created = state
        .storage
        .put_extent(&id, Box::new(reader), size_hint)
        .await?;

    if created {
        Ok(StatusCode::CREATED)
    } else {
        Ok(StatusCode::OK) // Already existed
    }
}

/// HEAD /extents/:id - Check if extent exists
async fn head_extent<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, StorageError> {
    let id = parse_id(&id)?;
    let meta = state.storage.extent_meta(&id).await?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, meta.size)
        .body(Body::empty())
        .unwrap())
}

#[derive(Deserialize)]
struct CheckRequest {
    ids: Vec<String>,
}

#[derive(Serialize)]
struct CheckResponse {
    exists: Vec<bool>,
}

/// POST /extents/check - Batch check which extents exist
async fn check_extents<S: Storage>(
    State(state): State<AppState<S>>,
    Json(req): Json<CheckRequest>,
) -> Result<impl IntoResponse, StorageError> {
    let ids: Result<Vec<[u8; 32]>, _> = req.ids.iter().map(|s| parse_id(s)).collect();
    let ids = ids?;
    let exists = state.storage.extents_exist(&ids).await?;
    Ok(Json(CheckResponse { exists }))
}

fn parse_id(s: &str) -> Result<[u8; 32], StorageError> {
    let bytes = hex::decode(s).map_err(|_| StorageError::InvalidData("invalid hex".into()))?;
    bytes
        .try_into()
        .map_err(|_| StorageError::InvalidData("ID must be 32 bytes".into()))
}
