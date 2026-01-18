use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, head, put},
};

use crate::api::AppState;
use crate::storage::{Storage, StorageError};

pub fn router<S: Storage>() -> Router<AppState<S>> {
    Router::new()
        .route("/{id}", get(get_blob))
        .route("/{id}", put(put_blob))
        .route("/{id}", head(head_blob))
}

/// GET /blobs/:id - Download blob layout
async fn get_blob<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, StorageError> {
    let id = parse_id(&id)?;
    let data = state.storage.get_blob(&id).await?;

    Ok((
        StatusCode::OK,
        [("content-type", "application/octet-stream")],
        data,
    ))
}

/// PUT /blobs/:id - Upload blob layout
async fn put_blob<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, StorageError> {
    let id = parse_id(&id)?;
    let created = state.storage.put_blob(&id, body).await?;

    if created {
        Ok(StatusCode::CREATED)
    } else {
        Ok(StatusCode::OK) // Already existed
    }
}

/// HEAD /blobs/:id - Check if blob exists
async fn head_blob<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, StorageError> {
    let id = parse_id(&id)?;
    if state.storage.blob_exists(&id).await? {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

fn parse_id(s: &str) -> Result<[u8; 32], StorageError> {
    let bytes = hex::decode(s).map_err(|_| StorageError::InvalidData("invalid hex".into()))?;
    bytes
        .try_into()
        .map_err(|_| StorageError::InvalidData("ID must be 32 bytes".into()))
}
