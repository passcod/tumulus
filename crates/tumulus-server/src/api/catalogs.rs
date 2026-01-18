use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, head, put},
};
use uuid::Uuid;

use crate::api::AppState;
use crate::storage::{Storage, StorageError};

pub fn router<S: Storage>() -> Router<AppState<S>> {
    Router::new()
        .route("/", get(list_catalogs))
        .route("/{id}", get(get_catalog))
        .route("/{id}", put(put_catalog))
        .route("/{id}", head(head_catalog))
}

/// GET /catalogs - List all catalogs
async fn list_catalogs<S: Storage>(
    State(state): State<AppState<S>>,
) -> Result<impl IntoResponse, StorageError> {
    let ids = state.storage.list_catalogs().await?;
    let ids: Vec<String> = ids.iter().map(|id| id.simple().to_string()).collect();
    Ok(Json(ids))
}

/// GET /catalogs/:id - Download catalog
async fn get_catalog<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, StorageError> {
    let id = parse_uuid(&id)?;
    let data = state.storage.get_catalog(id).await?;
    Ok((StatusCode::OK, data))
}

/// PUT /catalogs/:id - Upload catalog
async fn put_catalog<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, StorageError> {
    let id = parse_uuid(&id)?;
    state.storage.put_catalog(id, body).await?;
    Ok(StatusCode::CREATED)
}

/// HEAD /catalogs/:id - Check if catalog exists
async fn head_catalog<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, StorageError> {
    let id = parse_uuid(&id)?;
    if state.storage.catalog_exists(id).await? {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

fn parse_uuid(s: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(s).map_err(|_| StorageError::InvalidData("invalid UUID".into()))
}
