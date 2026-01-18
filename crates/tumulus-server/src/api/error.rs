use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::storage::StorageError;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl IntoResponse for StorageError {
    fn into_response(self) -> Response {
        let (status, error, detail) = match &self {
            StorageError::NotFound => (StatusCode::NOT_FOUND, "Not found", None),
            StorageError::HashMismatch { expected, actual } => (
                StatusCode::BAD_REQUEST,
                "Hash mismatch",
                Some(format!("expected {}, got {}", expected, actual)),
            ),
            StorageError::InvalidData(msg) => {
                (StatusCode::BAD_REQUEST, "Invalid data", Some(msg.clone()))
            }
            StorageError::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal error", None),
        };

        let body = ErrorResponse {
            error: error.to_string(),
            detail,
        };

        (status, Json(body)).into_response()
    }
}
