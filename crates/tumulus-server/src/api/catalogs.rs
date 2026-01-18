//! Catalog upload API handlers.
//!
//! Implements the catalog upload flow:
//! - POST /catalog - Initiate upload with catalog ID + checksum
//! - PUT /catalog/:id - Upload catalog data
//! - POST /catalog/:id - Finalize upload, check for missing extents

use std::io::{BufReader, Write};
use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
};
use bytes::Buf;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::api::AppState;
use crate::blob::BlobLayout;
use crate::db::CatalogStatus;
use crate::storage::{Storage, StorageError};

/// Request body for initiating a catalog upload.
#[derive(Debug, Deserialize)]
pub struct InitiateRequest {
    /// The catalog ID (UUID)
    pub id: Uuid,
    /// BLAKE3 checksum of the catalog file (hex-encoded)
    pub checksum: String,
}

/// Response for initiating a catalog upload.
#[derive(Debug, Serialize)]
pub struct InitiateResponse {
    /// The catalog ID to use for upload (may differ from request if conflict)
    pub id: String,
    /// Whether this is resuming an existing upload
    pub resuming: bool,
    /// If resuming, the list of extents still needed (hex-encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing_extents: Option<Vec<String>>,
}

/// Response for uploading a catalog.
#[derive(Debug, Serialize)]
pub struct UploadResponse {
    /// List of extent IDs that need to be uploaded (hex-encoded)
    pub missing_extents: Vec<String>,
}

/// Response for finalizing a catalog.
#[derive(Debug, Serialize)]
pub struct FinalizeResponse {
    /// If true, upload is complete. If false, extents are still missing.
    pub complete: bool,
    /// List of extent IDs still missing (hex-encoded), if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing_extents: Option<Vec<String>>,
}

pub fn router<S: Storage>() -> Router<AppState<S>> {
    Router::new()
        .route("/", get(list_catalogs))
        .route("/", post(initiate_upload))
        .route("/{id}", put(upload_catalog))
        .route("/{id}", post(finalize_upload))
}

/// GET /catalogs - List all complete catalogs
async fn list_catalogs<S: Storage>(
    State(state): State<AppState<S>>,
) -> Result<impl IntoResponse, StorageError> {
    let ids = state.storage.list_catalogs().await?;
    let ids: Vec<String> = ids.iter().map(|id| id.simple().to_string()).collect();
    Ok(Json(ids))
}

/// Result of checking catalog state in the database
enum CatalogCheckResult {
    /// Catalog exists with matching checksum, return extent IDs to check
    ResumeUpload { extent_ids: Vec<[u8; 32]> },
    /// Catalog exists with different checksum, use new ID
    NewId { new_id: Uuid },
    /// Catalog doesn't exist, created new entry
    Created,
}

/// POST /catalog - Initiate a catalog upload
///
/// Checks if the catalog ID exists:
/// - If exists with matching checksum → resuming upload
/// - If exists with different checksum → generate new ID
/// - Otherwise → create new entry
async fn initiate_upload<S: Storage>(
    State(state): State<AppState<S>>,
    Json(req): Json<InitiateRequest>,
) -> Result<impl IntoResponse, CatalogError> {
    let checksum = parse_checksum(&req.checksum)?;

    // Do all database operations without holding the lock across await
    let check_result = {
        let db = state.db.lock().unwrap();

        if let Some(existing) = db.get_catalog(req.id)? {
            if existing.checksum == checksum {
                // Resuming - get extent IDs to check
                let extent_ids = db.get_catalog_extents(req.id)?;
                CatalogCheckResult::ResumeUpload { extent_ids }
            } else {
                // Checksum mismatch - generate a new ID
                let new_id = db.generate_catalog_id();
                db.create_catalog(new_id, &checksum)?;
                CatalogCheckResult::NewId { new_id }
            }
        } else {
            // New catalog upload
            db.create_catalog(req.id, &checksum)?;
            CatalogCheckResult::Created
        }
    };

    match check_result {
        CatalogCheckResult::ResumeUpload { extent_ids } => {
            info!(catalog_id = %req.id, "Resuming catalog upload");

            // Now do async storage check outside of lock
            let missing = get_missing_extents_from_ids(&state.storage, extent_ids).await?;
            let missing_hex: Vec<String> = missing.iter().map(hex::encode).collect();

            Ok((
                StatusCode::OK,
                Json(InitiateResponse {
                    id: req.id.simple().to_string(),
                    resuming: true,
                    missing_extents: Some(missing_hex),
                }),
            ))
        }
        CatalogCheckResult::NewId { new_id } => {
            info!(
                old_id = %req.id,
                new_id = %new_id,
                "Catalog ID exists with different checksum, generating new ID"
            );

            Ok((
                StatusCode::SEE_OTHER,
                Json(InitiateResponse {
                    id: new_id.simple().to_string(),
                    resuming: false,
                    missing_extents: None,
                }),
            ))
        }
        CatalogCheckResult::Created => {
            info!(catalog_id = %req.id, "Initiating new catalog upload");

            Ok((
                StatusCode::OK,
                Json(InitiateResponse {
                    id: req.id.simple().to_string(),
                    resuming: false,
                    missing_extents: None,
                }),
            ))
        }
    }
}

/// Result of checking catalog for upload
enum UploadCheckResult {
    /// Catalog already uploaded, return existing extent IDs
    AlreadyUploaded { extent_ids: Vec<[u8; 32]> },
    /// Catalog pending, proceed with upload
    Pending { expected_checksum: [u8; 32] },
    /// Catalog not found
    NotFound,
}

/// PUT /catalog/:id - Upload catalog data
///
/// Receives the catalog file, verifies checksum, extracts blob/extent info,
/// and returns the list of extents that need to be uploaded.
async fn upload_catalog<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, CatalogError> {
    let catalog_id = parse_uuid(&id)?;

    // Get the expected checksum from the database (no await while holding lock)
    let check_result = {
        let db = state.db.lock().unwrap();
        match db.get_catalog(catalog_id)? {
            Some(info) => {
                if info.status != CatalogStatus::Pending {
                    // Catalog already uploaded, get extent IDs to check
                    let extent_ids = db.get_catalog_extents(catalog_id)?;
                    UploadCheckResult::AlreadyUploaded { extent_ids }
                } else {
                    UploadCheckResult::Pending {
                        expected_checksum: info.checksum,
                    }
                }
            }
            None => UploadCheckResult::NotFound,
        }
    };

    match check_result {
        UploadCheckResult::NotFound => Err(CatalogError::NotFound(catalog_id)),
        UploadCheckResult::AlreadyUploaded { extent_ids } => {
            // Just return missing extents
            let missing = get_missing_extents_from_ids(&state.storage, extent_ids).await?;
            let missing_hex: Vec<String> = missing.iter().map(hex::encode).collect();
            Ok(Json(UploadResponse {
                missing_extents: missing_hex,
            }))
        }
        UploadCheckResult::Pending { expected_checksum } => {
            // Verify the checksum
            let actual_checksum = blake3::hash(&body);
            if actual_checksum.as_bytes() != &expected_checksum {
                return Err(CatalogError::ChecksumMismatch {
                    expected: hex::encode(expected_checksum),
                    actual: actual_checksum.to_hex().to_string(),
                });
            }

            // Write the catalog to storage
            state
                .storage
                .put_catalog(catalog_id, body.clone())
                .await
                .map_err(CatalogError::Storage)?;

            // Decompress and parse the catalog to extract extent/blob info
            let (extent_ids, blob_layouts) = parse_catalog_contents(&body)?;

            info!(
                catalog_id = %catalog_id,
                extent_count = extent_ids.len(),
                blob_count = blob_layouts.len(),
                "Parsed catalog contents"
            );

            // Write all blob layouts to storage
            for (blob_id, layout) in &blob_layouts {
                let encoded = layout.encode();
                match state.storage.put_blob(blob_id, encoded).await {
                    Ok(created) => {
                        if created {
                            debug!(blob_id = %hex::encode(blob_id), "Stored new blob layout");
                        }
                    }
                    Err(e) => {
                        warn!(blob_id = %hex::encode(blob_id), error = %e, "Failed to store blob layout");
                    }
                }
            }

            // Batch check which extents already exist
            let exists = state
                .storage
                .extents_exist(&extent_ids)
                .await
                .map_err(CatalogError::Storage)?;

            // Filter to only missing extents
            let missing_extents: Vec<[u8; 32]> = extent_ids
                .into_iter()
                .zip(exists.iter())
                .filter_map(|(id, &exists)| if exists { None } else { Some(id) })
                .collect();

            info!(
                catalog_id = %catalog_id,
                missing_count = missing_extents.len(),
                "Identified missing extents"
            );

            // Store the missing extents in the database (sync, no await)
            {
                let db = state.db.lock().unwrap();
                db.set_catalog_extents(catalog_id, &missing_extents)?;
                db.update_status(catalog_id, CatalogStatus::Uploading)?;
            }

            let missing_hex: Vec<String> = missing_extents.iter().map(hex::encode).collect();

            Ok(Json(UploadResponse {
                missing_extents: missing_hex,
            }))
        }
    }
}

/// Result of checking catalog for finalization
enum FinalizeCheckResult {
    /// Already complete
    Complete,
    /// Need to check these extent IDs
    CheckExtents { extent_ids: Vec<[u8; 32]> },
    /// Not found
    NotFound,
}

/// POST /catalog/:id - Finalize catalog upload
///
/// Checks if all required extents are now present. If so, marks the catalog
/// as complete and returns 204. Otherwise, returns the list of still-missing extents.
async fn finalize_upload<S: Storage>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, CatalogError> {
    let catalog_id = parse_uuid(&id)?;

    // Check catalog state without holding lock across await
    let check_result = {
        let db = state.db.lock().unwrap();

        match db.get_catalog(catalog_id)? {
            Some(info) => {
                if info.status == CatalogStatus::Complete {
                    FinalizeCheckResult::Complete
                } else {
                    let extent_ids = db.get_catalog_extents(catalog_id)?;
                    FinalizeCheckResult::CheckExtents { extent_ids }
                }
            }
            None => FinalizeCheckResult::NotFound,
        }
    };

    match check_result {
        FinalizeCheckResult::NotFound => Err(CatalogError::NotFound(catalog_id)),
        FinalizeCheckResult::Complete => {
            Ok((StatusCode::NO_CONTENT, Json(None::<FinalizeResponse>)).into_response())
        }
        FinalizeCheckResult::CheckExtents { extent_ids } => {
            // Check which extents are still missing (async)
            let missing = get_missing_extents_from_ids(&state.storage, extent_ids).await?;

            if missing.is_empty() {
                // All extents are present, mark as complete
                {
                    let db = state.db.lock().unwrap();
                    db.update_status(catalog_id, CatalogStatus::Complete)?;
                }
                info!(catalog_id = %catalog_id, "Catalog upload complete");

                // TODO: Spawn task to update catalog index

                Ok((StatusCode::NO_CONTENT, Json(None::<FinalizeResponse>)).into_response())
            } else {
                // Some extents are still missing
                let missing_hex: Vec<String> = missing.iter().map(hex::encode).collect();
                info!(
                    catalog_id = %catalog_id,
                    missing_count = missing.len(),
                    "Catalog upload not yet complete"
                );

                Ok((
                    StatusCode::OK,
                    Json(Some(FinalizeResponse {
                        complete: false,
                        missing_extents: Some(missing_hex),
                    })),
                )
                    .into_response())
            }
        }
    }
}

/// Get the list of extents that are still missing given a list of extent IDs.
async fn get_missing_extents_from_ids<S: Storage>(
    storage: &Arc<S>,
    extent_ids: Vec<[u8; 32]>,
) -> Result<Vec<[u8; 32]>, CatalogError> {
    if extent_ids.is_empty() {
        return Ok(Vec::new());
    }

    let exists = storage
        .extents_exist(&extent_ids)
        .await
        .map_err(CatalogError::Storage)?;

    let missing: Vec<[u8; 32]> = extent_ids
        .into_iter()
        .zip(exists.iter())
        .filter_map(|(id, &exists)| if exists { None } else { Some(id) })
        .collect();

    Ok(missing)
}

/// Parse a catalog file (possibly zstd-compressed) and extract extent/blob info.
fn parse_catalog_contents(
    data: &[u8],
) -> Result<(Vec<[u8; 32]>, Vec<([u8; 32], BlobLayout)>), CatalogError> {
    // Check if the data is zstd-compressed
    let is_compressed = data.len() >= 4 && data[0..4] == [0x28, 0xB5, 0x2F, 0xFD];

    // Decompress if needed
    let temp_file = if is_compressed {
        let mut temp = NamedTempFile::new().map_err(CatalogError::Io)?;
        let reader = BufReader::new(data.reader());
        let mut decoder = zstd::stream::Decoder::new(reader).map_err(CatalogError::Io)?;
        std::io::copy(&mut decoder, &mut temp).map_err(CatalogError::Io)?;
        temp.flush().map_err(CatalogError::Io)?;
        temp
    } else {
        let mut temp = NamedTempFile::new().map_err(CatalogError::Io)?;
        temp.write_all(data).map_err(CatalogError::Io)?;
        temp.flush().map_err(CatalogError::Io)?;
        temp
    };

    // Open as SQLite database
    let conn = Connection::open(temp_file.path()).map_err(|e| {
        CatalogError::InvalidCatalog(format!("Failed to open catalog database: {}", e))
    })?;

    // Extract all unique extent IDs (non-null extent_id from blob_extents)
    let mut extent_ids: Vec<[u8; 32]> = Vec::new();
    {
        let mut stmt = conn
            .prepare("SELECT DISTINCT extent_id FROM blob_extents WHERE extent_id IS NOT NULL")
            .map_err(|e| CatalogError::InvalidCatalog(format!("Failed to query extents: {}", e)))?;

        let rows = stmt
            .query_map([], |row| {
                let extent_id: Vec<u8> = row.get(0)?;
                Ok(extent_id)
            })
            .map_err(|e| CatalogError::InvalidCatalog(format!("Failed to query extents: {}", e)))?;

        for row in rows {
            let extent_id: Vec<u8> = row.map_err(|e| {
                CatalogError::InvalidCatalog(format!("Failed to read extent: {}", e))
            })?;
            let extent_id: [u8; 32] = extent_id
                .try_into()
                .map_err(|_| CatalogError::InvalidCatalog("Invalid extent ID size".to_string()))?;
            extent_ids.push(extent_id);
        }
    }

    // Extract all blobs with their extent mappings
    let mut blob_layouts: Vec<([u8; 32], BlobLayout)> = Vec::new();
    {
        let mut blob_stmt = conn
            .prepare("SELECT blob_id, bytes FROM blobs")
            .map_err(|e| CatalogError::InvalidCatalog(format!("Failed to query blobs: {}", e)))?;

        let blob_rows = blob_stmt
            .query_map([], |row| {
                let blob_id: Vec<u8> = row.get(0)?;
                let bytes: i64 = row.get(1)?;
                Ok((blob_id, bytes as u64))
            })
            .map_err(|e| CatalogError::InvalidCatalog(format!("Failed to query blobs: {}", e)))?;

        for blob_row in blob_rows {
            let (blob_id_vec, total_bytes) = blob_row
                .map_err(|e| CatalogError::InvalidCatalog(format!("Failed to read blob: {}", e)))?;

            let blob_id: [u8; 32] = blob_id_vec
                .try_into()
                .map_err(|_| CatalogError::InvalidCatalog("Invalid blob ID size".to_string()))?;

            // Get extents for this blob
            let mut extent_stmt = conn
                .prepare("SELECT extent_id, offset, bytes FROM blob_extents WHERE blob_id = ?1 AND extent_id IS NOT NULL ORDER BY offset")
                .map_err(|e| CatalogError::InvalidCatalog(format!("Failed to query blob extents: {}", e)))?;

            let extent_rows = extent_stmt
                .query_map([blob_id.as_slice()], |row| {
                    let extent_id: Vec<u8> = row.get(0)?;
                    let offset: i64 = row.get(1)?;
                    let bytes: i64 = row.get(2)?;
                    Ok((extent_id, offset as u64, bytes as u64))
                })
                .map_err(|e| {
                    CatalogError::InvalidCatalog(format!("Failed to query blob extents: {}", e))
                })?;

            let mut extents = Vec::new();
            for extent_row in extent_rows {
                let (extent_id_vec, offset, length) = extent_row.map_err(|e| {
                    CatalogError::InvalidCatalog(format!("Failed to read blob extent: {}", e))
                })?;

                let extent_id: [u8; 32] = extent_id_vec.try_into().map_err(|_| {
                    CatalogError::InvalidCatalog(
                        "Invalid extent ID size in blob_extents".to_string(),
                    )
                })?;

                extents.push(crate::blob::BlobExtent {
                    offset,
                    length,
                    extent_id,
                });
            }

            blob_layouts.push((
                blob_id,
                BlobLayout {
                    total_bytes,
                    extents,
                },
            ));
        }
    }

    Ok((extent_ids, blob_layouts))
}

fn parse_uuid(s: &str) -> Result<Uuid, CatalogError> {
    Uuid::parse_str(s).map_err(|_| CatalogError::InvalidUuid(s.to_string()))
}

fn parse_checksum(s: &str) -> Result<[u8; 32], CatalogError> {
    let bytes = hex::decode(s).map_err(|_| CatalogError::InvalidChecksum(s.to_string()))?;
    bytes
        .try_into()
        .map_err(|_| CatalogError::InvalidChecksum(s.to_string()))
}

/// Error type for catalog operations.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("Catalog not found: {0}")]
    NotFound(Uuid),

    #[error("Invalid UUID: {0}")]
    InvalidUuid(String),

    #[error("Invalid checksum: {0}")]
    InvalidChecksum(String),

    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("Invalid catalog format: {0}")]
    InvalidCatalog(String),

    #[error("Database error: {0}")]
    Database(#[from] crate::db::DbError),

    #[error("Storage error: {0}")]
    Storage(StorageError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl IntoResponse for CatalogError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;

        let (status, error, detail) = match &self {
            CatalogError::NotFound(_) => (StatusCode::NOT_FOUND, "Catalog not found", None),
            CatalogError::InvalidUuid(s) => {
                (StatusCode::BAD_REQUEST, "Invalid UUID", Some(s.clone()))
            }
            CatalogError::InvalidChecksum(s) => {
                (StatusCode::BAD_REQUEST, "Invalid checksum", Some(s.clone()))
            }
            CatalogError::ChecksumMismatch { expected, actual } => (
                StatusCode::BAD_REQUEST,
                "Checksum mismatch",
                Some(format!("expected {}, got {}", expected, actual)),
            ),
            CatalogError::InvalidCatalog(msg) => (
                StatusCode::BAD_REQUEST,
                "Invalid catalog",
                Some(msg.clone()),
            ),
            CatalogError::Database(e) => {
                error!(error = %e, "Database error");
                (StatusCode::INTERNAL_SERVER_ERROR, "Database error", None)
            }
            CatalogError::Storage(e) => {
                error!(error = %e, "Storage error");
                (StatusCode::INTERNAL_SERVER_ERROR, "Storage error", None)
            }
            CatalogError::Io(e) => {
                error!(error = %e, "I/O error");
                (StatusCode::INTERNAL_SERVER_ERROR, "I/O error", None)
            }
        };

        let body = crate::api::ErrorResponse {
            error: error.to_string(),
            detail,
        };

        (status, Json(body)).into_response()
    }
}
