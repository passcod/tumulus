//! tumulus-upload - Upload catalogs to a tumulus server.
//!
//! This binary takes a catalog file, verifies it matches the local machine,
//! and uploads it to a tumulus server.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use clap::Parser;
use lloggs::LoggingArgs;
use reqwest::blocking::Client;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use tumulus::open_catalog;

#[derive(Parser)]
#[command(name = "tumulus-upload")]
#[command(about = "Upload a catalog to a tumulus server")]
struct Args {
    /// Path to the catalog file to upload
    catalog: PathBuf,

    /// Server URL (e.g., http://localhost:3000)
    #[arg(long, short)]
    server: String,

    /// Skip machine ID verification
    #[arg(long)]
    skip_machine_check: bool,

    /// Override the source path from the catalog with a different path
    #[arg(long)]
    override_source: Option<PathBuf>,

    #[command(flatten)]
    logging: LoggingArgs,
}

/// Request body for initiating a catalog upload.
#[derive(Debug, Serialize)]
struct InitiateRequest {
    id: Uuid,
    checksum: String,
}

/// Response from initiating a catalog upload.
#[derive(Debug, Deserialize)]
struct InitiateResponse {
    id: String,
    resuming: bool,
    #[serde(default)]
    missing_extents: Option<Vec<String>>,
}

/// Response from uploading a catalog.
#[derive(Debug, Deserialize)]
struct UploadResponse {
    missing_extents: Vec<String>,
}

/// Response from finalizing a catalog.
#[derive(Debug, Deserialize)]
struct FinalizeResponse {
    complete: bool,
    #[serde(default)]
    missing_extents: Option<Vec<String>>,
}

/// Error response from the server.
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: String,
    #[serde(default)]
    detail: Option<String>,
}

#[derive(Debug, thiserror::Error)]
enum UploadError {
    #[error("Failed to open catalog: {0}")]
    OpenCatalog(String),

    #[error("Machine ID mismatch: catalog is for '{catalog}', but this machine is '{local}'")]
    MachineIdMismatch { catalog: String, local: String },

    #[error("Source path does not exist: {0}")]
    SourcePathNotFound(PathBuf),

    #[error("Missing metadata in catalog: {0}")]
    MissingMetadata(String),

    #[error("Invalid metadata value: {0}")]
    InvalidMetadata(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Server error: {error}{}", detail.as_ref().map(|d| format!(" - {}", d)).unwrap_or_default())]
    Server {
        error: String,
        detail: Option<String>,
    },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error(
        "Catalog ID changed by server from {original} to {new}. Please update the catalog and retry."
    )]
    IdChanged { original: Uuid, new: Uuid },

    #[error(
        "Extent {extent_id} has changed on disk: expected hash {expected}, got {actual}. Aborting upload."
    )]
    ExtentChanged {
        extent_id: String,
        expected: String,
        actual: String,
    },

    #[error("Extent {extent_id} not found in catalog")]
    ExtentNotInCatalog { extent_id: String },

    #[error("File not found for extent {extent_id}: {path}")]
    FileNotFound { extent_id: String, path: PathBuf },
}

/// Metadata extracted from the catalog.
struct CatalogMetadata {
    id: Uuid,
    machine_id: String,
    source_path: Option<PathBuf>,
}

/// Information about where to find an extent on disk.
#[derive(Debug, Clone)]
struct ExtentLocation {
    /// Relative path to the file containing this extent
    file_path: String,
    /// Offset within the file where the extent starts
    offset: u64,
    /// Length of the extent in bytes
    length: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let _guard = args.logging.setup(|v| match v {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    })?;

    if let Err(e) = run(args) {
        error!("{}", e);
        std::process::exit(1);
    }

    Ok(())
}

fn run(args: Args) -> Result<(), UploadError> {
    info!(catalog = ?args.catalog, server = %args.server, "Starting catalog upload");

    // Open and read catalog metadata
    let (conn, _tempfile) =
        open_catalog(&args.catalog).map_err(|e| UploadError::OpenCatalog(e.to_string()))?;

    let metadata = read_catalog_metadata(&conn)?;
    info!(
        catalog_id = %metadata.id,
        machine_id = %metadata.machine_id,
        source_path = ?metadata.source_path,
        "Read catalog metadata"
    );

    // Verify machine ID matches
    if !args.skip_machine_check {
        let local_machine_id = tumulus::get_machine_id()
            .map_err(|e| UploadError::OpenCatalog(format!("Failed to get machine ID: {}", e)))?;

        if metadata.machine_id != local_machine_id {
            return Err(UploadError::MachineIdMismatch {
                catalog: metadata.machine_id,
                local: local_machine_id,
            });
        }
        debug!("Machine ID verified");
    } else {
        warn!("Skipping machine ID verification");
    }

    // Determine the source path to use
    let source_path = if let Some(ref override_path) = args.override_source {
        info!(
            catalog_path = ?metadata.source_path,
            override_path = ?override_path,
            "Using overridden source path"
        );
        override_path.clone()
    } else if let Some(ref catalog_path) = metadata.source_path {
        catalog_path.clone()
    } else {
        return Err(UploadError::MissingMetadata(
            "source_path (use --override-source to specify one)".to_string(),
        ));
    };

    // Verify source path exists
    if !source_path.exists() {
        return Err(UploadError::SourcePathNotFound(source_path));
    }
    debug!(path = ?source_path, "Source path verified");

    // Build extent location map from catalog
    let extent_locations = build_extent_location_map(&conn)?;
    info!(
        extent_count = extent_locations.len(),
        "Built extent location map"
    );

    // Compute checksum of the catalog file
    let catalog_data = fs::read(&args.catalog)?;
    let checksum = blake3::hash(&catalog_data);
    let checksum_hex = checksum.to_hex().to_string();
    info!(checksum = %checksum_hex, size = catalog_data.len(), "Computed catalog checksum");

    // Create HTTP client
    let client = Client::new();
    let server_url = args.server.trim_end_matches('/');

    // Step 1: Initiate upload
    info!("Initiating upload with server");
    let initiate_resp = initiate_upload(&client, server_url, metadata.id, &checksum_hex)?;

    // Check if server assigned a different ID
    let server_id = Uuid::parse_str(&initiate_resp.id).map_err(|_| {
        UploadError::InvalidMetadata(format!("Invalid UUID from server: {}", initiate_resp.id))
    })?;

    if server_id != metadata.id {
        return Err(UploadError::IdChanged {
            original: metadata.id,
            new: server_id,
        });
    }

    let missing_extents = if initiate_resp.resuming {
        info!(
            missing_count = initiate_resp
                .missing_extents
                .as_ref()
                .map(|v| v.len())
                .unwrap_or(0),
            "Resuming existing upload"
        );
        initiate_resp.missing_extents.unwrap_or_default()
    } else {
        // Step 2: Upload the catalog data
        info!("Uploading catalog data");
        let upload_resp = upload_catalog(&client, server_url, server_id, &catalog_data)?;
        info!(
            missing_count = upload_resp.missing_extents.len(),
            "Catalog uploaded"
        );
        upload_resp.missing_extents
    };

    // Step 3 & 4: Upload extents and finalize in a loop until complete
    let mut current_missing = missing_extents;
    let mut attempt = 0;

    loop {
        attempt += 1;

        // Upload missing extents
        if !current_missing.is_empty() {
            info!(
                attempt,
                count = current_missing.len(),
                "Uploading missing extents"
            );

            upload_extents(
                &client,
                server_url,
                &current_missing,
                &extent_locations,
                &source_path,
            )?;

            info!(
                attempt,
                count = current_missing.len(),
                "Finished uploading extents"
            );
        }

        // Try to finalize
        info!(attempt, "Finalizing upload");
        let finalize_resp = finalize_upload(&client, server_url, server_id)?;

        match finalize_resp {
            None => {
                // 204 No Content - success!
                break;
            }
            Some(resp) if resp.complete => {
                // Explicitly complete
                break;
            }
            Some(resp) => {
                // Not complete, get the new list of missing extents
                current_missing = resp.missing_extents.unwrap_or_default();
                warn!(
                    attempt,
                    missing_count = current_missing.len(),
                    "Finalization reported missing extents, continuing upload"
                );

                if current_missing.is_empty() {
                    // Server said not complete but no missing extents? Weird, but treat as done
                    warn!(
                        "Server reported incomplete but no missing extents, treating as complete"
                    );
                    break;
                }
            }
        }
    }

    info!(catalog_id = %server_id, "Upload complete!");
    Ok(())
}

fn read_catalog_metadata(conn: &Connection) -> Result<CatalogMetadata, UploadError> {
    // Read catalog ID
    let id_str: String = conn
        .query_row("SELECT value FROM metadata WHERE key = 'id'", [], |row| {
            row.get(0)
        })
        .map_err(|_| UploadError::MissingMetadata("id".to_string()))?;

    // Parse the JSON string value
    let id_str: String = serde_json::from_str(&id_str)
        .map_err(|_| UploadError::InvalidMetadata(format!("Invalid id value: {}", id_str)))?;

    let id = Uuid::parse_str(&id_str)
        .map_err(|_| UploadError::InvalidMetadata(format!("Invalid UUID: {}", id_str)))?;

    // Read machine ID
    let machine_str: String = conn
        .query_row(
            "SELECT value FROM metadata WHERE key = 'machine'",
            [],
            |row| row.get(0),
        )
        .map_err(|_| UploadError::MissingMetadata("machine".to_string()))?;

    let machine_id: String = serde_json::from_str(&machine_str).map_err(|_| {
        UploadError::InvalidMetadata(format!("Invalid machine value: {}", machine_str))
    })?;

    // Read source path (optional)
    let source_path: Option<PathBuf> = conn
        .query_row(
            "SELECT value FROM metadata WHERE key = 'source_path'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .and_then(|s| serde_json::from_str::<String>(&s).ok())
        .map(PathBuf::from);

    Ok(CatalogMetadata {
        id,
        machine_id,
        source_path,
    })
}

/// Build a map from extent ID (hex) to its location on disk.
///
/// This queries the catalog to find all extents and which files contain them.
fn build_extent_location_map(
    conn: &Connection,
) -> Result<HashMap<String, ExtentLocation>, UploadError> {
    let mut map = HashMap::new();

    // Query to find extent locations:
    // For each extent, find a file that contains it via:
    // files.blob_id -> blob_extents.blob_id -> blob_extents.extent_id
    let mut stmt = conn.prepare(
        r#"
        SELECT
            hex(be.extent_id) as extent_id,
            f.path,
            be.offset,
            be.bytes
        FROM blob_extents be
        JOIN files f ON f.blob_id = be.blob_id
        WHERE be.extent_id IS NOT NULL
        "#,
    )?;

    let rows = stmt.query_map([], |row| {
        let extent_id: String = row.get(0)?;
        let path_bytes: Vec<u8> = row.get(1)?;
        let offset: i64 = row.get(2)?;
        let bytes: i64 = row.get(3)?;

        Ok((extent_id, path_bytes, offset as u64, bytes as u64))
    })?;

    for row in rows {
        let (extent_id, path_bytes, offset, length) = row?;

        // Convert path bytes to string
        let file_path = String::from_utf8_lossy(&path_bytes).to_string();

        // Only insert if we don't already have this extent
        // (multiple files might reference the same extent due to dedup)
        map.entry(extent_id.to_lowercase())
            .or_insert(ExtentLocation {
                file_path,
                offset,
                length,
            });
    }

    Ok(map)
}

fn initiate_upload(
    client: &Client,
    server_url: &str,
    catalog_id: Uuid,
    checksum: &str,
) -> Result<InitiateResponse, UploadError> {
    let url = format!("{}/catalogs", server_url);
    let req = InitiateRequest {
        id: catalog_id,
        checksum: checksum.to_string(),
    };

    let resp = client.post(&url).json(&req).send()?;

    if !resp.status().is_success() && resp.status().as_u16() != 303 {
        let error_resp: ErrorResponse = resp.json()?;
        return Err(UploadError::Server {
            error: error_resp.error,
            detail: error_resp.detail,
        });
    }

    let initiate_resp: InitiateResponse = resp.json()?;
    Ok(initiate_resp)
}

fn upload_catalog(
    client: &Client,
    server_url: &str,
    catalog_id: Uuid,
    data: &[u8],
) -> Result<UploadResponse, UploadError> {
    let url = format!("{}/catalogs/{}", server_url, catalog_id.simple());

    let resp = client
        .put(&url)
        .header("Content-Type", "application/octet-stream")
        .body(data.to_vec())
        .send()?;

    if !resp.status().is_success() {
        let error_resp: ErrorResponse = resp.json()?;
        return Err(UploadError::Server {
            error: error_resp.error,
            detail: error_resp.detail,
        });
    }

    let upload_resp: UploadResponse = resp.json()?;
    Ok(upload_resp)
}

/// Upload a list of extents to the server.
///
/// For each extent:
/// 1. Look up its location in the catalog
/// 2. Read from the source file at the specified offset
/// 3. Compute BLAKE3 hash while reading
/// 4. If hash doesn't match, abort the entire upload
/// 5. Stream data to server
fn upload_extents(
    client: &Client,
    server_url: &str,
    extent_ids: &[String],
    extent_locations: &HashMap<String, ExtentLocation>,
    source_path: &Path,
) -> Result<(), UploadError> {
    for (i, extent_id_hex) in extent_ids.iter().enumerate() {
        let extent_id_lower = extent_id_hex.to_lowercase();

        // Find the extent location in our map
        let location = extent_locations.get(&extent_id_lower).ok_or_else(|| {
            UploadError::ExtentNotInCatalog {
                extent_id: extent_id_hex.clone(),
            }
        })?;

        debug!(
            extent = %extent_id_hex,
            file = %location.file_path,
            offset = location.offset,
            length = location.length,
            progress = format!("{}/{}", i + 1, extent_ids.len()),
            "Uploading extent"
        );

        // Construct full path to the file
        let file_path = source_path.join(&location.file_path);

        if !file_path.exists() {
            return Err(UploadError::FileNotFound {
                extent_id: extent_id_hex.clone(),
                path: file_path,
            });
        }

        // Read the extent data and compute hash
        let extent_data = read_extent_with_hash_check(
            &file_path,
            location.offset,
            location.length,
            extent_id_hex,
        )?;

        // Upload to server
        upload_extent(client, server_url, extent_id_hex, &extent_data)?;

        if (i + 1) % 100 == 0 || i + 1 == extent_ids.len() {
            info!(
                progress = format!("{}/{}", i + 1, extent_ids.len()),
                "Extent upload progress"
            );
        }
    }

    Ok(())
}

/// Read extent data from a file and verify the hash matches.
///
/// Returns the extent data if the hash matches, or an error if it doesn't.
fn read_extent_with_hash_check(
    file_path: &Path,
    offset: u64,
    length: u64,
    expected_hash_hex: &str,
) -> Result<Vec<u8>, UploadError> {
    let mut file = File::open(file_path)?;

    // Seek to the extent offset
    file.seek(SeekFrom::Start(offset))?;

    // Read the extent data
    let mut data = vec![0u8; length as usize];
    file.read_exact(&mut data)?;

    // Compute the BLAKE3 hash
    let actual_hash = blake3::hash(&data);
    let actual_hash_hex = actual_hash.to_hex().to_string();

    // Compare (case-insensitive)
    if actual_hash_hex.to_lowercase() != expected_hash_hex.to_lowercase() {
        return Err(UploadError::ExtentChanged {
            extent_id: expected_hash_hex.to_string(),
            expected: expected_hash_hex.to_string(),
            actual: actual_hash_hex,
        });
    }

    Ok(data)
}

/// Upload a single extent to the server.
fn upload_extent(
    client: &Client,
    server_url: &str,
    extent_id: &str,
    data: &[u8],
) -> Result<(), UploadError> {
    let url = format!("{}/extents/{}", server_url, extent_id.to_lowercase());

    let resp = client
        .put(&url)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", data.len())
        .body(data.to_vec())
        .send()?;

    // 200 OK = already existed, 201 Created = newly stored
    if !resp.status().is_success() {
        let error_resp: ErrorResponse = resp.json()?;
        return Err(UploadError::Server {
            error: error_resp.error,
            detail: error_resp.detail,
        });
    }

    Ok(())
}

fn finalize_upload(
    client: &Client,
    server_url: &str,
    catalog_id: Uuid,
) -> Result<Option<FinalizeResponse>, UploadError> {
    let url = format!("{}/catalogs/{}", server_url, catalog_id.simple());

    let resp = client.post(&url).send()?;

    if resp.status().as_u16() == 204 {
        // Success, no content
        return Ok(None);
    }

    if !resp.status().is_success() {
        let error_resp: ErrorResponse = resp.json()?;
        return Err(UploadError::Server {
            error: error_resp.error,
            detail: error_resp.detail,
        });
    }

    let finalize_resp: FinalizeResponse = resp.json()?;
    Ok(Some(finalize_resp))
}
