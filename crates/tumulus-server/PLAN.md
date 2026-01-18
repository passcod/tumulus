# tumulus-server Implementation Plan

## Overview

A stateless HTTP server that stores and serves extents, blobs, and catalogs. Uses axum for the HTTP layer and abstracts storage behind a trait for future S3 support.

### Design Principles

1. **Stateless** - No in-memory state between requests; everything persisted to storage backend
2. **Idempotent** - PUT operations succeed if object already exists with same content
3. **Horizontally scalable** - Multiple instances can run against shared storage
4. **Backend agnostic** - Storage abstracted behind a trait (filesystem first, S3 later)
5. **Streaming** - Extents are streamed in/out to handle large sizes (128KB to hundreds of MB)

---

## Crate Structure

```
tumulus-server/
├── Cargo.toml
└── src/
    ├── lib.rs              # Re-exports, shared types
    ├── main.rs             # CLI entry point, starts server
    ├── config.rs           # Server configuration
    ├── blob.rs             # Blob layout binary format encode/decode
    ├── storage.rs          # Storage trait definition + re-exports
    ├── storage/
    │   ├── types.rs        # StorageError, ObjectMeta
    │   └── fs.rs           # Filesystem backend implementation
    ├── api.rs              # Router setup, shared state, re-exports
    └── api/
        ├── error.rs        # API error types → HTTP responses
        ├── extents.rs      # GET/PUT/HEAD /extents/:id
        ├── blobs.rs        # GET/PUT/HEAD /blobs/:id
        └── catalogs.rs     # GET/PUT/HEAD/LIST /catalogs/:id
```

---

## Phase 1: Storage Types & Trait

### `storage/types.rs`

```rust
use std::io;
use std::time::SystemTime;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Object not found")]
    NotFound,

    #[error("Hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("Invalid data: {0}")]
    InvalidData(String),
}

/// Metadata about a stored object
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub size: u64,
    pub created: Option<SystemTime>,
}
```

### `storage.rs` - The Storage Trait

```rust
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use tokio::io::AsyncRead;
use uuid::Uuid;

mod types;
mod fs;

pub use types::{ObjectMeta, StorageError};
pub use fs::FsStorage;

/// A boxed stream of byte chunks for streaming reads
pub type ByteStream = Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send + Unpin>;

/// A boxed async reader for streaming writes
pub type ByteReader = Box<dyn AsyncRead + Send + Unpin>;

#[async_trait]
pub trait Storage: Send + Sync + 'static {
    // --- Extents ---

    /// Store extent data from a stream.
    /// Returns Ok(true) if newly stored, Ok(false) if already existed.
    /// MUST verify that BLAKE3(data) == id, return HashMismatch if not.
    /// The `size_hint` is optional but helps with pre-allocation.
    async fn put_extent(
        &self,
        id: &[u8; 32],
        data: ByteReader,
        size_hint: Option<u64>,
    ) -> Result<bool, StorageError>;

    /// Get extent data as a stream.
    /// Returns a stream of chunks for efficient memory usage with large extents.
    async fn get_extent(&self, id: &[u8; 32]) -> Result<ByteStream, StorageError>;

    /// Get extent data as a complete buffer (convenience for small extents).
    /// Default implementation collects from the stream.
    async fn get_extent_bytes(&self, id: &[u8; 32]) -> Result<Bytes, StorageError> {
        use futures::StreamExt;
        let mut stream = self.get_extent(id).await?;
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next().await {
            chunks.push(chunk?);
        }
        let total: Vec<u8> = chunks.into_iter().flat_map(|b| b.to_vec()).collect();
        Ok(Bytes::from(total))
    }

    /// Check if extent exists.
    async fn extent_exists(&self, id: &[u8; 32]) -> Result<bool, StorageError>;

    /// Batch check which extents exist (optimization for sync).
    /// Returns a Vec<bool> in the same order as input IDs.
    async fn extents_exist(&self, ids: &[[u8; 32]]) -> Result<Vec<bool>, StorageError>;

    /// Get extent metadata without fetching data.
    async fn extent_meta(&self, id: &[u8; 32]) -> Result<ObjectMeta, StorageError>;

    // --- Blobs ---

    /// Store blob layout data.
    /// Returns Ok(true) if newly stored, Ok(false) if already existed.
    async fn put_blob(&self, id: &[u8; 32], data: Bytes) -> Result<bool, StorageError>;

    /// Get blob layout by ID.
    async fn get_blob(&self, id: &[u8; 32]) -> Result<Bytes, StorageError>;

    /// Check if blob exists.
    async fn blob_exists(&self, id: &[u8; 32]) -> Result<bool, StorageError>;

    /// Get blob metadata without fetching data.
    async fn blob_meta(&self, id: &[u8; 32]) -> Result<ObjectMeta, StorageError>;

    // --- Catalogs ---

    /// Store a catalog file.
    async fn put_catalog(&self, id: Uuid, data: Bytes) -> Result<(), StorageError>;

    /// Get catalog by ID.
    async fn get_catalog(&self, id: Uuid) -> Result<Bytes, StorageError>;

    /// Check if catalog exists.
    async fn catalog_exists(&self, id: Uuid) -> Result<bool, StorageError>;

    /// Get catalog metadata without fetching data.
    async fn catalog_meta(&self, id: Uuid) -> Result<ObjectMeta, StorageError>;

    /// List all catalog IDs.
    async fn list_catalogs(&self) -> Result<Vec<Uuid>, StorageError>;
}
```

---

## Phase 2: Filesystem Backend

### `storage/fs.rs`

```rust
use std::path::{Path, PathBuf};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::storage::{ByteReader, ByteStream, ObjectMeta, Storage, StorageError};

pub struct FsStorage {
    base_path: PathBuf,
}

impl FsStorage {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self { base_path: base_path.into() }
    }

    /// Initialize directory structure
    pub async fn init(&self) -> Result<(), StorageError> {
        fs::create_dir_all(self.base_path.join("extents")).await?;
        fs::create_dir_all(self.base_path.join("blobs")).await?;
        fs::create_dir_all(self.base_path.join("catalogs")).await?;
        Ok(())
    }

    /// Convert a 32-byte ID to a sharded path.
    /// Example: ab/cd/ef0123456789... (first 2 bytes as subdirs)
    fn sharded_path(&self, prefix: &str, id: &[u8; 32]) -> PathBuf {
        let hex = hex::encode(id);
        self.base_path
            .join(prefix)
            .join(&hex[0..2])
            .join(&hex[2..4])
            .join(&hex[4..])
    }

    fn catalog_path(&self, id: Uuid) -> PathBuf {
        self.base_path
            .join("catalogs")
            .join(id.simple().to_string())
    }

    /// Atomic write: write to tempfile, then rename
    async fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
        })?;
        fs::create_dir_all(parent).await?;

        let temp = tempfile::NamedTempFile::new_in(parent)?;
        fs::write(temp.path(), data).await?;
        temp.persist(path)?;
        Ok(())
    }
}

#[async_trait]
impl Storage for FsStorage {
    async fn put_extent(
        &self,
        id: &[u8; 32],
        mut data: ByteReader,
        size_hint: Option<u64>,
    ) -> Result<bool, StorageError> {
        let path = self.sharded_path("extents", id);

        // Check if already exists
        if fs::try_exists(&path).await.unwrap_or(false) {
            return Ok(false);
        }

        // Create parent directories
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Write to tempfile while computing hash
        let temp = tempfile::NamedTempFile::new_in(
            path.parent().unwrap_or(Path::new(".")),
        )?;
        let temp_path = temp.path().to_path_buf();

        let mut file = File::create(&temp_path).await?;
        let mut hasher = blake3::Hasher::new();

        // Pre-allocate buffer based on size hint
        let buf_size = size_hint.map(|s| s.min(1024 * 1024) as usize).unwrap_or(128 * 1024);
        let mut buf = vec![0u8; buf_size];

        loop {
            let n = data.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            file.write_all(&buf[..n]).await?;
        }

        file.flush().await?;
        drop(file);

        // Verify hash
        let actual = hasher.finalize();
        if actual.as_bytes() != id {
            // Clean up temp file
            let _ = fs::remove_file(&temp_path).await;
            return Err(StorageError::HashMismatch {
                expected: hex::encode(id),
                actual: actual.to_hex().to_string(),
            });
        }

        // Atomically move to final location
        temp.persist(&path)?;
        Ok(true)
    }

    async fn get_extent(&self, id: &[u8; 32]) -> Result<ByteStream, StorageError> {
        let path = self.sharded_path("extents", id);

        let file = File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound
            } else {
                StorageError::Io(e)
            }
        })?;

        // Use a buffered reader with reasonable chunk size (64KB)
        let reader = BufReader::with_capacity(64 * 1024, file);
        let stream = ReaderStream::new(reader);

        // Map the stream to our error type
        let mapped = stream.map(|result| {
            result.map_err(StorageError::Io)
        });

        Ok(Box::new(mapped))
    }

    async fn extent_exists(&self, id: &[u8; 32]) -> Result<bool, StorageError> {
        let path = self.sharded_path("extents", id);
        Ok(path.exists())
    }

    async fn extents_exist(&self, ids: &[[u8; 32]]) -> Result<Vec<bool>, StorageError> {
        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            results.push(self.extent_exists(id).await?);
        }
        Ok(results)
    }

    // ... similar implementations for blobs and catalogs
}
```

Key implementation details:

1. **Directory sharding**: `extents/ab/cd/ef0123...` prevents too many files in one directory
2. **Atomic writes**: tempfile + rename prevents partial writes
3. **Hash verification**: Extent uploads are verified before storing
4. **Async I/O**: Uses `tokio::fs` throughout

---

## Phase 3: Blob Layout Format

### `blob.rs`

The blob layout describes how a file's content maps to extents. Sparse holes are NOT stored explicitly; instead, they are inferred by finding gaps between consecutive offset+length pairs.

**Binary Format:**

```
Header (18 bytes):
┌─────────┬─────────────────┬────────────────┬───────────────┐
│ version │ extent_id_size  │ total_bytes    │ extent_count  │
│ 1 byte  │ 1 byte (0x20)   │ u64 LE         │ u64 LE        │
└─────────┴─────────────────┴────────────────┴───────────────┘

Extent Map (repeated extent_count times, only non-sparse extents):
┌────────────┬────────────┬─────────────────────────────────┐
│ offset     │ length     │ extent_id                       │
│ u64 LE     │ u64 LE     │ H bytes                         │
└────────────┴────────────┴─────────────────────────────────┘
```

- **version**: Always `0x01`
- **extent_id_size**: Always `0x20` (32 bytes for BLAKE3)
- **total_bytes**: Total logical size of the blob (including holes)
- **extent_count**: Number of entries in the map (only non-sparse extents)
- **offset**: Byte offset within the blob where this extent starts
- **length**: Length of this extent in bytes
- **extent_id**: BLAKE3 hash of extent data

**Sparse Hole Detection:**

Holes are detected by looking for gaps:
- If first extent starts at offset > 0, there's a leading hole
- If extent[i].offset + extent[i].length < extent[i+1].offset, there's a gap (hole)
- If last extent ends before total_bytes, there's a trailing hole

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};

const BLOB_VERSION: u8 = 0x01;
const EXTENT_ID_SIZE: u8 = 0x20;

#[derive(Debug, Clone)]
pub struct BlobLayout {
    pub total_bytes: u64,
    pub extents: Vec<BlobExtent>,
}

#[derive(Debug, Clone)]
pub struct BlobExtent {
    pub offset: u64,
    pub length: u64,
    pub extent_id: [u8; 32],
}

/// Represents a region of the blob (either data or hole)
#[derive(Debug, Clone)]
pub enum BlobRegion {
    Data(BlobExtent),
    Hole { offset: u64, length: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum BlobDecodeError {
    #[error("Invalid version: {0}")]
    InvalidVersion(u8),
    #[error("Invalid extent ID size: {0}")]
    InvalidExtentIdSize(u8),
    #[error("Truncated data")]
    Truncated,
    #[error("Extents not sorted by offset")]
    NotSorted,
    #[error("Overlapping extents")]
    Overlapping,
}

impl BlobLayout {
    /// Header size in bytes
    const HEADER_SIZE: usize = 1 + 1 + 8 + 8; // 18 bytes

    /// Size of each extent entry
    const EXTENT_ENTRY_SIZE: usize = 8 + 8 + 32; // 48 bytes

    /// Encode to binary format (only non-sparse extents are written)
    pub fn encode(&self) -> Bytes {
        let size = Self::HEADER_SIZE + self.extents.len() * Self::EXTENT_ENTRY_SIZE;
        let mut buf = BytesMut::with_capacity(size);

        // Header
        buf.put_u8(BLOB_VERSION);
        buf.put_u8(EXTENT_ID_SIZE);
        buf.put_u64_le(self.total_bytes);
        buf.put_u64_le(self.extents.len() as u64);

        // Extent map (only actual extents, not holes)
        for extent in &self.extents {
            buf.put_u64_le(extent.offset);
            buf.put_u64_le(extent.length);
            buf.put_slice(&extent.extent_id);
        }

        buf.freeze()
    }

    /// Decode from binary format
    pub fn decode(mut data: &[u8]) -> Result<Self, BlobDecodeError> {
        if data.len() < Self::HEADER_SIZE {
            return Err(BlobDecodeError::Truncated);
        }

        let version = data.get_u8();
        if version != BLOB_VERSION {
            return Err(BlobDecodeError::InvalidVersion(version));
        }

        let id_size = data.get_u8();
        if id_size != EXTENT_ID_SIZE {
            return Err(BlobDecodeError::InvalidExtentIdSize(id_size));
        }

        let total_bytes = data.get_u64_le();
        let extent_count = data.get_u64_le() as usize;

        let expected_size = extent_count * Self::EXTENT_ENTRY_SIZE;
        if data.len() < expected_size {
            return Err(BlobDecodeError::Truncated);
        }

        let mut extents = Vec::with_capacity(extent_count);
        let mut prev_end: u64 = 0;

        for _ in 0..extent_count {
            let offset = data.get_u64_le();
            let length = data.get_u64_le();

            let mut extent_id = [0u8; 32];
            data.copy_to_slice(&mut extent_id);

            // Validate ordering
            if offset < prev_end {
                if offset + length > prev_end {
                    return Err(BlobDecodeError::Overlapping);
                }
                return Err(BlobDecodeError::NotSorted);
            }

            prev_end = offset + length;

            extents.push(BlobExtent {
                offset,
                length,
                extent_id,
            });
        }

        Ok(Self {
            total_bytes,
            extents,
        })
    }

    /// Iterate over all regions including holes
    pub fn regions(&self) -> Vec<BlobRegion> {
        let mut regions = Vec::new();
        let mut pos: u64 = 0;

        for extent in &self.extents {
            // Check for hole before this extent
            if extent.offset > pos {
                regions.push(BlobRegion::Hole {
                    offset: pos,
                    length: extent.offset - pos,
                });
            }

            regions.push(BlobRegion::Data(extent.clone()));
            pos = extent.offset + extent.length;
        }

        // Check for trailing hole
        if pos < self.total_bytes {
            regions.push(BlobRegion::Hole {
                offset: pos,
                length: self.total_bytes - pos,
            });
        }

        regions
    }
}
```

---

## Phase 4: API Layer

### `api/error.rs`

```rust
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
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
            StorageError::InvalidData(msg) => (
                StatusCode::BAD_REQUEST,
                "Invalid data",
                Some(msg.clone()),
            ),
            StorageError::Io(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error",
                None,
            ),
        };

        let body = ErrorResponse {
            error: error.to_string(),
            detail,
        };

        (status, Json(body)).into_response()
    }
}
```

### `api.rs`

```rust
use std::sync::Arc;
use axum::Router;

use crate::storage::Storage;

mod error;
mod extents;
mod blobs;
mod catalogs;

pub use error::ErrorResponse;

pub struct AppState<S: Storage> {
    pub storage: Arc<S>,
}

impl<S: Storage> Clone for AppState<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
        }
    }
}

pub fn router<S: Storage>(storage: S) -> Router {
    let state = AppState {
        storage: Arc::new(storage),
    };

    Router::new()
        .nest("/extents", extents::router())
        .nest("/blobs", blobs::router())
        .nest("/catalogs", catalogs::router())
        .with_state(state)
}
```

### `api/extents.rs`

```rust
use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, head, post, put},
    Json, Router,
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
    let stream = stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let reader = StreamReader::new(stream);

    let created = state.storage.put_extent(&id, Box::new(reader), size_hint).await?;

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
    if state.storage.extent_exists(&id).await? {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
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
    let bytes = hex::decode(s)
        .map_err(|_| StorageError::InvalidData("invalid hex".into()))?;
    bytes
        .try_into()
        .map_err(|_| StorageError::InvalidData("ID must be 32 bytes".into()))
}
```

### `api/blobs.rs`

Similar structure to `api/extents.rs`:
- `GET /blobs/:id` - Download blob layout
- `PUT /blobs/:id` - Upload blob layout
- `HEAD /blobs/:id` - Check existence

### `api/catalogs.rs`

```rust
/// GET /catalogs - List all catalogs
async fn list_catalogs<S: Storage>(
    State(state): State<AppState<S>>,
) -> Result<impl IntoResponse, StorageError> {
    let ids = state.storage.list_catalogs().await?;
    let ids: Vec<String> = ids.iter().map(|id| id.simple().to_string()).collect();
    Ok(Json(ids))
}

/// GET /catalogs/:id - Download catalog
/// PUT /catalogs/:id - Upload catalog
/// HEAD /catalogs/:id - Check existence
```

---

## Phase 5: CLI & Server Startup

### `config.rs`

```rust
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub storage_path: PathBuf,
}
```

### `main.rs`

```rust
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use lloggs::LoggingArgs;
use tracing::info;

use tumulus_server::{api, storage::FsStorage};

#[derive(Parser)]
#[command(name = "tumulus-server")]
#[command(about = "Tumulus backup storage server")]
struct Args {
    /// Address to listen on
    #[arg(long, short, default_value = "127.0.0.1:3000")]
    listen: SocketAddr,

    /// Storage directory path
    #[arg(long, short)]
    storage: PathBuf,

    #[command(flatten)]
    logging: LoggingArgs,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let _guard = args.logging.setup(|v| match v {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    })?;

    info!(listen = %args.listen, storage = ?args.storage, "Starting server");

    // Initialize storage
    let storage = FsStorage::new(&args.storage);
    storage.init().await?;

    // Build router
    let app = api::router(storage);

    // Start server
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    info!("Listening on {}", args.listen);
    axum::serve(listener, app).await?;

    Ok(())
}
```

---

## Dependencies

```toml
[package]
name = "tumulus-server"
version = "0.0.0"
edition = "2024"

[dependencies]
axum = "0.8"
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["io"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["trace"] }
bytes = "1"
futures = "0.3"
async-trait = "0.1"
hex = "0.4"
uuid = { version = "1", features = ["v4", "serde"] }
blake3 = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
clap = { version = "4", features = ["derive"] }
lloggs = "1.3"
tracing = "0.1"
tempfile = "3"
thiserror = "2"
```

---

## Implementation Order

| Phase | Files | Description |
|-------|-------|-------------|
| 1 | `storage/types.rs`, `storage.rs` | Error types and storage trait |
| 2 | `storage/fs.rs` | Filesystem backend |
| 3 | `blob.rs` | Blob layout encode/decode |
| 4 | `api/error.rs`, `api.rs` | Router setup and error handling |
| 5 | `api/extents.rs` | Extent endpoints |
| 6 | `api/blobs.rs` | Blob endpoints |
| 7 | `api/catalogs.rs` | Catalog endpoints |
| 8 | `config.rs`, `main.rs`, `lib.rs` | CLI and startup |
| 9 | Tests | Integration tests |

---

## Testing Strategy

### Unit Tests
- `blob.rs`: Encode/decode roundtrip, hole detection via gaps
- `storage/fs.rs`: Path sharding logic, streaming hash verification

### Integration Tests
- Start server with tempdir storage
- Test all endpoints with actual HTTP requests
- Test idempotent PUT behavior
- Test batch existence check
- Test error responses (404, 400)
- Test streaming large extents (>1MB) - verify memory usage stays bounded
- Test hash mismatch rejection on upload

### Manual Testing
```bash
# Start server
cargo run --bin tumulus-server -- -s /tmp/tumulus-storage -v

# Upload an extent
echo -n "hello" | curl -X PUT --data-binary @- http://localhost:3000/extents/$(echo -n "hello" | b3sum --no-names)

# Download it back
curl http://localhost:3000/extents/ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f

# Check existence
curl -I http://localhost:3000/extents/ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f

# Batch check
curl -X POST -H "Content-Type: application/json" \
  -d '{"ids":["ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f","0000000000000000000000000000000000000000000000000000000000000000"]}' \
  http://localhost:3000/extents/check
```

---

## Future Considerations

### S3 Backend
The trait design accommodates S3:
- All methods are async
- Uses `Bytes` for efficient handling
- `put_*` returns bool for conditional writes
- Batch operations reduce round trips

### Range Requests
For partial downloads, consider:
- HTTP Range header support for resumed downloads
- Byte-range responses for large extents

### Multipart Uploads
For very large extents (>100MB), consider:
- Chunked multipart upload API
- Resume support for failed uploads

### Authentication
When needed:
- JWT tokens (stateless)
- API keys with middleware
- mTLS for server-to-server

### Compression
- Server can accept/serve compressed extents
- Content-Encoding header for negotiation
- Store compression metadata (requires trait extension)