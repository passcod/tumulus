use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use tokio::io::AsyncRead;
use uuid::Uuid;

mod fs;
mod types;

pub use fs::FsStorage;
pub use types::{ObjectMeta, StorageError};

use crate::B3Id;

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
        id: &B3Id,
        data: ByteReader,
        size_hint: Option<u64>,
    ) -> Result<bool, StorageError>;

    /// Get extent data as a stream.
    /// Returns a stream of chunks for efficient memory usage with large extents.
    async fn get_extent(&self, id: &B3Id) -> Result<ByteStream, StorageError>;

    /// Get extent data as a complete buffer (convenience for small extents).
    /// Default implementation collects from the stream.
    async fn get_extent_bytes(&self, id: &B3Id) -> Result<Bytes, StorageError> {
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
    async fn extent_exists(&self, id: &B3Id) -> Result<bool, StorageError>;

    /// Batch check which extents exist (optimization for sync).
    /// Returns a Vec<bool> in the same order as input IDs.
    async fn extents_exist(&self, ids: &[B3Id]) -> Result<Vec<bool>, StorageError>;

    /// Get extent metadata without fetching data.
    async fn extent_meta(&self, id: &B3Id) -> Result<ObjectMeta, StorageError>;

    // --- Blobs ---

    /// Store blob layout data.
    /// Returns Ok(true) if newly stored, Ok(false) if already existed.
    async fn put_blob(&self, id: &B3Id, data: Bytes) -> Result<bool, StorageError>;

    /// Get blob layout by ID.
    async fn get_blob(&self, id: &B3Id) -> Result<Bytes, StorageError>;

    /// Check if blob exists.
    async fn blob_exists(&self, id: &B3Id) -> Result<bool, StorageError>;

    /// Get blob metadata without fetching data.
    async fn blob_meta(&self, id: &B3Id) -> Result<ObjectMeta, StorageError>;

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
