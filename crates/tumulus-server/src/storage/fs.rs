use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::B3Id;

use super::{ByteReader, ByteStream, ObjectMeta, Storage, StorageError};

pub struct FsStorage {
    base_path: PathBuf,
}

impl FsStorage {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
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
    fn sharded_path(&self, prefix: &str, id: &B3Id) -> PathBuf {
        let hex = id.as_hex();
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
        temp.persist(path).map_err(|e| e.error)?;
        Ok(())
    }
}

#[async_trait]
impl Storage for FsStorage {
    async fn put_extent(
        &self,
        id: &B3Id,
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
        let temp = tempfile::NamedTempFile::new_in(path.parent().unwrap_or(Path::new(".")))?;
        let temp_path = temp.path().to_path_buf();

        let mut file = File::create(&temp_path).await?;
        let mut hasher = blake3::Hasher::new();

        // Pre-allocate buffer based on size hint
        let buf_size = size_hint
            .map(|s| s.min(1024 * 1024) as usize)
            .unwrap_or(128 * 1024);
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
        if actual != id.0 {
            // Clean up temp file
            let _ = fs::remove_file(&temp_path).await;
            return Err(StorageError::HashMismatch {
                expected: id.as_hex(),
                actual: actual.to_hex().to_string(),
            });
        }

        // Atomically move to final location
        temp.persist(&path).map_err(|e| StorageError::Io(e.error))?;
        Ok(true)
    }

    async fn get_extent(&self, id: &B3Id) -> Result<ByteStream, StorageError> {
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
        let mapped = stream.map(|result| result.map_err(StorageError::Io));

        Ok(Box::new(mapped))
    }

    async fn extent_exists(&self, id: &B3Id) -> Result<bool, StorageError> {
        let path = self.sharded_path("extents", id);
        Ok(fs::try_exists(&path).await.unwrap_or(false))
    }

    async fn extents_exist(&self, ids: &[B3Id]) -> Result<Vec<bool>, StorageError> {
        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            results.push(self.extent_exists(id).await?);
        }
        Ok(results)
    }

    async fn extent_meta(&self, id: &B3Id) -> Result<ObjectMeta, StorageError> {
        let path = self.sharded_path("extents", id);
        let metadata = fs::metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound
            } else {
                StorageError::Io(e)
            }
        })?;

        Ok(ObjectMeta {
            size: metadata.len(),
            created: metadata.created().ok(),
        })
    }

    async fn put_blob(&self, id: &B3Id, data: Bytes) -> Result<bool, StorageError> {
        let path = self.sharded_path("blobs", id);

        // Check if already exists
        if fs::try_exists(&path).await.unwrap_or(false) {
            return Ok(false);
        }

        self.atomic_write(&path, &data).await?;
        Ok(true)
    }

    async fn get_blob(&self, id: &B3Id) -> Result<Bytes, StorageError> {
        let path = self.sharded_path("blobs", id);
        let data = fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound
            } else {
                StorageError::Io(e)
            }
        })?;
        Ok(Bytes::from(data))
    }

    async fn blob_exists(&self, id: &B3Id) -> Result<bool, StorageError> {
        let path = self.sharded_path("blobs", id);
        Ok(fs::try_exists(&path).await.unwrap_or(false))
    }

    async fn blob_meta(&self, id: &B3Id) -> Result<ObjectMeta, StorageError> {
        let path = self.sharded_path("blobs", id);
        let metadata = fs::metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound
            } else {
                StorageError::Io(e)
            }
        })?;

        Ok(ObjectMeta {
            size: metadata.len(),
            created: metadata.created().ok(),
        })
    }

    async fn put_catalog(&self, id: Uuid, data: Bytes) -> Result<(), StorageError> {
        let path = self.catalog_path(id);
        self.atomic_write(&path, &data).await?;
        Ok(())
    }

    async fn get_catalog(&self, id: Uuid) -> Result<Bytes, StorageError> {
        let path = self.catalog_path(id);
        let data = fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound
            } else {
                StorageError::Io(e)
            }
        })?;
        Ok(Bytes::from(data))
    }

    async fn catalog_exists(&self, id: Uuid) -> Result<bool, StorageError> {
        let path = self.catalog_path(id);
        Ok(fs::try_exists(&path).await.unwrap_or(false))
    }

    async fn catalog_meta(&self, id: Uuid) -> Result<ObjectMeta, StorageError> {
        let path = self.catalog_path(id);
        let metadata = fs::metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound
            } else {
                StorageError::Io(e)
            }
        })?;

        Ok(ObjectMeta {
            size: metadata.len(),
            created: metadata.created().ok(),
        })
    }

    async fn list_catalogs(&self) -> Result<Vec<Uuid>, StorageError> {
        let catalogs_dir = self.base_path.join("catalogs");

        // If directory doesn't exist, return empty list
        if !fs::try_exists(&catalogs_dir).await.unwrap_or(false) {
            return Ok(Vec::new());
        }

        let mut entries = fs::read_dir(&catalogs_dir).await?;
        let mut ids = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            if let Some(name) = entry.file_name().to_str()
                && let Ok(uuid) = Uuid::parse_str(name)
            {
                ids.push(uuid);
            }
        }

        Ok(ids)
    }
}
