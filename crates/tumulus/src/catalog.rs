//! Catalog database schema and writing functionality.

use std::collections::HashMap;

use rusqlite::{Connection, params};

use crate::extents::ExtentInfo;
use crate::file::FileInfo;

/// Statistics about the catalog after writing.
#[derive(Debug, Clone)]
pub struct CatalogStats {
    pub file_count: i64,
    pub total_extents: i64,
    pub unique_extent_count: i64,
    pub total_bytes: i64,
    pub unique_bytes: i64,
    pub sparse_bytes: i64,
}

impl CatalogStats {
    /// Calculate the deduplication ratio.
    pub fn dedup_ratio(&self) -> f64 {
        if self.unique_bytes > 0 {
            self.total_bytes as f64 / self.unique_bytes as f64
        } else {
            1.0
        }
    }

    /// Calculate the space saved in bytes.
    pub fn space_saved(&self) -> i64 {
        (self.total_bytes - self.unique_bytes).max(0)
    }

    /// Calculate the percentage of space saved.
    pub fn space_saved_pct(&self) -> f64 {
        if self.total_bytes > 0 {
            (self.space_saved() as f64 / self.total_bytes as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Create the catalog database schema.
pub fn create_catalog_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS extents (
            extent_id BLOB PRIMARY KEY,
            bytes INTEGER NOT NULL CHECK(bytes > 0)
        );

        CREATE TABLE IF NOT EXISTS blobs (
            blob_id BLOB PRIMARY KEY,
            bytes INTEGER NOT NULL,
            extents INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS blob_extents (
            blob_id BLOB NOT NULL,
            extent_id BLOB,
            offset INTEGER NOT NULL,
            bytes INTEGER NOT NULL,
            PRIMARY KEY (blob_id, offset)
        );
        CREATE INDEX IF NOT EXISTS idx_blob_extents_blob ON blob_extents(blob_id);
        CREATE INDEX IF NOT EXISTS idx_blob_extents_extent ON blob_extents(extent_id);

        CREATE TABLE IF NOT EXISTS files (
            file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            path BLOB NOT NULL,
            blob_id BLOB,
            ts_created INTEGER,
            ts_changed INTEGER,
            ts_modified INTEGER,
            ts_accessed INTEGER,
            attributes TEXT,
            unix_mode INTEGER,
            unix_owner_id INTEGER,
            unix_owner_name TEXT,
            unix_group_id INTEGER,
            unix_group_name TEXT,
            special TEXT,
            fs_inode INTEGER,
            extra TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
        CREATE INDEX IF NOT EXISTS idx_files_blob ON files(blob_id);
        CREATE INDEX IF NOT EXISTS idx_files_ts_created ON files(ts_created);
        CREATE INDEX IF NOT EXISTS idx_files_ts_changed ON files(ts_changed);
        CREATE INDEX IF NOT EXISTS idx_files_ts_modified ON files(ts_modified);
        CREATE INDEX IF NOT EXISTS idx_files_ts_accessed ON files(ts_accessed);
        "#,
    )
}

/// Write file information to the catalog database.
///
/// This handles deduplication of blobs and extents, and returns statistics
/// about the written data.
pub fn write_catalog(conn: &Connection, file_infos: &[FileInfo]) -> rusqlite::Result<CatalogStats> {
    // Deduplicate blobs before inserting - only process each unique blob once
    // Also deduplicate extents within each blob by offset
    let mut seen_blobs: HashMap<[u8; 32], Vec<&ExtentInfo>> = HashMap::new();
    for file_info in file_infos {
        if let Some(ref blob) = file_info.blob {
            seen_blobs.entry(blob.blob_id).or_insert_with(|| {
                // Deduplicate extents by offset within this blob
                let mut extents_by_offset: HashMap<u64, &ExtentInfo> = HashMap::new();
                for extent in &blob.extents {
                    extents_by_offset.entry(extent.offset).or_insert(extent);
                }
                extents_by_offset.into_values().collect()
            });
        }
    }

    // Also collect blob metadata (bytes, extent count) separately
    let mut blob_metadata: HashMap<[u8; 32], (u64, usize)> = HashMap::new();
    for file_info in file_infos {
        if let Some(ref blob) = file_info.blob {
            blob_metadata.entry(blob.blob_id).or_insert_with(|| {
                let extent_count = seen_blobs.get(&blob.blob_id).map(|e| e.len()).unwrap_or(0);
                (blob.bytes, extent_count)
            });
        }
    }

    // Insert extents, blobs, blob_extents, and files
    let tx = conn.unchecked_transaction()?;

    {
        let mut extent_stmt =
            tx.prepare("INSERT OR IGNORE INTO extents (extent_id, bytes) VALUES (?1, ?2)")?;
        let mut blob_stmt =
            tx.prepare("INSERT INTO blobs (blob_id, bytes, extents) VALUES (?1, ?2, ?3)")?;
        let mut blob_extent_stmt = tx.prepare(
            "INSERT INTO blob_extents (blob_id, extent_id, offset, bytes) VALUES (?1, ?2, ?3, ?4)",
        )?;

        // Insert unique blobs and their extents
        for (blob_id, extents) in &seen_blobs {
            let (bytes, extent_count) = blob_metadata.get(blob_id).copied().unwrap_or((0, 0));

            // Insert extents (skip sparse holes - they have no extent_id)
            for extent in extents {
                if !extent.is_sparse {
                    extent_stmt
                        .execute(params![extent.extent_id.as_slice(), extent.bytes as i64])?;
                }
            }

            // Insert blob
            blob_stmt.execute(params![
                blob_id.as_slice(),
                bytes as i64,
                extent_count as i64
            ])?;

            // Insert blob_extents (include sparse holes with null extent_id)
            for extent in extents {
                let extent_id: Option<&[u8]> = if extent.is_sparse {
                    None
                } else {
                    Some(extent.extent_id.as_slice())
                };
                blob_extent_stmt.execute(params![
                    blob_id.as_slice(),
                    extent_id,
                    extent.offset as i64,
                    extent.bytes as i64
                ])?;
            }
        }

        // Insert files
        let mut file_stmt = tx.prepare(
            r#"INSERT INTO files (
                path, blob_id, ts_created, ts_changed, ts_modified, ts_accessed,
                unix_mode, unix_owner_id, unix_group_id, special, fs_inode
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"#,
        )?;

        for file_info in file_infos {
            file_stmt.execute(params![
                file_info.relative_path.as_bytes(),
                file_info.blob.as_ref().map(|b| b.blob_id.as_slice()),
                file_info.ts_created,
                file_info.ts_changed,
                file_info.ts_modified,
                file_info.ts_accessed,
                file_info.unix_mode,
                file_info.unix_owner_id,
                file_info.unix_group_id,
                file_info.special.as_ref().map(|v| v.to_string()),
                file_info.fs_inode.map(|i| i as i64),
            ])?;
        }
    }

    tx.commit()?;

    // Calculate statistics using SQL
    let file_count: i64 = conn.query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))?;

    let total_extents: i64 =
        conn.query_row("SELECT COUNT(*) FROM blob_extents", [], |row| row.get(0))?;

    let unique_extent_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM extents", [], |row| row.get(0))?;

    let total_bytes: i64 = conn.query_row(
        "SELECT COALESCE(SUM(bytes), 0) FROM blob_extents WHERE extent_id IS NOT NULL",
        [],
        |row| row.get(0),
    )?;

    let unique_bytes: i64 =
        conn.query_row("SELECT COALESCE(SUM(bytes), 0) FROM extents", [], |row| {
            row.get(0)
        })?;

    let sparse_bytes: i64 = conn.query_row(
        "SELECT COALESCE(SUM(bytes), 0) FROM blob_extents WHERE extent_id IS NULL",
        [],
        |row| row.get(0),
    )?;

    Ok(CatalogStats {
        file_count,
        total_extents,
        unique_extent_count,
        total_bytes,
        unique_bytes,
        sparse_bytes,
    })
}
