//! Database module for tracking catalog upload state.
//!
//! Uses SQLite to track catalog upload sessions, their status,
//! and which extents are needed for each upload.

use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;
use uuid::Uuid;

/// Database error type.
#[derive(Debug, Error)]
pub enum DbError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("Catalog not found: {0}")]
    CatalogNotFound(Uuid),
}

/// Status of a catalog upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogStatus {
    /// Catalog upload initiated but not yet received
    Pending,
    /// Catalog received, extents being uploaded
    Uploading,
    /// All extents uploaded, catalog is complete
    Complete,
}

impl CatalogStatus {
    fn as_str(&self) -> &'static str {
        match self {
            CatalogStatus::Pending => "pending",
            CatalogStatus::Uploading => "uploading",
            CatalogStatus::Complete => "complete",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(CatalogStatus::Pending),
            "uploading" => Some(CatalogStatus::Uploading),
            "complete" => Some(CatalogStatus::Complete),
            _ => None,
        }
    }
}

/// Information about a catalog upload session.
#[derive(Debug, Clone)]
pub struct CatalogInfo {
    pub id: Uuid,
    pub checksum: [u8; 32],
    pub status: CatalogStatus,
    pub created_at: i64,
}

/// Database handle for tracking catalog uploads.
pub struct UploadDb {
    conn: Connection,
}

impl UploadDb {
    /// Open or create the upload tracking database.
    pub fn open(path: &Path) -> Result<Self, DbError> {
        let conn = Connection::open(path)?;
        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    /// Open an in-memory database (for testing).
    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self, DbError> {
        let conn = Connection::open_in_memory()?;
        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    /// Initialize the database schema.
    fn init_schema(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS catalogs (
                id BLOB PRIMARY KEY,
                checksum BLOB NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_catalogs_checksum ON catalogs(checksum);
            CREATE INDEX IF NOT EXISTS idx_catalogs_status ON catalogs(status);

            -- Track which extents are needed for each catalog
            CREATE TABLE IF NOT EXISTS catalog_extents (
                catalog_id BLOB NOT NULL,
                extent_id BLOB NOT NULL,
                PRIMARY KEY (catalog_id, extent_id),
                FOREIGN KEY (catalog_id) REFERENCES catalogs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_catalog_extents_extent ON catalog_extents(extent_id);
            "#,
        )?;
        Ok(())
    }

    /// Look up a catalog by ID.
    pub fn get_catalog(&self, id: Uuid) -> Result<Option<CatalogInfo>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, checksum, status, created_at FROM catalogs WHERE id = ?1",
                params![id.as_bytes().as_slice()],
                |row| {
                    let id_bytes: Vec<u8> = row.get(0)?;
                    let checksum_bytes: Vec<u8> = row.get(1)?;
                    let status_str: String = row.get(2)?;
                    let created_at: i64 = row.get(3)?;

                    Ok((id_bytes, checksum_bytes, status_str, created_at))
                },
            )
            .optional()?;

        match result {
            Some((id_bytes, checksum_bytes, status_str, created_at)) => {
                let id = Uuid::from_slice(&id_bytes).map_err(|_| {
                    rusqlite::Error::InvalidColumnType(0, "id".into(), rusqlite::types::Type::Blob)
                })?;
                let checksum: [u8; 32] = checksum_bytes.try_into().map_err(|_| {
                    rusqlite::Error::InvalidColumnType(
                        1,
                        "checksum".into(),
                        rusqlite::types::Type::Blob,
                    )
                })?;
                let status = CatalogStatus::from_str(&status_str).ok_or_else(|| {
                    rusqlite::Error::InvalidColumnType(
                        2,
                        "status".into(),
                        rusqlite::types::Type::Text,
                    )
                })?;

                Ok(Some(CatalogInfo {
                    id,
                    checksum,
                    status,
                    created_at,
                }))
            }
            None => Ok(None),
        }
    }

    /// Look up a catalog by checksum.
    pub fn find_catalog_by_checksum(
        &self,
        checksum: &[u8; 32],
    ) -> Result<Option<CatalogInfo>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, checksum, status, created_at FROM catalogs WHERE checksum = ?1 LIMIT 1",
                params![checksum.as_slice()],
                |row| {
                    let id_bytes: Vec<u8> = row.get(0)?;
                    let checksum_bytes: Vec<u8> = row.get(1)?;
                    let status_str: String = row.get(2)?;
                    let created_at: i64 = row.get(3)?;

                    Ok((id_bytes, checksum_bytes, status_str, created_at))
                },
            )
            .optional()?;

        match result {
            Some((id_bytes, checksum_bytes, status_str, created_at)) => {
                let id = Uuid::from_slice(&id_bytes).map_err(|_| {
                    rusqlite::Error::InvalidColumnType(0, "id".into(), rusqlite::types::Type::Blob)
                })?;
                let checksum: [u8; 32] = checksum_bytes.try_into().map_err(|_| {
                    rusqlite::Error::InvalidColumnType(
                        1,
                        "checksum".into(),
                        rusqlite::types::Type::Blob,
                    )
                })?;
                let status = CatalogStatus::from_str(&status_str).ok_or_else(|| {
                    rusqlite::Error::InvalidColumnType(
                        2,
                        "status".into(),
                        rusqlite::types::Type::Text,
                    )
                })?;

                Ok(Some(CatalogInfo {
                    id,
                    checksum,
                    status,
                    created_at,
                }))
            }
            None => Ok(None),
        }
    }

    /// Create a new catalog entry.
    pub fn create_catalog(&self, id: Uuid, checksum: &[u8; 32]) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO catalogs (id, checksum, status) VALUES (?1, ?2, ?3)",
            params![
                id.as_bytes().as_slice(),
                checksum.as_slice(),
                CatalogStatus::Pending.as_str()
            ],
        )?;
        Ok(())
    }

    /// Generate a new unique catalog ID.
    pub fn generate_catalog_id(&self) -> Uuid {
        Uuid::new_v4()
    }

    /// Update the status of a catalog.
    pub fn update_status(&self, id: Uuid, status: CatalogStatus) -> Result<(), DbError> {
        let rows = self.conn.execute(
            "UPDATE catalogs SET status = ?1 WHERE id = ?2",
            params![status.as_str(), id.as_bytes().as_slice()],
        )?;
        if rows == 0 {
            return Err(DbError::CatalogNotFound(id));
        }
        Ok(())
    }

    /// Store the list of extent IDs needed for a catalog.
    pub fn set_catalog_extents(
        &self,
        catalog_id: Uuid,
        extent_ids: &[[u8; 32]],
    ) -> Result<(), DbError> {
        // First, clear any existing extents for this catalog
        self.conn.execute(
            "DELETE FROM catalog_extents WHERE catalog_id = ?1",
            params![catalog_id.as_bytes().as_slice()],
        )?;

        // Insert new extents
        let mut stmt = self
            .conn
            .prepare("INSERT INTO catalog_extents (catalog_id, extent_id) VALUES (?1, ?2)")?;

        for extent_id in extent_ids {
            stmt.execute(params![
                catalog_id.as_bytes().as_slice(),
                extent_id.as_slice()
            ])?;
        }

        Ok(())
    }

    /// Get the list of extent IDs needed for a catalog.
    pub fn get_catalog_extents(&self, catalog_id: Uuid) -> Result<Vec<[u8; 32]>, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT extent_id FROM catalog_extents WHERE catalog_id = ?1")?;

        let rows = stmt.query_map(params![catalog_id.as_bytes().as_slice()], |row| {
            let extent_id: Vec<u8> = row.get(0)?;
            Ok(extent_id)
        })?;

        let mut extents = Vec::new();
        for row in rows {
            let extent_id: Vec<u8> = row?;
            let extent_id: [u8; 32] = extent_id.try_into().map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    0,
                    "extent_id".into(),
                    rusqlite::types::Type::Blob,
                )
            })?;
            extents.push(extent_id);
        }

        Ok(extents)
    }

    /// Delete a catalog and its associated extents.
    pub fn delete_catalog(&self, id: Uuid) -> Result<(), DbError> {
        self.conn.execute(
            "DELETE FROM catalogs WHERE id = ?1",
            params![id.as_bytes().as_slice()],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get_catalog() {
        let db = UploadDb::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        let checksum = [0x42u8; 32];

        db.create_catalog(id, &checksum).unwrap();

        let info = db.get_catalog(id).unwrap().unwrap();
        assert_eq!(info.id, id);
        assert_eq!(info.checksum, checksum);
        assert_eq!(info.status, CatalogStatus::Pending);
    }

    #[test]
    fn test_find_by_checksum() {
        let db = UploadDb::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        let checksum = [0x42u8; 32];

        db.create_catalog(id, &checksum).unwrap();

        let info = db.find_catalog_by_checksum(&checksum).unwrap().unwrap();
        assert_eq!(info.id, id);
    }

    #[test]
    fn test_update_status() {
        let db = UploadDb::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        let checksum = [0x42u8; 32];

        db.create_catalog(id, &checksum).unwrap();
        db.update_status(id, CatalogStatus::Uploading).unwrap();

        let info = db.get_catalog(id).unwrap().unwrap();
        assert_eq!(info.status, CatalogStatus::Uploading);
    }

    #[test]
    fn test_catalog_extents() {
        let db = UploadDb::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        let checksum = [0x42u8; 32];

        db.create_catalog(id, &checksum).unwrap();

        let extents = vec![[0x01u8; 32], [0x02u8; 32], [0x03u8; 32]];
        db.set_catalog_extents(id, &extents).unwrap();

        let retrieved = db.get_catalog_extents(id).unwrap();
        assert_eq!(retrieved.len(), 3);
        assert!(retrieved.contains(&[0x01u8; 32]));
        assert!(retrieved.contains(&[0x02u8; 32]));
        assert!(retrieved.contains(&[0x03u8; 32]));
    }

    #[test]
    fn test_delete_catalog() {
        let db = UploadDb::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        let checksum = [0x42u8; 32];

        db.create_catalog(id, &checksum).unwrap();
        db.delete_catalog(id).unwrap();

        let info = db.get_catalog(id).unwrap();
        assert!(info.is_none());
    }
}
