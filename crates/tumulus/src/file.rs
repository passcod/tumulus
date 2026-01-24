//! File metadata and processing functionality.

use std::{fs, io, path::Path};

use crate::B3Id;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use extentria::RangeReader;
use serde_json::json;

use crate::extents::{BlobInfo, process_file_extents, process_file_extents_with_reader};

/// Information about a file to be cataloged
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub relative_path: String,
    pub blob: Option<BlobInfo>,
    pub ts_created: Option<i64>,
    pub ts_modified: Option<i64>,
    pub ts_accessed: Option<i64>,
    pub ts_changed: Option<i64>,
    pub unix_mode: Option<u32>,
    pub unix_owner_id: Option<u32>,
    pub unix_group_id: Option<u32>,
    pub fs_inode: Option<u64>,
    pub special: Option<serde_json::Value>,
}

/// Extract Unix-specific metadata from file metadata.
#[cfg(unix)]
#[allow(clippy::type_complexity)]
fn extract_platform_metadata(
    metadata: &fs::Metadata,
) -> (
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u64>,
) {
    let ts_created = None; // Linux doesn't have creation time in standard stat
    let ts_modified = metadata.mtime().checked_mul(1000);
    let ts_accessed = metadata.atime().checked_mul(1000);
    let ts_changed = metadata.ctime().checked_mul(1000);
    let unix_mode = Some(metadata.permissions().mode());
    let unix_owner_id = Some(metadata.uid());
    let unix_group_id = Some(metadata.gid());
    let fs_inode = Some(metadata.ino());

    (
        ts_created,
        ts_modified,
        ts_accessed,
        ts_changed,
        unix_mode,
        unix_owner_id,
        unix_group_id,
        fs_inode,
    )
}

/// Extract Windows-specific metadata from file metadata.
#[cfg(windows)]
#[allow(clippy::type_complexity)]
fn extract_platform_metadata(
    metadata: &fs::Metadata,
) -> (
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u64>,
) {
    use std::time::UNIX_EPOCH;

    // Windows has creation time
    let ts_created = metadata
        .created()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64);

    let ts_modified = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64);

    let ts_accessed = metadata
        .accessed()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64);

    // Windows doesn't have ctime (inode change time)
    let ts_changed = None;

    // Unix-specific fields are not available on Windows
    let unix_mode = None;
    let unix_owner_id = None;
    let unix_group_id = None;

    // Windows doesn't have inodes; we could use volume serial + file index
    // but that requires opening the file handle. For now, return None.
    let fs_inode = None;

    (
        ts_created,
        ts_modified,
        ts_accessed,
        ts_changed,
        unix_mode,
        unix_owner_id,
        unix_group_id,
        fs_inode,
    )
}

/// Process a file and extract its metadata and blob information.
///
/// The `source_root` is used to compute the relative path for the file.
pub fn process_file(path: &Path, source_root: &Path) -> io::Result<FileInfo> {
    let metadata = fs::symlink_metadata(path)?;
    let relative_path = path
        .strip_prefix(source_root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");

    let (
        ts_created,
        ts_modified,
        ts_accessed,
        ts_changed,
        unix_mode,
        unix_owner_id,
        unix_group_id,
        fs_inode,
    ) = extract_platform_metadata(&metadata);

    // Handle special files
    let file_type = metadata.file_type();
    let special = if file_type.is_symlink() {
        let target = fs::read_link(path)?;
        Some(json!({
            "type": "symlink",
            "target": target.to_string_lossy()
        }))
    } else if file_type.is_dir() {
        Some(json!({ "type": "directory" }))
    } else if !file_type.is_file() {
        // Block device, char device, fifo, socket
        Some(json!({ "type": "other" }))
    } else {
        None
    };

    // Only process regular files for blob/extent data
    let blob = if metadata.is_file() && metadata.len() > 0 {
        process_file_extents(path)?
    } else if metadata.is_file() {
        // Zero-sized file still gets a blob
        Some(BlobInfo {
            blob_id: B3Id::hash(&[]),
            bytes: 0,
            extents: Vec::new(),
        })
    } else {
        None
    };

    Ok(FileInfo {
        relative_path,
        blob,
        ts_created,
        ts_modified,
        ts_accessed,
        ts_changed,
        unix_mode,
        unix_owner_id,
        unix_group_id,
        fs_inode,
        special,
    })
}

/// Process a file with a reusable RangeReader for better performance.
///
/// This is more efficient when processing multiple files as it reuses
/// the internal buffer for extent queries (on platforms that use buffers).
pub fn process_file_with_reader(
    path: &Path,
    source_root: &Path,
    reader: &mut RangeReader,
) -> io::Result<FileInfo> {
    let metadata = fs::symlink_metadata(path)?;
    let relative_path = path
        .strip_prefix(source_root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");

    let (
        ts_created,
        ts_modified,
        ts_accessed,
        ts_changed,
        unix_mode,
        unix_owner_id,
        unix_group_id,
        fs_inode,
    ) = extract_platform_metadata(&metadata);

    // Handle special files
    let file_type = metadata.file_type();
    let special = if file_type.is_symlink() {
        let target = fs::read_link(path)?;
        Some(json!({
            "type": "symlink",
            "target": target.to_string_lossy()
        }))
    } else if file_type.is_dir() {
        Some(json!({ "type": "directory" }))
    } else if !file_type.is_file() {
        // Block device, char device, fifo, socket
        Some(json!({ "type": "other" }))
    } else {
        None
    };

    // Only process regular files for blob/extent data
    let blob = if metadata.is_file() && metadata.len() > 0 {
        process_file_extents_with_reader(path, reader)?
    } else if metadata.is_file() {
        // Zero-sized file still gets a blob
        Some(BlobInfo {
            blob_id: B3Id::hash(&[]),
            bytes: 0,
            extents: Vec::new(),
        })
    } else {
        None
    };

    Ok(FileInfo {
        relative_path,
        blob,
        ts_created,
        ts_modified,
        ts_accessed,
        ts_changed,
        unix_mode,
        unix_owner_id,
        unix_group_id,
        fs_inode,
        special,
    })
}
