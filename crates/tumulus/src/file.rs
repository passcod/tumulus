//! File metadata and processing functionality.

use std::{
    fs, io,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
};

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

    let ts_modified = metadata.mtime().checked_mul(1000);
    let ts_accessed = metadata.atime().checked_mul(1000);
    let ts_changed = metadata.ctime().checked_mul(1000);

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
            blob_id: *blake3::hash(&[]).as_bytes(),
            bytes: 0,
            extents: Vec::new(),
        })
    } else {
        None
    };

    Ok(FileInfo {
        relative_path,
        blob,
        ts_created: None, // Linux doesn't have creation time in standard stat
        ts_modified,
        ts_accessed,
        ts_changed,
        unix_mode: Some(metadata.permissions().mode()),
        unix_owner_id: Some(metadata.uid()),
        unix_group_id: Some(metadata.gid()),
        fs_inode: Some(metadata.ino()),
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

    let ts_modified = metadata.mtime().checked_mul(1000);
    let ts_accessed = metadata.atime().checked_mul(1000);
    let ts_changed = metadata.ctime().checked_mul(1000);

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
            blob_id: *blake3::hash(&[]).as_bytes(),
            bytes: 0,
            extents: Vec::new(),
        })
    } else {
        None
    };

    Ok(FileInfo {
        relative_path,
        blob,
        ts_created: None, // Linux doesn't have creation time in standard stat
        ts_modified,
        ts_accessed,
        ts_changed,
        unix_mode: Some(metadata.permissions().mode()),
        unix_owner_id: Some(metadata.uid()),
        unix_group_id: Some(metadata.gid()),
        fs_inode: Some(metadata.ino()),
        special,
    })
}
