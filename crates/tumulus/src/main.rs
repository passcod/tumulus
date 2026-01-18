use std::{
    collections::BTreeMap,
    fs::{self, File},
    io,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};

use blake3::Hasher;
use extentria::fiemap::FiemapLookup;
use jiff::Timestamp;
use memmap2::Mmap;
use rayon::prelude::*;
use rusqlite::{Connection, params};
use serde_json::json;
use uuid::Uuid;
use walkdir::WalkDir;

/// Information about a file extent
struct ExtentInfo {
    extent_id: [u8; 32],
    offset: u64,
    bytes: u64,
}

/// Information about a file's blob
struct BlobInfo {
    blob_id: [u8; 32],
    bytes: u64,
    extents: Vec<ExtentInfo>,
}

/// Information about a file to be cataloged
struct FileInfo {
    relative_path: String,
    blob: Option<BlobInfo>,
    ts_created: Option<i64>,
    ts_modified: Option<i64>,
    ts_accessed: Option<i64>,
    ts_changed: Option<i64>,
    unix_mode: Option<u32>,
    unix_owner_id: Option<u32>,
    unix_group_id: Option<u32>,
    fs_inode: Option<u64>,
    special: Option<serde_json::Value>,
}

fn get_machine_id() -> [u8; 32] {
    // Try to read machine-id, fall back to a hash of hostname
    if let Ok(id) = fs::read_to_string("/etc/machine-id") {
        let mut hasher = Hasher::new();
        hasher.update(id.trim().as_bytes());
        *hasher.finalize().as_bytes()
    } else if let Ok(hostname) = fs::read_to_string("/etc/hostname") {
        let mut hasher = Hasher::new();
        hasher.update(hostname.trim().as_bytes());
        *hasher.finalize().as_bytes()
    } else {
        [0u8; 32]
    }
}

fn process_file(path: &Path, source_root: &Path) -> io::Result<FileInfo> {
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

fn process_file_extents(path: &Path) -> io::Result<Option<BlobInfo>> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();

    if file_len == 0 {
        return Ok(Some(BlobInfo {
            blob_id: *blake3::hash(&[]).as_bytes(),
            bytes: 0,
            extents: Vec::new(),
        }));
    }

    let mmap = unsafe { Mmap::map(&file)? };

    // Get extent information from FIEMAP
    let extent_infos: Vec<_> = FiemapLookup::extents_for_file(&file)?
        .filter_map(|r| r.ok())
        .collect();

    if extent_infos.is_empty() {
        // No extents reported, treat whole file as one extent
        let extent_id = *blake3::hash(&mmap[..]).as_bytes();
        let mut blob_hasher = Hasher::new();
        blob_hasher.update(&mmap[..]);
        let blob_id = *blob_hasher.finalize().as_bytes();

        return Ok(Some(BlobInfo {
            blob_id,
            bytes: file_len,
            extents: vec![ExtentInfo {
                extent_id,
                offset: 0,
                bytes: file_len,
            }],
        }));
    }

    // Process extents in parallel
    let extents: Vec<ExtentInfo> = extent_infos
        .par_iter()
        .map(|extent| {
            let start = (extent.logical_offset as usize).min(mmap.len());
            let end = (start + extent.length as usize).min(mmap.len());
            let slice = &mmap[start..end];
            let extent_id = *blake3::hash(slice).as_bytes();

            ExtentInfo {
                extent_id,
                offset: extent.logical_offset,
                bytes: (end - start) as u64,
            }
        })
        .collect();

    // Compute blob hash (hash of full file contents)
    let mut blob_hasher = Hasher::new();
    blob_hasher.update_rayon(&mmap[..]);
    let blob_id = *blob_hasher.finalize().as_bytes();

    Ok(Some(BlobInfo {
        blob_id,
        bytes: file_len,
        extents,
    }))
}

fn create_catalog_schema(conn: &Connection) -> rusqlite::Result<()> {
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

fn compute_tree_hash(files: &[FileInfo]) -> [u8; 32] {
    // Build sorted tree map: path -> blob_id
    let mut tree_entries: BTreeMap<&str, &[u8; 32]> = BTreeMap::new();

    for file in files {
        if let Some(ref blob) = file.blob {
            tree_entries.insert(&file.relative_path, &blob.blob_id);
        }
    }

    // Hash the tree
    let mut hasher = Hasher::new();
    for (path, blob_id) in tree_entries {
        let path_bytes = path.as_bytes();
        let path_len = (path_bytes.len() as u32).to_le_bytes();
        hasher.update(&path_len);
        hasher.update(path_bytes);
        hasher.update(blob_id);
    }

    *hasher.finalize().as_bytes()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("USAGE: tumulus <source_path> <catalog_output>");
        std::process::exit(1);
    }

    let source_path = PathBuf::from(&args[1]);
    let catalog_path = PathBuf::from(&args[2]);

    let started = Timestamp::now();
    let catalog_id = Uuid::new_v4();
    let machine_id = get_machine_id();

    eprintln!("Building catalog {} from {:?}", catalog_id, source_path);

    // Collect all file paths first
    let paths: Vec<PathBuf> = WalkDir::new(&source_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.into_path())
        .collect();

    eprintln!("Found {} entries", paths.len());

    // Process files in parallel
    let file_infos: Vec<FileInfo> = paths
        .par_iter()
        .filter_map(|path| match process_file(path, &source_path) {
            Ok(info) => Some(info),
            Err(err) => {
                eprintln!("Error processing {:?}: {}", path, err);
                None
            }
        })
        .collect();

    eprintln!("Processed {} files", file_infos.len());

    // Compute tree hash
    let tree_hash = compute_tree_hash(&file_infos);

    // Create the catalog database
    let conn = Connection::open(&catalog_path)?;
    create_catalog_schema(&conn)?;

    let created = Timestamp::now();

    // Insert metadata
    conn.execute(
        "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
        params!["protocol", json!(1).to_string()],
    )?;
    conn.execute(
        "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
        params!["id", json!(catalog_id.simple().to_string()).to_string()],
    )?;
    conn.execute(
        "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
        params!["machine", json!(hex::encode(machine_id)).to_string()],
    )?;
    conn.execute(
        "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
        params!["tree", json!(hex::encode(tree_hash)).to_string()],
    )?;
    conn.execute(
        "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
        params!["created", json!(created.as_millisecond()).to_string()],
    )?;
    conn.execute(
        "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
        params!["started", json!(started.as_millisecond()).to_string()],
    )?;
    conn.execute(
        "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
        params![
            "source_path",
            json!(source_path.to_string_lossy()).to_string()
        ],
    )?;

    // Insert extents, blobs, blob_extents, and files
    let tx = conn.unchecked_transaction()?;

    {
        let mut extent_stmt =
            tx.prepare("INSERT OR IGNORE INTO extents (extent_id, bytes) VALUES (?1, ?2)")?;
        let mut blob_stmt = tx
            .prepare("INSERT OR IGNORE INTO blobs (blob_id, bytes, extents) VALUES (?1, ?2, ?3)")?;
        let mut blob_extent_stmt = tx.prepare(
            "INSERT OR IGNORE INTO blob_extents (blob_id, extent_id, offset, bytes) VALUES (?1, ?2, ?3, ?4)",
        )?;
        let mut file_stmt = tx.prepare(
            r#"INSERT INTO files (
                path, blob_id, ts_created, ts_changed, ts_modified, ts_accessed,
                unix_mode, unix_owner_id, unix_group_id, special, fs_inode
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"#,
        )?;

        for file_info in &file_infos {
            if let Some(ref blob) = file_info.blob {
                // Insert extents
                for extent in &blob.extents {
                    extent_stmt
                        .execute(params![extent.extent_id.as_slice(), extent.bytes as i64])?;
                }

                // Insert blob
                blob_stmt.execute(params![
                    blob.blob_id.as_slice(),
                    blob.bytes as i64,
                    blob.extents.len() as i64
                ])?;

                // Insert blob_extents
                for extent in &blob.extents {
                    blob_extent_stmt.execute(params![
                        blob.blob_id.as_slice(),
                        extent.extent_id.as_slice(),
                        extent.offset as i64,
                        extent.bytes as i64
                    ])?;
                }
            }

            // Insert file
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

    eprintln!("Catalog written to {:?}", catalog_path);
    eprintln!("  ID: {}", catalog_id);
    eprintln!("  Tree hash: {}", hex::encode(tree_hash));
    eprintln!("  Files: {}", file_infos.len());
    eprintln!(
        "  Extents: {}",
        file_infos
            .iter()
            .filter_map(|f| f.blob.as_ref())
            .map(|b| b.extents.len())
            .sum::<usize>()
    );

    Ok(())
}
