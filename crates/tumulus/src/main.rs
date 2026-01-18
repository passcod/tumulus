use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};

use blake3::Hasher;
use clap::Parser;
use extentria::fiemap::FiemapLookup;
use jiff::Timestamp;
use lloggs::LoggingArgs;
use memmap2::Mmap;
use rayon::prelude::*;
use rusqlite::{Connection, params};
use serde_json::json;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(name = "tumulus")]
#[command(about = "Build a snapshot catalog from a directory tree")]
struct Args {
    /// Source directory to catalog
    source_path: PathBuf,

    /// Output catalog file path
    catalog_output: PathBuf,

    /// Make extent read errors fatal (exit on first error)
    #[arg(long, short = 'e')]
    fatal_errors: bool,

    #[command(flatten)]
    logging: LoggingArgs,
}

/// Information about a file extent
struct ExtentInfo {
    extent_id: [u8; 32],
    offset: u64,
    bytes: u64,
    is_sparse: bool,
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

fn get_machine_id() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    machine_uid::get().map_err(|e| format!("Failed to get machine ID: {}", e).into())
}

fn detect_sparse_holes(extents: &[(u64, u64, [u8; 32])], file_size: u64) -> Vec<ExtentInfo> {
    let mut result = Vec::new();
    let mut current_pos: u64 = 0;

    for (logical_offset, length, extent_id) in extents {
        // If there's a gap before this extent, it's a sparse hole
        if *logical_offset > current_pos {
            let hole_size = logical_offset - current_pos;
            debug!(
                offset = current_pos,
                size = hole_size,
                "Detected sparse hole"
            );
            result.push(ExtentInfo {
                extent_id: [0u8; 32], // Sparse extents have no ID
                offset: current_pos,
                bytes: hole_size,
                is_sparse: true,
            });
        }

        result.push(ExtentInfo {
            extent_id: *extent_id,
            offset: *logical_offset,
            bytes: *length,
            is_sparse: false,
        });

        current_pos = logical_offset + length;
    }

    // Check for trailing sparse hole
    if current_pos < file_size {
        let hole_size = file_size - current_pos;
        debug!(
            offset = current_pos,
            size = hole_size,
            "Detected trailing sparse hole"
        );
        result.push(ExtentInfo {
            extent_id: [0u8; 32],
            offset: current_pos,
            bytes: hole_size,
            is_sparse: true,
        });
    }

    result
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
    debug!(?path, "Processing file extents");

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
    let extent_results: Result<Vec<_>, _> = FiemapLookup::extents_for_file(&file)?
        .map(|r| {
            r.map(|extent| {
                let start = (extent.logical_offset as usize).min(mmap.len());
                let end = (start + extent.length as usize).min(mmap.len());
                let slice = &mmap[start..end];
                let extent_id = *blake3::hash(slice).as_bytes();
                (extent.logical_offset, (end - start) as u64, extent_id)
            })
        })
        .collect();

    let raw_extents = extent_results?;

    if raw_extents.is_empty() {
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
                is_sparse: false,
            }],
        }));
    }

    // Detect sparse holes
    let extents = detect_sparse_holes(&raw_extents, file_len);

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

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let _guard = args.logging.setup(|v| match v {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    })?;

    let source_path = args.source_path.canonicalize()?;
    let catalog_path = &args.catalog_output;

    let started = Timestamp::now();
    let catalog_id = Uuid::new_v4();
    let machine_id = get_machine_id()?;

    info!(?catalog_id, ?source_path, "Building catalog");

    // Collect all file paths first
    let paths: Vec<PathBuf> = WalkDir::new(&source_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.into_path())
        .collect();

    info!(entries = paths.len(), "Found entries");

    // Process files in parallel
    let results: Vec<_> = paths
        .par_iter()
        .map(|path| (path.clone(), process_file(path, &source_path)))
        .collect();

    // Collect successful results and handle errors
    let mut file_infos: Vec<FileInfo> = Vec::new();
    let mut error_count = 0;

    for (path, result) in results {
        match result {
            Ok(info) => file_infos.push(info),
            Err(err) => {
                error_count += 1;
                if args.fatal_errors {
                    error!(?path, %err, "Fatal error processing file");
                    return Err(err.into());
                } else {
                    warn!(?path, %err, "Skipping file due to error");
                }
            }
        }
    }

    if error_count > 0 {
        warn!(error_count, "Some files were skipped due to errors");
    }

    info!(files = file_infos.len(), "Processed files");

    // Compute tree hash
    let tree_hash = compute_tree_hash(&file_infos);

    // Create the catalog database
    let conn = Connection::open(catalog_path)?;
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
        params!["machine", json!(machine_id).to_string()],
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

    // Deduplicate blobs before inserting - only process each unique blob once
    // Also deduplicate extents within each blob by offset
    let mut seen_blobs: HashMap<[u8; 32], Vec<&ExtentInfo>> = HashMap::new();
    for file_info in &file_infos {
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
    for file_info in &file_infos {
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

        for file_info in &file_infos {
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

    // Calculate deduplication stats using SQL
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

    let dedup_ratio = if unique_bytes > 0 {
        total_bytes as f64 / unique_bytes as f64
    } else {
        1.0
    };

    let space_saved = (total_bytes - unique_bytes).max(0);
    let space_saved_pct = if total_bytes > 0 {
        (space_saved as f64 / total_bytes as f64) * 100.0
    } else {
        0.0
    };

    info!(?catalog_path, "Catalog written");
    eprintln!("Catalog written to {:?}", catalog_path);
    eprintln!("  ID: {}", catalog_id);
    eprintln!("  Tree hash: {}", hex::encode(tree_hash));
    eprintln!("  Files: {}", file_count);
    eprintln!(
        "  Extents: {} ({} unique)",
        total_extents, unique_extent_count
    );
    eprintln!(
        "  Total size: {} bytes ({} unique)",
        total_bytes, unique_bytes
    );
    if sparse_bytes > 0 {
        eprintln!("  Sparse holes: {} bytes", sparse_bytes);
    }
    eprintln!(
        "  Dedup ratio: {:.2}x ({:.1}% space saved, {} bytes)",
        dedup_ratio, space_saved_pct, space_saved
    );

    Ok(())
}
