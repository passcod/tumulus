//! Build a snapshot catalog from a directory tree

use std::collections::HashMap;
use std::path::PathBuf;

use clap::Args;
use jiff::Timestamp;
use rayon::prelude::*;
use rusqlite::{Connection, params};
use serde_json::json;
use tracing::{error, info, warn};
use uuid::Uuid;
use walkdir::WalkDir;

use fs_info::{get_fs_info, is_readonly};
use tumulus::{
    DEFAULT_COMPRESSION_LEVEL, FileInfo, RangeReader, RangeReaderImpl,
    compression::compress_file_with_level, compute_tree_hash, create_catalog_schema, get_hostname,
    get_machine_id, process_file_with_reader, write_catalog,
};

/// Build a snapshot catalog from a directory tree
#[derive(Args, Debug)]
pub struct CatalogArgs {
    /// Source directory to catalog
    source_path: PathBuf,

    /// Output catalog file path
    catalog_output: PathBuf,

    /// Make extent read errors fatal (exit on first error)
    #[arg(long, short = 'e')]
    fatal_errors: bool,

    /// Zstd compression level (0 to disable, 1-22 for compression)
    #[arg(long, short = 'c', default_value_t = DEFAULT_COMPRESSION_LEVEL)]
    compression: i32,

    /// Friendly name for this catalog
    #[arg(long, short = 'n')]
    name: Option<String>,

    /// Extra metadata in KEY=VALUE format (can be specified multiple times)
    #[arg(long, short = 'm', value_parser = parse_key_value)]
    meta: Vec<(String, String)>,
}

/// Parse a KEY=VALUE string into a tuple.
fn parse_key_value(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=VALUE: no '=' found in '{}'", s))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}

pub fn run(args: CatalogArgs) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    // Process files in parallel, with per-thread RangeReader for buffer reuse
    let results: Vec<_> = paths
        .par_iter()
        .map_init(RangeReader::new, |reader, path| {
            (
                path.clone(),
                process_file_with_reader(path, &source_path, reader),
            )
        })
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

    // Collect all metadata
    let mut metadata: HashMap<&str, serde_json::Value> = HashMap::new();

    // Mandatory metadata
    metadata.insert("protocol", json!(1));
    metadata.insert("id", json!(catalog_id.simple().to_string()));
    metadata.insert("machine", json!(machine_id));
    metadata.insert("tree", json!(tree_hash.as_hex()));
    metadata.insert("created", json!(created.as_millisecond()));

    // Optional metadata - started and source_path
    metadata.insert("started", json!(started.as_millisecond()));
    metadata.insert("source_path", json!(source_path.to_string_lossy()));

    // Insert mandatory and basic optional metadata
    for (key, value) in &metadata {
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
            params![key, value.to_string()],
        )?;
    }

    // Optional: catalog name
    if let Some(ref name) = args.name {
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
            params!["name", json!(name).to_string()],
        )?;
    }

    // Optional: machine hostname
    if let Some(hostname) = get_hostname() {
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
            params!["machine_hostname", json!(hostname).to_string()],
        )?;
    }

    // Optional: filesystem info
    if let Ok(fs_info) = get_fs_info(&source_path) {
        if let Some(ref fs_type) = fs_info.fs_type {
            conn.execute(
                "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
                params!["fs_type", json!(fs_type).to_string()],
            )?;
        }
        if let Some(ref fs_id) = fs_info.fs_id {
            conn.execute(
                "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
                params!["fs_id", json!(fs_id).to_string()],
            )?;
        }
    }

    // Optional: fs_writeable (true if not readonly)
    if let Ok(readonly) = is_readonly(&source_path)
        && !readonly
    {
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
            params!["fs_writeable", json!(true).to_string()],
        )?;
    }

    // User-provided extra metadata
    for (key, value) in &args.meta {
        let prefixed_key = format!("extra.{}", key);
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES (?1, ?2)",
            params![prefixed_key, json!(value).to_string()],
        )?;
    }

    // Write catalog data
    let stats = write_catalog(&conn, &file_infos)?;

    // Close the connection before compressing
    drop(conn);

    // Compress the catalog file
    if args.compression > 0 {
        info!(level = args.compression, "Compressing catalog");
        let temp_output = tempfile::NamedTempFile::new_in(
            catalog_path.parent().unwrap_or(std::path::Path::new(".")),
        )?;
        compress_file_with_level(catalog_path, temp_output.path(), args.compression)?;
        temp_output.persist(catalog_path)?;
    }

    info!(?catalog_path, "Catalog written");
    eprintln!("Catalog written to {:?}", catalog_path);
    eprintln!("  ID: {}", catalog_id);
    eprintln!("  Tree hash: {}", tree_hash.as_hex());
    eprintln!("  Files: {}", stats.file_count);
    eprintln!(
        "  Extents: {} ({} unique)",
        stats.total_extents, stats.unique_extent_count
    );
    eprintln!(
        "  Total size: {} bytes ({} unique)",
        stats.total_bytes, stats.unique_bytes
    );
    if stats.sparse_bytes > 0 {
        eprintln!("  Sparse holes: {} bytes", stats.sparse_bytes);
    }
    eprintln!(
        "  Dedup ratio: {:.2}x ({:.1}% space saved, {} bytes)",
        stats.dedup_ratio(),
        stats.space_saved_pct(),
        stats.space_saved()
    );

    Ok(())
}
