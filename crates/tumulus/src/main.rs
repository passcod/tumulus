use std::path::PathBuf;

use clap::Parser;
use jiff::Timestamp;
use lloggs::LoggingArgs;
use rayon::prelude::*;
use rusqlite::{Connection, params};
use serde_json::json;
use tracing::{error, info, warn};
use uuid::Uuid;
use walkdir::WalkDir;

use tumulus::{
    FileInfo, compute_tree_hash, create_catalog_schema, get_machine_id, process_file, write_catalog,
};

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

    // Write catalog data
    let stats = write_catalog(&conn, &file_infos)?;

    info!(?catalog_path, "Catalog written");
    eprintln!("Catalog written to {:?}", catalog_path);
    eprintln!("  ID: {}", catalog_id);
    eprintln!("  Tree hash: {}", hex::encode(tree_hash));
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
