//! Compare two catalogs and report transfer requirements

use std::path::PathBuf;

use clap::Args;
use tracing::info;

use tumulus::open_catalog;

/// Compare two catalogs and report transfer requirements
#[derive(Args, Debug)]
pub struct CompareArgs {
    /// Local catalog file (source)
    local_catalog: PathBuf,

    /// Remote catalog file (destination)
    remote_catalog: PathBuf,
}

pub fn run(args: CompareArgs) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let local_path = &args.local_catalog;
    let remote_path = &args.remote_catalog;

    info!(?local_path, ?remote_path, "Comparing catalogs");

    // Open catalogs (automatically decompresses if needed)
    let (local_conn, _local_tempfile) = open_catalog(local_path)?;
    let (remote_conn, _remote_tempfile) = open_catalog(remote_path)?;

    // Get local catalog stats
    let local_extent_count: i64 =
        local_conn.query_row("SELECT COUNT(*) FROM extents", [], |row| row.get(0))?;
    let local_extent_bytes: i64 =
        local_conn.query_row("SELECT COALESCE(SUM(bytes), 0) FROM extents", [], |row| {
            row.get(0)
        })?;

    // Get remote catalog stats
    let remote_extent_count: i64 =
        remote_conn.query_row("SELECT COUNT(*) FROM extents", [], |row| row.get(0))?;
    let remote_extent_bytes: i64 =
        remote_conn.query_row("SELECT COALESCE(SUM(bytes), 0) FROM extents", [], |row| {
            row.get(0)
        })?;

    eprintln!("Local catalog: {:?}", local_path);
    eprintln!("  Extents: {}", local_extent_count);
    eprintln!("  Bytes: {}", local_extent_bytes);
    eprintln!();
    eprintln!("Remote catalog: {:?}", remote_path);
    eprintln!("  Extents: {}", remote_extent_count);
    eprintln!("  Bytes: {}", remote_extent_bytes);
    eprintln!();

    // Attach remote database to local connection for comparison
    // Use the actual file path (which may be a tempfile if compressed)
    let remote_db_path = _remote_tempfile
        .as_ref()
        .map(|t| t.path().to_path_buf())
        .unwrap_or_else(|| remote_path.clone());
    local_conn.execute(
        "ATTACH DATABASE ?1 AS remote",
        [remote_db_path.to_string_lossy().as_ref()],
    )?;

    // Find extents in local that are not in remote
    let (missing_count, missing_bytes): (i64, i64) = local_conn.query_row(
        r#"
        SELECT
            COUNT(*),
            COALESCE(SUM(l.bytes), 0)
        FROM extents l
        WHERE NOT EXISTS (
            SELECT 1 FROM remote.extents r
            WHERE r.extent_id = l.extent_id
        )
        "#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    // Find extents that exist in both
    let (shared_count, shared_bytes): (i64, i64) = local_conn.query_row(
        r#"
        SELECT
            COUNT(*),
            COALESCE(SUM(l.bytes), 0)
        FROM extents l
        WHERE EXISTS (
            SELECT 1 FROM remote.extents r
            WHERE r.extent_id = l.extent_id
        )
        "#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    let transfer_pct = if local_extent_bytes > 0 {
        (missing_bytes as f64 / local_extent_bytes as f64) * 100.0
    } else {
        0.0
    };

    let shared_pct = if local_extent_bytes > 0 {
        (shared_bytes as f64 / local_extent_bytes as f64) * 100.0
    } else {
        0.0
    };

    println!("Transfer required:");
    println!("  Extents to upload: {}", missing_count);
    println!(
        "  Bytes to upload: {} ({:.1}%)",
        missing_bytes, transfer_pct
    );
    println!();
    println!("Already on remote:");
    println!("  Shared extents: {}", shared_count);
    println!("  Shared bytes: {} ({:.1}%)", shared_bytes, shared_pct);

    // Also show what's on remote but not local (informational)
    let (remote_only_count, remote_only_bytes): (i64, i64) = local_conn.query_row(
        r#"
        SELECT
            COUNT(*),
            COALESCE(SUM(r.bytes), 0)
        FROM remote.extents r
        WHERE NOT EXISTS (
            SELECT 1 FROM extents l
            WHERE l.extent_id = r.extent_id
        )
        "#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    if remote_only_count > 0 {
        println!();
        println!("Remote-only (not in local):");
        println!("  Extents: {}", remote_only_count);
        println!("  Bytes: {}", remote_only_bytes);
    }

    info!(
        missing_count,
        missing_bytes, shared_count, shared_bytes, "Comparison complete"
    );

    Ok(())
}
