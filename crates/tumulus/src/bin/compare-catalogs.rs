use std::path::PathBuf;

use clap::Parser;
use lloggs::LoggingArgs;
use rusqlite::Connection;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "compare-catalogs")]
#[command(about = "Compare two catalogs and report transfer requirements")]
struct Args {
    /// Local catalog file (source)
    local_catalog: PathBuf,

    /// Remote catalog file (destination)
    remote_catalog: PathBuf,

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

    let local_path = &args.local_catalog;
    let remote_path = &args.remote_catalog;

    info!(?local_path, ?remote_path, "Comparing catalogs");

    let local_conn = Connection::open(local_path)?;
    let remote_conn = Connection::open(remote_path)?;

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
    local_conn.execute(
        "ATTACH DATABASE ?1 AS remote",
        [remote_path.to_string_lossy().as_ref()],
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
