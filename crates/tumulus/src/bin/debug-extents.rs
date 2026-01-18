use std::{convert::identity, fs::File, io, path::PathBuf};

use clap::Parser;
use extentria::fiemap::FiemapLookup;
use lloggs::LoggingArgs;
use memmap2::Mmap;
use rayon::prelude::*;
use tracing::{debug, error, info, warn};

use tumulus::extents::detect_sparse_holes;

#[derive(Parser, Debug)]
#[command(name = "debug-extents")]
#[command(about = "Display extent information for files")]
struct Args {
    /// Files to analyze
    #[arg(required = true)]
    paths: Vec<PathBuf>,

    /// Make extent read errors fatal (exit on first error)
    #[arg(long, short = 'e')]
    fatal_errors: bool,

    #[command(flatten)]
    logging: LoggingArgs,
}

struct ExtentDisplay {
    logical_offset: u64,
    length: u64,
    flags: String,
    is_sparse: bool,
    hash: Option<String>,
    bytes_read: usize,
}

struct FileResult {
    path: PathBuf,
    extents: Vec<ExtentDisplay>,
    file_hash: String,
    total_read: u64,
    true_size: u64,
    sparse_bytes: u64,
    extent_read_error: Option<io::Error>,
}

fn process_file(path: PathBuf) -> Result<FileResult, std::io::Error> {
    debug!(?path, "Processing file");

    let file = File::open(&path)?;
    let true_size = file.metadata()?.len();

    if true_size == 0 {
        return Ok(FileResult {
            path,
            extents: Vec::new(),
            file_hash: blake3::hash(&[]).to_hex().to_string(),
            total_read: 0,
            true_size: 0,
            sparse_bytes: 0,
            extent_read_error: None,
        });
    }

    let mmap = unsafe { Mmap::map(&file)? };
    let file_len = mmap.len();

    // Get extent information from FIEMAP and compute hashes
    // Collect extents until we hit an error, then stop but keep what we have
    let mut raw_extents: Vec<(u64, u64, [u8; 32], String)> = Vec::new();
    let mut extent_read_error: Option<io::Error> = None;

    for result in FiemapLookup::extents_for_file(&file)? {
        match result {
            Ok(extent) => {
                let start = (extent.logical_offset as usize).min(file_len);
                let end = (start + extent.length as usize).min(file_len);
                let slice = &mmap[start..end];
                let extent_id = *blake3::hash(slice).as_bytes();

                let flags = [
                    extent.encrypted().then_some("encrypted"),
                    extent.encoded().then_some("encoded"),
                    extent.inline().then_some("inline"),
                    extent.shared().then_some("shared"),
                    extent.delayed_allocation().then_some("delayed"),
                    extent.location_unknown().then_some("unknown"),
                    extent.not_aligned().then_some("unaligned"),
                    extent.packed().then_some("packed"),
                    extent.simulated().then_some("sim"),
                    extent.unwritten().then_some("unwritten"),
                    extent.last().then_some("last"),
                ]
                .into_iter()
                .filter_map(identity)
                .collect::<Vec<_>>()
                .join(",");

                raw_extents.push((
                    extent.logical_offset,
                    (end - start) as u64,
                    extent_id,
                    flags,
                ));
            }
            Err(err) => {
                error!(?path, %err, extents_read = raw_extents.len(), "Error reading extents, stopping");
                extent_read_error = Some(err);
                break;
            }
        }
    }

    // Convert to format needed by detect_sparse_holes
    let extents_for_holes: Vec<(u64, u64, [u8; 32])> = raw_extents
        .iter()
        .map(|(offset, len, id, _)| (*offset, *len, *id))
        .collect();

    // Detect sparse holes using library function
    let extents_with_holes = detect_sparse_holes(&extents_for_holes, true_size);

    // Build a map from (offset, bytes) to flags for non-sparse extents
    let flags_map: std::collections::HashMap<(u64, u64), &str> = raw_extents
        .iter()
        .map(|(offset, len, _, flags)| ((*offset, *len), flags.as_str()))
        .collect();

    // Process extents in parallel
    let extent_displays: Vec<ExtentDisplay> = extents_with_holes
        .into_par_iter()
        .map(|extent| {
            if extent.is_sparse {
                ExtentDisplay {
                    logical_offset: extent.offset,
                    length: extent.bytes,
                    flags: "sparse".to_string(),
                    is_sparse: true,
                    hash: None,
                    bytes_read: 0,
                }
            } else {
                let flags = flags_map
                    .get(&(extent.offset, extent.bytes))
                    .copied()
                    .unwrap_or("")
                    .to_string();

                ExtentDisplay {
                    logical_offset: extent.offset,
                    length: extent.bytes,
                    flags,
                    is_sparse: false,
                    hash: Some(hex::encode(extent.extent_id)),
                    bytes_read: extent.bytes as usize,
                }
            }
        })
        .collect();

    // Compute file hash in parallel
    let mut file_hasher = blake3::Hasher::new();
    file_hasher.update_rayon(&mmap[..]);
    let file_hash = file_hasher.finalize().to_hex().to_string();

    let total_read: u64 = extent_displays.iter().map(|r| r.bytes_read as u64).sum();
    let sparse_bytes: u64 = extent_displays
        .iter()
        .filter(|r| r.is_sparse)
        .map(|r| r.length)
        .sum();

    Ok(FileResult {
        path,
        extents: extent_displays,
        file_hash,
        total_read,
        true_size,
        sparse_bytes,
        extent_read_error,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let _guard = args.logging.setup(|v| match v {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    })?;

    info!(files = args.paths.len(), "Starting extent analysis");

    // Process all files in parallel
    let results: Vec<_> = args
        .paths
        .clone()
        .into_par_iter()
        .map(|path| (path.clone(), process_file(path)))
        .collect();

    let mut had_errors = false;

    // Print results in order
    for (path, result) in results {
        match result {
            Ok(file_result) => {
                for ext in &file_result.extents {
                    let hash_str = ext.hash.as_deref().unwrap_or("(sparse)");
                    println!(
                        "{}\textent start={:7}\tend={:7}\tsize={:7}\tflags={}\thash={}\tread={}",
                        file_result.path.display(),
                        ext.logical_offset,
                        ext.logical_offset + ext.length,
                        ext.length,
                        ext.flags,
                        hash_str,
                        ext.bytes_read,
                    );
                }

                let sparse_info = if file_result.sparse_bytes > 0 {
                    format!("\tsparse={}", file_result.sparse_bytes)
                } else {
                    String::new()
                };

                let error_info = if file_result.extent_read_error.is_some() {
                    "\t(incomplete due to error)"
                } else {
                    ""
                };

                println!(
                    "{}\tfile\tsize={}\ttrue={}\thash={}{}{}",
                    file_result.path.display(),
                    file_result.total_read,
                    file_result.true_size,
                    file_result.file_hash,
                    sparse_info,
                    error_info,
                );

                if file_result.extent_read_error.is_some() {
                    had_errors = true;
                }
            }
            Err(err) => {
                had_errors = true;
                if args.fatal_errors {
                    error!(?path, %err, "Fatal error reading file");
                    return Err(err.into());
                } else {
                    warn!(?path, %err, "Skipping file due to error");
                }
            }
        }
    }

    if had_errors && !args.fatal_errors {
        warn!("Some files were skipped due to errors");
    }

    Ok(())
}
