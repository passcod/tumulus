use std::{convert::identity, fs::File, path::PathBuf};

use clap::Parser;
use extentria::fiemap::FiemapLookup;
use lloggs::LoggingArgs;
use memmap2::Mmap;
use rayon::prelude::*;
use tracing::{debug, error, info, warn};

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

struct FileExtent {
    logical_offset: u64,
    length: u64,
    flags: String,
    is_sparse: bool,
}

struct ExtentResult {
    extent: FileExtent,
    hash: Option<String>,
    bytes_read: usize,
}

struct FileResult {
    path: PathBuf,
    extent_results: Vec<ExtentResult>,
    file_hash: String,
    total_read: u64,
    true_size: u64,
    sparse_bytes: u64,
}

fn detect_sparse_holes(extents: &[(u64, u64, String)], file_size: u64) -> Vec<FileExtent> {
    let mut result = Vec::new();
    let mut current_pos: u64 = 0;

    for (logical_offset, length, flags) in extents {
        // If there's a gap before this extent, it's a sparse hole
        if *logical_offset > current_pos {
            let hole_size = logical_offset - current_pos;
            debug!(
                offset = current_pos,
                size = hole_size,
                "Detected sparse hole"
            );
            result.push(FileExtent {
                logical_offset: current_pos,
                length: hole_size,
                flags: "sparse".to_string(),
                is_sparse: true,
            });
        }

        result.push(FileExtent {
            logical_offset: *logical_offset,
            length: *length,
            flags: flags.clone(),
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
        result.push(FileExtent {
            logical_offset: current_pos,
            length: hole_size,
            flags: "sparse".to_string(),
            is_sparse: true,
        });
    }

    result
}

fn process_file(path: PathBuf) -> Result<FileResult, std::io::Error> {
    debug!(?path, "Processing file");

    let file = File::open(&path)?;
    let true_size = file.metadata()?.len();

    if true_size == 0 {
        return Ok(FileResult {
            path,
            extent_results: Vec::new(),
            file_hash: blake3::hash(&[]).to_hex().to_string(),
            total_read: 0,
            true_size: 0,
            sparse_bytes: 0,
        });
    }

    let mmap = unsafe { Mmap::map(&file)? };
    let file_len = mmap.len();

    // Get extent information from FIEMAP
    let raw_extents: Vec<(u64, u64, String)> = FiemapLookup::extents_for_file(&file)?
        .filter_map(|r| r.ok())
        .map(|extent| {
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

            (extent.logical_offset, extent.length, flags)
        })
        .collect();

    // Detect sparse holes
    let extents_with_holes = detect_sparse_holes(&raw_extents, true_size);

    // Process extents in parallel (skip sparse holes for hashing)
    let extent_results: Vec<ExtentResult> = extents_with_holes
        .into_par_iter()
        .map(|extent| {
            if extent.is_sparse {
                ExtentResult {
                    extent,
                    hash: None,
                    bytes_read: 0,
                }
            } else {
                let start = (extent.logical_offset as usize).min(file_len);
                let end = (start + extent.length as usize).min(file_len);
                let slice = &mmap[start..end];
                let hash = blake3::hash(slice).to_hex().to_string();

                ExtentResult {
                    bytes_read: end - start,
                    hash: Some(hash),
                    extent,
                }
            }
        })
        .collect();

    // Compute file hash in parallel
    let mut file_hasher = blake3::Hasher::new();
    file_hasher.update_rayon(&mmap[..]);
    let file_hash = file_hasher.finalize().to_hex().to_string();

    let total_read: u64 = extent_results.iter().map(|r| r.bytes_read as u64).sum();
    let sparse_bytes: u64 = extent_results
        .iter()
        .filter(|r| r.extent.is_sparse)
        .map(|r| r.extent.length)
        .sum();

    Ok(FileResult {
        path,
        extent_results,
        file_hash,
        total_read,
        true_size,
        sparse_bytes,
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
                for er in &file_result.extent_results {
                    let hash_str = er.hash.as_deref().unwrap_or("(sparse)");
                    println!(
                        "{}\textent start={:7}\tend={:7}\tsize={:7}\tflags={}\thash={}\tread={}",
                        file_result.path.display(),
                        er.extent.logical_offset,
                        er.extent.logical_offset + er.extent.length,
                        er.extent.length,
                        er.extent.flags,
                        hash_str,
                        er.bytes_read,
                    );
                }

                let sparse_info = if file_result.sparse_bytes > 0 {
                    format!("\tsparse={}", file_result.sparse_bytes)
                } else {
                    String::new()
                };

                println!(
                    "{}\tfile\tsize={}\ttrue={}\thash={}{}",
                    file_result.path.display(),
                    file_result.total_read,
                    file_result.true_size,
                    file_result.file_hash,
                    sparse_info,
                );
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
