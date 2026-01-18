use std::{fs::File, io, path::PathBuf};

use clap::Parser;
use extentria::{DataRange, RangeReader, can_detect_shared};
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

    // Get extent information using cross-platform API
    let mut reader = RangeReader::new();
    let mut extent_displays: Vec<ExtentDisplay> = Vec::new();
    let mut extent_read_error: Option<io::Error> = None;

    let ranges: Result<Vec<DataRange>, io::Error> = match reader.read_ranges(&file) {
        Ok(iter) => iter.collect(),
        Err(e) => Err(e),
    };

    match ranges {
        Ok(range_list) => {
            for range in range_list {
                if range.flags.sparse {
                    extent_displays.push(ExtentDisplay {
                        logical_offset: range.offset,
                        length: range.length,
                        flags: "sparse".to_string(),
                        is_sparse: true,
                        hash: None,
                        bytes_read: 0,
                    });
                } else {
                    let start = (range.offset as usize).min(file_len);
                    let end = (start + range.length as usize).min(file_len);
                    let slice = &mmap[start..end];
                    let extent_id = blake3::hash(slice);

                    let mut flags = Vec::new();
                    if range.flags.shared {
                        flags.push("shared");
                    }
                    let flags_str = flags.join(",");

                    extent_displays.push(ExtentDisplay {
                        logical_offset: range.offset,
                        length: (end - start) as u64,
                        flags: flags_str,
                        is_sparse: false,
                        hash: Some(hex::encode(extent_id.as_bytes())),
                        bytes_read: end - start,
                    });
                }
            }
        }
        Err(err) => {
            error!(?path, %err, "Error reading extents");
            extent_read_error = Some(err);
        }
    }

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

    if can_detect_shared() {
        debug!("Platform supports shared extent detection");
    } else {
        debug!("Platform does not support shared extent detection");
    }

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
