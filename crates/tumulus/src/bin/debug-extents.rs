use std::{convert::identity, fs::File, path::PathBuf};

use extentria::fiemap::FiemapLookup;
use memmap2::Mmap;
use rayon::prelude::*;

struct FileExtent {
    logical_offset: u64,
    length: u64,
    flags: String,
}

struct ExtentResult {
    extent: FileExtent,
    hash: String,
    bytes_read: usize,
}

struct FileResult {
    path: PathBuf,
    extent_results: Vec<ExtentResult>,
    file_hash: String,
    total_read: u64,
    true_size: u64,
}

fn process_file(path: PathBuf) -> std::io::Result<FileResult> {
    // Collect extent info first (FIEMAP is synchronous)
    let file = File::open(&path)?;
    let extent_infos: Vec<FileExtent> = FiemapLookup::extents_for_file(&file)?
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

            FileExtent {
                logical_offset: extent.logical_offset,
                length: extent.length,
                flags,
            }
        })
        .collect();

    // Memory-map the file
    let file = File::open(&path)?;
    let true_size = file.metadata()?.len();
    let mmap = unsafe { Mmap::map(&file)? };
    let file_len = mmap.len();

    // Process extents in parallel
    let extent_results: Vec<ExtentResult> = extent_infos
        .into_par_iter()
        .map(|extent| {
            let start = (extent.logical_offset as usize).min(file_len);
            let end = (start + extent.length as usize).min(file_len);
            let slice = &mmap[start..end];

            let hash = blake3::hash(slice).to_hex().to_string();

            ExtentResult {
                extent,
                hash,
                bytes_read: end - start,
            }
        })
        .collect();

    // Compute file hash in parallel using update_rayon
    let mut file_hasher = blake3::Hasher::new();
    file_hasher.update_rayon(&mmap[..]);
    let file_hash = file_hasher.finalize().to_hex().to_string();

    let total_read = extent_results.iter().map(|r| r.bytes_read as u64).sum();

    Ok(FileResult {
        path,
        extent_results,
        file_hash,
        total_read,
        true_size,
    })
}

fn main() -> std::io::Result<()> {
    let paths: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();

    if paths.is_empty() {
        eprintln!("USAGE: debug-extents PATH [PATH...]");
        std::process::exit(1);
    }

    // Process all files in parallel
    let results: Vec<_> = paths
        .into_par_iter()
        .map(|path| (path.clone(), process_file(path)))
        .collect();

    // Print results in order
    for (path, result) in results {
        match result {
            Ok(file_result) => {
                for er in &file_result.extent_results {
                    println!(
                        "{}\textent start={:7}\tend={:7}\tsize={:7}\tflags={}\thash={}\tread={}",
                        file_result.path.display(),
                        er.extent.logical_offset,
                        er.extent.logical_offset + er.extent.length,
                        er.extent.length,
                        er.extent.flags,
                        er.hash,
                        er.bytes_read,
                    );
                }
                println!(
                    "{}\tfile\tsize={}\ttrue={}\thash={}",
                    file_result.path.display(),
                    file_result.total_read,
                    file_result.true_size,
                    file_result.file_hash,
                );
            }
            Err(err) => {
                eprintln!("{}\terror: {}", path.display(), err);
            }
        }
    }

    Ok(())
}
