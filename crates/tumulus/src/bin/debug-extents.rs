use std::{convert::identity, fs::File};

use extentria::fiemap::FiemapLookup;
use memmap2::Mmap;
use rayon::prelude::*;

struct ExtentInfo {
    logical_offset: u64,
    length: u64,
    flags: String,
}

struct ExtentResult {
    info: ExtentInfo,
    hash: String,
    bytes_read: usize,
}

fn main() -> std::io::Result<()> {
    let path = std::env::args().nth(1).expect("USAGE: debug-extents PATH");

    // Collect extent info first (FIEMAP is synchronous)
    let file = File::open(&path)?;
    let extent_infos: Vec<ExtentInfo> = FiemapLookup::extents_for_file(&file)?
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

            ExtentInfo {
                logical_offset: extent.logical_offset,
                length: extent.length,
                flags,
            }
        })
        .collect();

    // Memory-map the file
    let file = File::open(&path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let file_len = mmap.len();

    // Process extents in parallel
    let results: Vec<ExtentResult> = extent_infos
        .into_par_iter()
        .map(|info| {
            let start = (info.logical_offset as usize).min(file_len);
            let end = (start + info.length as usize).min(file_len);
            let slice = &mmap[start..end];

            let hash = blake3::hash(slice).to_hex().to_string();

            ExtentResult {
                info,
                hash,
                bytes_read: end - start,
            }
        })
        .collect();

    // Compute file hash in parallel using update_rayon
    let mut file_hasher = blake3::Hasher::new();
    file_hasher.update_rayon(&mmap[..]);
    let file_hash = file_hasher.finalize();

    // Print results in order
    let mut total_length = 0u64;

    for result in results {
        let info = &result.info;
        println!(
            "extent start={:7}\tend={:7}\tsize={:7}\tflags={}\thash={}\tread={}",
            info.logical_offset,
            info.logical_offset + info.length,
            info.length,
            info.flags,
            result.hash,
            result.bytes_read,
        );

        total_length += result.bytes_read as u64;
    }

    println!(
        "file\tsize={total_length}\ttrue={}\thash={}",
        file.metadata()?.len(),
        file_hash.to_hex()
    );

    Ok(())
}
