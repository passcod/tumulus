//! Extent and blob processing functionality.

use std::{fs::File, io, path::Path};

use blake3::Hasher;
use extentria::{DataRange, RangeReader};
use memmap2::Mmap;
use tracing::debug;

/// Information about a file extent
#[derive(Debug, Clone)]
pub struct ExtentInfo {
    pub extent_id: [u8; 32],
    pub offset: u64,
    pub bytes: u64,
    pub is_sparse: bool,
    pub is_shared: bool,
}

/// Information about a file's blob
#[derive(Debug, Clone)]
pub struct BlobInfo {
    pub blob_id: [u8; 32],
    pub bytes: u64,
    pub extents: Vec<ExtentInfo>,
}

/// Detect sparse holes by finding gaps between extents.
///
/// Takes a list of (logical_offset, length, extent_id) tuples and the total file size,
/// and returns a complete list of ExtentInfo including sparse holes.
pub fn detect_sparse_holes(extents: &[(u64, u64, [u8; 32])], file_size: u64) -> Vec<ExtentInfo> {
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
                is_shared: false,
            });
        }

        result.push(ExtentInfo {
            extent_id: *extent_id,
            offset: *logical_offset,
            bytes: *length,
            is_sparse: false,
            is_shared: false,
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
            is_shared: false,
        });
    }

    result
}

/// Convert a DataRange to ExtentInfo, computing the extent hash from file data.
fn range_to_extent_info(range: &DataRange, mmap: &Mmap) -> ExtentInfo {
    if range.flags.sparse {
        ExtentInfo {
            extent_id: [0u8; 32],
            offset: range.offset,
            bytes: range.length,
            is_sparse: true,
            is_shared: false,
        }
    } else {
        let start = (range.offset as usize).min(mmap.len());
        let end = (start + range.length as usize).min(mmap.len());
        let slice = &mmap[start..end];
        let extent_id = *blake3::hash(slice).as_bytes();

        ExtentInfo {
            extent_id,
            offset: range.offset,
            bytes: (end - start) as u64,
            is_sparse: false,
            is_shared: range.flags.shared,
        }
    }
}

/// Process a file's extents and compute its blob information.
///
/// Returns `None` for empty files or files that cannot have extents.
pub fn process_file_extents(path: &Path) -> io::Result<Option<BlobInfo>> {
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

    // Get extent information using cross-platform API
    let mut reader = RangeReader::new();
    let ranges: Result<Vec<DataRange>, _> = reader.read_ranges(&file)?.collect();
    let ranges = ranges?;

    if ranges.is_empty() {
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
                is_shared: false,
            }],
        }));
    }

    // Convert ranges to ExtentInfo, computing hashes for data ranges
    let extents: Vec<ExtentInfo> = ranges
        .iter()
        .map(|range| range_to_extent_info(range, &mmap))
        .collect();

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

/// Process a file's extents with a reusable RangeReader for better performance
/// when processing multiple files.
pub fn process_file_extents_with_reader(
    path: &Path,
    reader: &mut RangeReader,
) -> io::Result<Option<BlobInfo>> {
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

    // Get extent information using cross-platform API
    let ranges: Result<Vec<DataRange>, _> = reader.read_ranges(&file)?.collect();
    let ranges = ranges?;

    if ranges.is_empty() {
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
                is_shared: false,
            }],
        }));
    }

    // Convert ranges to ExtentInfo, computing hashes for data ranges
    let extents: Vec<ExtentInfo> = ranges
        .iter()
        .map(|range| range_to_extent_info(range, &mmap))
        .collect();

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
