//! Extent and blob processing functionality.

use std::{fs::File, io, path::Path};

use blake3::Hasher;
use extentria::{DataRange, RangeReader, RangeReaderImpl};
use memmap2::Mmap;
use tracing::debug;

/// Maximum size for a single extent chunk (128 KB).
pub const MAX_EXTENT_SIZE: u64 = 128 * 1024;

/// Information about a file extent
#[derive(Debug, Clone)]
pub struct ExtentInfo {
    pub extent_id: [u8; 32],
    pub offset: u64,
    pub bytes: u64,
    pub is_sparse: bool,
    pub is_shared: bool,
    /// Filesystem extent index - incremented for each new filesystem extent.
    /// Multiple ExtentInfo entries with the same fs_extent value are subchunks
    /// of the same underlying filesystem extent.
    pub fs_extent: u32,
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
                fs_extent: 0, // Legacy function, fs_extent not tracked
            });
        }

        result.push(ExtentInfo {
            extent_id: *extent_id,
            offset: *logical_offset,
            bytes: *length,
            is_sparse: false,
            is_shared: false,
            fs_extent: 0, // Legacy function, fs_extent not tracked
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
            fs_extent: 0, // Legacy function, fs_extent not tracked
        });
    }

    result
}

/// Convert a DataRange to one or more ExtentInfo entries, subchunking large extents.
///
/// If the extent is larger than MAX_EXTENT_SIZE, it will be split into multiple
/// chunks, each with its own hash. All chunks share the same fs_extent value.
fn range_to_extent_infos(range: &DataRange, mmap: &Mmap, fs_extent: u32) -> Vec<ExtentInfo> {
    if range.flags.sparse {
        // Sparse holes are not subchunked - they represent gaps in the file
        return vec![ExtentInfo {
            extent_id: [0u8; 32],
            offset: range.offset,
            bytes: range.length,
            is_sparse: true,
            is_shared: false,
            fs_extent,
        }];
    }

    let start = (range.offset as usize).min(mmap.len());
    let end = (start + range.length as usize).min(mmap.len());
    let total_len = (end - start) as u64;

    if total_len == 0 {
        return vec![];
    }

    // If extent fits in one chunk, no subchunking needed
    if total_len <= MAX_EXTENT_SIZE {
        let slice = &mmap[start..end];
        let extent_id = *blake3::hash(slice).as_bytes();

        return vec![ExtentInfo {
            extent_id,
            offset: range.offset,
            bytes: total_len,
            is_sparse: false,
            is_shared: range.flags.shared,
            fs_extent,
        }];
    }

    // Subchunk the extent into MAX_EXTENT_SIZE pieces
    let mut chunks = Vec::new();
    let mut chunk_start = start;
    let mut chunk_offset = range.offset;

    while chunk_start < end {
        let chunk_end = (chunk_start + MAX_EXTENT_SIZE as usize).min(end);
        let chunk_len = (chunk_end - chunk_start) as u64;

        let slice = &mmap[chunk_start..chunk_end];
        let extent_id = *blake3::hash(slice).as_bytes();

        debug!(
            fs_extent,
            offset = chunk_offset,
            bytes = chunk_len,
            "Created subchunk"
        );

        chunks.push(ExtentInfo {
            extent_id,
            offset: chunk_offset,
            bytes: chunk_len,
            is_sparse: false,
            is_shared: range.flags.shared,
            fs_extent,
        });

        chunk_start = chunk_end;
        chunk_offset += chunk_len;
    }

    chunks
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
        // Still apply subchunking if file is large
        let single_range = DataRange::new(0, file_len);
        let extents = range_to_extent_infos(&single_range, &mmap, 1);

        let mut blob_hasher = Hasher::new();
        blob_hasher.update(&mmap[..]);
        let blob_id = *blob_hasher.finalize().as_bytes();

        return Ok(Some(BlobInfo {
            blob_id,
            bytes: file_len,
            extents,
        }));
    }

    // Convert ranges to ExtentInfo with subchunking, computing hashes for data ranges
    // Each filesystem extent gets a unique fs_extent index
    let mut extents: Vec<ExtentInfo> = Vec::new();
    let mut fs_extent_idx: u32 = 0;

    for range in &ranges {
        fs_extent_idx += 1;
        let chunk_infos = range_to_extent_infos(range, &mmap, fs_extent_idx);
        extents.extend(chunk_infos);
    }

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
        // Still apply subchunking if file is large
        let single_range = DataRange::new(0, file_len);
        let extents = range_to_extent_infos(&single_range, &mmap, 1);

        let mut blob_hasher = Hasher::new();
        blob_hasher.update(&mmap[..]);
        let blob_id = *blob_hasher.finalize().as_bytes();

        return Ok(Some(BlobInfo {
            blob_id,
            bytes: file_len,
            extents,
        }));
    }

    // Convert ranges to ExtentInfo with subchunking, computing hashes for data ranges
    // Each filesystem extent gets a unique fs_extent index
    let mut extents: Vec<ExtentInfo> = Vec::new();
    let mut fs_extent_idx: u32 = 0;

    for range in &ranges {
        fs_extent_idx += 1;
        let chunk_infos = range_to_extent_infos(range, &mmap, fs_extent_idx);
        extents.extend(chunk_infos);
    }

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
