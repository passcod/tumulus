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
    pub range: DataRange,
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

/// Convert a DataRange to one or more ExtentInfo entries, subchunking large extents.
///
/// If the extent is larger than MAX_EXTENT_SIZE, it will be split into multiple
/// chunks, each with its own hash. All chunks share the same fs_extent value.
fn range_to_extent_infos(range: DataRange, mmap: &Mmap, fs_extent: u32) -> Vec<ExtentInfo> {
    if range.hole {
        // Sparse holes are not subchunked
        return vec![ExtentInfo {
            extent_id: [0u8; 32],
            range,
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
            range: DataRange::new(range.offset, total_len),
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
            range: DataRange::new(chunk_offset, chunk_len),
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
        let extents = range_to_extent_infos(single_range, &mmap, 1);

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

    for range in ranges {
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
        let extents = range_to_extent_infos(single_range, &mmap, 1);

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

    for range in ranges {
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
