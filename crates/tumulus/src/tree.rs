//! Tree hash computation for snapshot deduplication.

use std::collections::BTreeMap;

use blake3::Hasher;

use crate::file::FileInfo;

/// Compute the tree hash for a set of files.
///
/// The tree hash is a BLAKE3 hash of a rigidly-structured mapping from file paths
/// to blob IDs. It's used to quickly determine if two snapshots have identical
/// file contents without comparing individual files.
///
/// The tree data is a byte-wise sorted list with each item being:
/// - 4 bytes (u32 LE): size of the filepath (P)
/// - P bytes: filepath in bytes with unix slashes
/// - 32 bytes: blob ID
///
/// Files without blobs (special files like symlinks) are not included in the tree hash.
pub fn compute_tree_hash(files: &[FileInfo]) -> [u8; 32] {
    // Build sorted tree map: path -> blob_id
    let mut tree_entries: BTreeMap<&str, &[u8; 32]> = BTreeMap::new();

    for file in files {
        if let Some(ref blob) = file.blob {
            tree_entries.insert(&file.relative_path, &blob.blob_id);
        }
    }

    // Hash the tree
    let mut hasher = Hasher::new();
    for (path, blob_id) in tree_entries {
        let path_bytes = path.as_bytes();
        let path_len = (path_bytes.len() as u32).to_le_bytes();
        hasher.update(&path_len);
        hasher.update(path_bytes);
        hasher.update(blob_id);
    }

    *hasher.finalize().as_bytes()
}
