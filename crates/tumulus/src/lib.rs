//! Tumulus - Snapshot catalog builder
//!
//! This library provides functionality to build snapshot catalogs from directory trees,
//! tracking file extents, blobs, and metadata in a SQLite database.

pub mod catalog;
pub mod compression;
pub mod extents;
pub mod file;
pub mod fsinfo;
pub mod machine;
pub mod tree;

pub use catalog::{CatalogStats, create_catalog_schema, write_catalog};
pub use compression::{
    DEFAULT_COMPRESSION_LEVEL, compress_catalog_in_place, compress_file, decompress_file,
    is_zstd_compressed, open_catalog,
};
pub use extentria::{RangeReader, RangeReaderImpl};
pub use extents::{
    BlobInfo, ExtentInfo, MAX_EXTENT_SIZE, process_file_extents, process_file_extents_with_reader,
};
pub use file::{FileInfo, process_file, process_file_with_reader};
pub use fsinfo::{FsInfo, get_fs_info, get_hostname, is_readonly};
pub use machine::get_machine_id;
pub use tree::compute_tree_hash;
