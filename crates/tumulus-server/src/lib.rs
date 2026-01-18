//! Tumulus server - HTTP storage backend for tumulus backup system.
//!
//! This crate provides a stateless HTTP server that stores and serves
//! extents, blobs, and catalogs.

pub mod api;
pub mod blob;
pub mod config;
pub mod db;
pub mod storage;

pub use api::{
    CatalogError, ErrorResponse, FinalizeResponse, InitiateRequest, InitiateResponse,
    UploadResponse, router,
};
pub use blob::{BlobDecodeError, BlobExtent, BlobLayout, BlobRegion};
pub use config::Config;
pub use db::{CatalogInfo, CatalogStatus, DbError, UploadDb};
pub use storage::{ByteReader, ByteStream, FsStorage, ObjectMeta, Storage, StorageError};
