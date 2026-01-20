//! Tumulus server - HTTP storage backend for tumulus backup system.
//!
//! This crate provides a stateless HTTP server that stores and serves
//! extents, blobs, and catalogs.

pub mod api;
pub mod blob;
pub mod config;
pub mod db;
pub mod storage;

use std::{array::TryFromSliceError, ops::Deref};

pub use api::{
    CatalogError, ErrorResponse, FinalizeResponse, InitiateRequest, InitiateResponse,
    UploadResponse, router,
};
pub use blob::{BlobDecodeError, BlobExtent, BlobLayout, BlobRegion};
pub use config::Config;
pub use db::{CatalogInfo, CatalogStatus, DbError, UploadDb};
pub use storage::{ByteReader, ByteStream, FsStorage, ObjectMeta, Storage, StorageError};

/// Newtype for blake3 hashes used as IDs
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct B3Id(pub blake3::Hash);

impl AsRef<[u8]> for B3Id {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Deref for B3Id {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        self.0.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for B3Id {
    type Error = TryFromSliceError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(blake3::Hash::from_bytes(bytes.as_slice().try_into()?)))
    }
}

impl From<[u8; 32]> for B3Id {
    fn from(value: [u8; 32]) -> Self {
        B3Id(blake3::Hash::from_bytes(value))
    }
}

impl From<blake3::Hash> for B3Id {
    fn from(value: blake3::Hash) -> Self {
        Self(value)
    }
}
