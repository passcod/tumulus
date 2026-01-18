use std::io;
use std::time::SystemTime;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Object not found")]
    NotFound,

    #[error("Hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("Invalid data: {0}")]
    InvalidData(String),
}

/// Metadata about a stored object
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub size: u64,
    pub created: Option<SystemTime>,
}
