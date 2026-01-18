//! Fallback range reader that treats the whole file as one extent.
//!
//! This is used on platforms where we don't have a way to query extent information.
//! It simply returns the entire file as a single data range.

use std::fs::File;
use std::io;

use crate::types::DataRange;

/// Fallback range reader that treats the whole file as one extent.
pub struct RangeReader;

impl RangeReader {
    /// Create a new fallback range reader.
    pub fn new() -> Self {
        Self
    }

    /// Buffer size is ignored on this platform (no buffer used).
    pub fn with_buffer_size(_size: usize) -> Self {
        Self::new()
    }

    /// Buffer is ignored on this platform.
    pub fn with_buffer(_buf: Box<[u8]>) -> Self {
        Self::new()
    }

    /// Returns None (no buffer used on this platform).
    pub fn into_buffer(self) -> Option<Box<[u8]>> {
        None
    }

    /// Read data ranges for a file.
    ///
    /// On platforms without extent support, this returns the entire file
    /// as a single data range (or nothing for empty files).
    pub fn read_ranges(
        &mut self,
        file: &File,
    ) -> io::Result<impl Iterator<Item = io::Result<DataRange>>> {
        let len = file.metadata()?.len();
        let range = if len > 0 {
            Some(DataRange::new(0, len))
        } else {
            None
        };
        Ok(range.into_iter().map(Ok))
    }
}

impl Default for RangeReader {
    fn default() -> Self {
        Self::new()
    }
}
