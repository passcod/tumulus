use std::fs::File;
use std::io;

use crate::types::DataRange;
use crate::unix_seek;

/// Range reader for macOS using SEEK_HOLE/SEEK_DATA.
pub struct RangeReader {
    // No state needed for seek-based approach
}

impl RangeReader {
    pub fn new() -> Self {
        Self {}
    }

    /// Buffer size is ignored on macOS (no buffer used).
    pub fn with_buffer_size(_size: usize) -> Self {
        Self::new()
    }

    /// Buffer is ignored on macOS.
    pub fn with_buffer(_buf: Box<[u8]>) -> Self {
        Self::new()
    }

    /// Returns None (no buffer used on this platform).
    pub fn into_buffer(self) -> Option<Box<[u8]>> {
        None
    }

    pub fn read_ranges(
        &mut self,
        file: &File,
    ) -> io::Result<impl Iterator<Item = io::Result<DataRange>>> {
        unix_seek::read_ranges(file)
    }
}

impl Default for RangeReader {
    fn default() -> Self {
        Self::new()
    }
}
