use std::fs::File;
use std::io;

use crate::types::DataRange;
use crate::unix_seek;

/// Range reader for FreeBSD using SEEK_HOLE/SEEK_DATA.
pub struct RangeReader {}

impl RangeReader {
    pub fn new() -> Self {
        Self {}
    }

    pub fn with_buffer_size(_size: usize) -> Self {
        Self::new()
    }

    pub fn with_buffer(_buf: Box<[u8]>) -> Self {
        Self::new()
    }

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
