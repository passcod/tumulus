//! Fallback range reader that treats the whole file as one extent.
//!
//! This is used on platforms where we don't have a way to query extent information.
//! It simply returns the entire file as a single data range.

use std::{fs::File, io};

use crate::types::{DataRange, RangeIter, RangeReaderImpl, private::Sealed};

/// Fallback range reader that treats the whole file as one extent.
#[derive(Debug)]
pub struct RangeReader;

impl Sealed for RangeReader {}

impl RangeReaderImpl for RangeReader {
    /// Create a new fallback range reader.
    fn new() -> Self {
        Self
    }

    /// Read data ranges for a file.
    ///
    /// On platforms without extent support, this returns the entire file
    /// as a single data range (or nothing for empty files).
    fn read_ranges<'a>(&'a mut self, file: &'a File) -> io::Result<RangeIter<'a>> {
        let len = file.metadata()?.len();
        let range = if len > 0 {
            Some(DataRange::new(0, len))
        } else {
            None
        };
        Ok(Box::new(range.into_iter().map(Ok)))
    }
}

impl Default for RangeReader {
    fn default() -> Self {
        Self::new()
    }
}
