use std::fs::File;
use std::io;
use std::os::fd::AsFd;

use crate::fiemap::FiemapLookup;
use crate::types::{DataRange, RangeFlags};

/// Range reader for Linux using FIEMAP.
pub struct RangeReader {
    buf_size: usize,
    buf: Option<Box<[u8]>>,
}

impl RangeReader {
    /// Create a new reader with default buffer size.
    pub fn new() -> Self {
        Self {
            buf_size: 64 * 1024, // 64KB default
            buf: None,
        }
    }

    /// Create a reader with a specific buffer size.
    pub fn with_buffer_size(size: usize) -> Self {
        Self {
            buf_size: size,
            buf: None,
        }
    }

    /// Create a reader reusing an existing buffer.
    pub fn with_buffer(buf: Box<[u8]>) -> Self {
        let buf_size = buf.len();
        Self {
            buf_size,
            buf: Some(buf),
        }
    }

    /// Consume the reader and return its buffer for reuse.
    pub fn into_buffer(self) -> Option<Box<[u8]>> {
        self.buf
    }

    /// Read data ranges for a file.
    pub fn read_ranges<'a>(
        &'a mut self,
        file: &'a File,
    ) -> io::Result<impl Iterator<Item = io::Result<DataRange>> + 'a> {
        let file_size = file.metadata()?.len();

        let results = if let Some(buf) = self.buf.take() {
            FiemapLookup::for_file_size(file_size).with_buf(file.as_fd(), buf)?
        } else {
            FiemapLookup::for_file_size(file_size).with_buf_size(file.as_fd(), self.buf_size)?
        };

        Ok(LinuxRangeIter {
            inner: results,
            file_size,
            current_pos: 0,
            pending_range: None,
            done: false,
        })
    }
}

struct LinuxRangeIter<'a> {
    inner: crate::fiemap::FiemapSearchResults<'a>,
    file_size: u64,
    current_pos: u64,
    pending_range: Option<DataRange>,
    done: bool,
}

impl Iterator for LinuxRangeIter<'_> {
    type Item = io::Result<DataRange>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        // Return any pending range first
        if let Some(range) = self.pending_range.take() {
            return Some(Ok(range));
        }

        match self.inner.next() {
            Some(Ok(extent)) => {
                // Check for sparse hole before this extent
                if extent.logical_offset > self.current_pos {
                    let hole = DataRange::sparse(
                        self.current_pos,
                        extent.logical_offset - self.current_pos,
                    );

                    // Store the data range to return next iteration
                    let range = DataRange {
                        offset: extent.logical_offset,
                        length: extent.length,
                        flags: RangeFlags {
                            sparse: false,
                            shared: extent.shared(),
                        },
                    };
                    self.current_pos = extent.logical_offset + extent.length;

                    if extent.last() && self.current_pos >= self.file_size {
                        self.done = true;
                    }

                    self.pending_range = Some(range);
                    return Some(Ok(hole));
                }

                // Return this extent as a data range
                let range = DataRange {
                    offset: extent.logical_offset,
                    length: extent.length,
                    flags: RangeFlags {
                        sparse: false,
                        shared: extent.shared(),
                    },
                };
                self.current_pos = extent.logical_offset + extent.length;

                if extent.last() && self.current_pos >= self.file_size {
                    self.done = true;
                }

                Some(Ok(range))
            }
            Some(Err(e)) => Some(Err(e)),
            None => {
                // Check for trailing sparse hole
                if self.current_pos < self.file_size {
                    let hole =
                        DataRange::sparse(self.current_pos, self.file_size - self.current_pos);
                    self.current_pos = self.file_size;
                    self.done = true;
                    return Some(Ok(hole));
                }
                self.done = true;
                None
            }
        }
    }
}

impl Default for RangeReader {
    fn default() -> Self {
        Self::new()
    }
}
