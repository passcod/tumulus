use std::fs::File;
use std::io;
use std::os::fd::AsFd;

use crate::fiemap::FiemapLookup;
use crate::types::DataRange;

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
    ///
    /// If the filesystem doesn't support FIEMAP (e.g., tmpfs, some network filesystems),
    /// this will fall back to treating the entire file as a single data range.
    pub fn read_ranges<'a>(
        &'a mut self,
        file: &'a File,
    ) -> io::Result<impl Iterator<Item = io::Result<DataRange>> + 'a> {
        let file_size = file.metadata()?.len();

        let fiemap_result = if let Some(buf) = self.buf.take() {
            FiemapLookup::for_file_size(file_size).with_buf(file.as_fd(), buf)
        } else {
            FiemapLookup::for_file_size(file_size).with_buf_size(file.as_fd(), self.buf_size)
        };

        match fiemap_result {
            Ok(results) => Ok(LinuxRangeIter::Fiemap(FiemapRangeIter {
                inner: results,
                file_size,
                current_pos: 0,
                pending_range: None,
                done: false,
            })),
            Err(e) if is_fiemap_unsupported(&e) => {
                // Filesystem doesn't support FIEMAP, fall back to single extent
                Ok(LinuxRangeIter::Fallback(FallbackRangeIter::new(file_size)))
            }
            Err(e) => Err(e),
        }
    }
}

/// Check if an error indicates FIEMAP is not supported by this filesystem.
fn is_fiemap_unsupported(err: &io::Error) -> bool {
    // Note: ENOTSUP and EOPNOTSUPP are the same value on Linux
    matches!(
        err.raw_os_error(),
        Some(libc::EOPNOTSUPP) | Some(libc::ENOTTY)
    )
}

/// Iterator that can be either FIEMAP-based or fallback.
enum LinuxRangeIter<'a> {
    Fiemap(FiemapRangeIter<'a>),
    Fallback(FallbackRangeIter),
}

impl Iterator for LinuxRangeIter<'_> {
    type Item = io::Result<DataRange>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            LinuxRangeIter::Fiemap(iter) => iter.next(),
            LinuxRangeIter::Fallback(iter) => iter.next(),
        }
    }
}

/// Fallback iterator that treats the whole file as a single data range.
struct FallbackRangeIter {
    range: Option<DataRange>,
}

impl FallbackRangeIter {
    fn new(file_size: u64) -> Self {
        let range = if file_size > 0 {
            Some(DataRange::new(0, file_size))
        } else {
            None
        };
        Self { range }
    }
}

impl Iterator for FallbackRangeIter {
    type Item = io::Result<DataRange>;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.take().map(Ok)
    }
}

/// Iterator over FIEMAP results, converting to DataRange.
struct FiemapRangeIter<'a> {
    inner: crate::fiemap::FiemapSearchResults<'a>,
    file_size: u64,
    current_pos: u64,
    pending_range: Option<DataRange>,
    done: bool,
}

impl Iterator for FiemapRangeIter<'_> {
    type Item = io::Result<DataRange>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::types::RangeFlags;

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
