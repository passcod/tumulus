use std::fs::File;
use std::io;

/// Iterator over data ranges returned by a RangeReader.
pub type RangeIter<'a> = Box<dyn Iterator<Item = io::Result<DataRange>> + 'a>;

pub(crate) mod private {
    /// Sealed trait marker to prevent external implementations of RangeReaderImpl.
    pub trait Sealed {}
}

/// Trait for platform-specific range reader implementations.
///
/// This trait ensures all platform implementations have a consistent interface
/// for reading file extent/range information.
///
/// This trait is sealed and cannot be implemented outside of this crate.
pub trait RangeReaderImpl: std::fmt::Debug + Default + private::Sealed {
    /// Create a new reader with default buffer size.
    fn new() -> Self;

    /// Create a reader with a specific buffer size.
    ///
    /// On platforms that don't use buffers, this is equivalent to `new()`.
    fn with_buffer_size(size: usize) -> Self {
        let _ = size;
        Self::new()
    }

    /// Create a reader reusing an existing buffer.
    ///
    /// On platforms that don't use buffers, the buffer is ignored.
    #[allow(clippy::boxed_local, reason = "it's the desired interface, dumbass")]
    fn with_buffer(buf: Box<[u8]>) -> Self {
        let _ = buf;
        Self::new()
    }

    /// Consume the reader and return its buffer for reuse.
    ///
    /// Returns `None` on platforms that don't use buffers, or if the buffer
    /// is currently in use by an active iterator.
    fn into_buffer(self) -> Option<Box<[u8]>> {
        None
    }

    /// Read data ranges for a file.
    ///
    /// Returns an iterator that yields data ranges (including sparse holes)
    /// for the file. The iterator may lazily fetch data from the kernel.
    fn read_ranges<'a>(&'a mut self, file: &'a File) -> io::Result<RangeIter<'a>>;
}

/// A contiguous range of data (or sparse hole) in a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataRange {
    /// Byte offset within the file.
    pub offset: u64,
    /// Length in bytes.
    pub length: u64,
    /// This range is a sparse hole (no data stored, reads as zeros).
    pub hole: bool,
}

impl DataRange {
    /// Create a new data range.
    pub fn new(offset: u64, length: u64) -> Self {
        Self {
            offset,
            length,
            hole: false,
        }
    }

    /// Create a sparse hole range.
    pub fn hole(offset: u64, length: u64) -> Self {
        Self {
            offset,
            length,
            hole: true,
        }
    }

    /// The end offset (exclusive) of this range.
    pub fn end(&self) -> u64 {
        self.offset + self.length
    }
}
