/// A contiguous range of data (or sparse hole) in a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataRange {
    /// Byte offset within the file.
    pub offset: u64,
    /// Length in bytes.
    pub length: u64,
    /// Properties of this range.
    pub flags: RangeFlags,
}

/// Flags describing properties of a data range.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RangeFlags {
    /// This range is a sparse hole (no data stored, reads as zeros).
    pub sparse: bool,
    /// This range is shared with other files (reflink/dedup).
    /// Only reliably detected on Linux via FIEMAP.
    pub shared: bool,
}

impl DataRange {
    /// Create a new data range.
    pub fn new(offset: u64, length: u64) -> Self {
        Self {
            offset,
            length,
            flags: RangeFlags::default(),
        }
    }

    /// Create a sparse hole range.
    pub fn sparse(offset: u64, length: u64) -> Self {
        Self {
            offset,
            length,
            flags: RangeFlags {
                sparse: true,
                shared: false,
            },
        }
    }

    /// The end offset (exclusive) of this range.
    pub fn end(&self) -> u64 {
        self.offset + self.length
    }
}
