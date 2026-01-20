use bytes::{BufMut, Bytes, BytesMut};

use crate::B3Id;

const BLOB_VERSION: u8 = 0x01;
const EXTENT_ID_SIZE: u8 = 0x20;

#[derive(Debug, Clone)]
pub struct BlobLayout {
    pub total_bytes: u64,
    pub extents: Vec<BlobExtent>,
}

#[derive(Debug, Clone)]
pub struct BlobExtent {
    pub offset: u64,
    pub length: u64,
    pub extent_id: B3Id,
}

/// Represents a region of the blob (either data or hole)
#[derive(Debug, Clone)]
pub enum BlobRegion {
    Data(BlobExtent),
    Hole { offset: u64, length: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum BlobDecodeError {
    #[error("Invalid version: {0}")]
    InvalidVersion(u8),
    #[error("Invalid extent ID size: {0}")]
    InvalidExtentIdSize(u8),
    #[error("Truncated data")]
    Truncated,
    #[error("Extents not sorted by offset")]
    NotSorted,
    #[error("Overlapping extents")]
    Overlapping,
}

impl BlobLayout {
    /// Header size in bytes
    const HEADER_SIZE: usize = 1 + 1 + 8 + 8; // 18 bytes

    /// Size of each extent entry
    const EXTENT_ENTRY_SIZE: usize = 8 + 8 + 32; // 48 bytes

    /// Encode to binary format (only non-sparse extents are written)
    pub fn encode(&self) -> Bytes {
        let size = Self::HEADER_SIZE + self.extents.len() * Self::EXTENT_ENTRY_SIZE;
        let mut buf = BytesMut::with_capacity(size);

        // Header
        buf.put_u8(BLOB_VERSION);
        buf.put_u8(EXTENT_ID_SIZE);
        buf.put_u64_le(self.total_bytes);
        buf.put_u64_le(self.extents.len() as u64);

        // Extent map (only actual extents, not holes)
        for extent in &self.extents {
            buf.put_u64_le(extent.offset);
            buf.put_u64_le(extent.length);
            buf.put_slice(extent.extent_id.as_ref());
        }

        buf.freeze()
    }

    /// Iterate over all regions including holes
    pub fn regions(&self) -> Vec<BlobRegion> {
        let mut regions = Vec::new();
        let mut pos: u64 = 0;

        for extent in &self.extents {
            // Check for hole before this extent
            if extent.offset > pos {
                regions.push(BlobRegion::Hole {
                    offset: pos,
                    length: extent.offset - pos,
                });
            }

            regions.push(BlobRegion::Data(extent.clone()));
            pos = extent.offset + extent.length;
        }

        // Check for trailing hole
        if pos < self.total_bytes {
            regions.push(BlobRegion::Hole {
                offset: pos,
                length: self.total_bytes - pos,
            });
        }

        regions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regions_with_holes() {
        let layout = BlobLayout {
            total_bytes: 1024,
            extents: vec![
                BlobExtent {
                    offset: 100,
                    length: 100,
                    extent_id: [1u8; 32].into(),
                },
                BlobExtent {
                    offset: 500,
                    length: 200,
                    extent_id: [2u8; 32].into(),
                },
            ],
        };

        let regions = layout.regions();

        assert_eq!(regions.len(), 5);

        // Leading hole
        match &regions[0] {
            BlobRegion::Hole { offset, length } => {
                assert_eq!(*offset, 0);
                assert_eq!(*length, 100);
            }
            _ => panic!("Expected hole"),
        }

        // First data
        match &regions[1] {
            BlobRegion::Data(extent) => {
                assert_eq!(extent.offset, 100);
                assert_eq!(extent.length, 100);
            }
            _ => panic!("Expected data"),
        }

        // Middle hole
        match &regions[2] {
            BlobRegion::Hole { offset, length } => {
                assert_eq!(*offset, 200);
                assert_eq!(*length, 300);
            }
            _ => panic!("Expected hole"),
        }

        // Second data
        match &regions[3] {
            BlobRegion::Data(extent) => {
                assert_eq!(extent.offset, 500);
                assert_eq!(extent.length, 200);
            }
            _ => panic!("Expected data"),
        }

        // Trailing hole
        match &regions[4] {
            BlobRegion::Hole { offset, length } => {
                assert_eq!(*offset, 700);
                assert_eq!(*length, 324);
            }
            _ => panic!("Expected hole"),
        }
    }

    #[test]
    fn test_no_holes() {
        let layout = BlobLayout {
            total_bytes: 512,
            extents: vec![
                BlobExtent {
                    offset: 0,
                    length: 256,
                    extent_id: [1u8; 32].into(),
                },
                BlobExtent {
                    offset: 256,
                    length: 256,
                    extent_id: [2u8; 32].into(),
                },
            ],
        };

        let regions = layout.regions();

        assert_eq!(regions.len(), 2);
        assert!(matches!(&regions[0], BlobRegion::Data(_)));
        assert!(matches!(&regions[1], BlobRegion::Data(_)));
    }
}
