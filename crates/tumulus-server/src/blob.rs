use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    pub extent_id: [u8; 32],
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
            buf.put_slice(&extent.extent_id);
        }

        buf.freeze()
    }

    /// Decode from binary format
    pub fn decode(mut data: &[u8]) -> Result<Self, BlobDecodeError> {
        if data.len() < Self::HEADER_SIZE {
            return Err(BlobDecodeError::Truncated);
        }

        let version = data.get_u8();
        if version != BLOB_VERSION {
            return Err(BlobDecodeError::InvalidVersion(version));
        }

        let id_size = data.get_u8();
        if id_size != EXTENT_ID_SIZE {
            return Err(BlobDecodeError::InvalidExtentIdSize(id_size));
        }

        let total_bytes = data.get_u64_le();
        let extent_count = data.get_u64_le() as usize;

        let expected_size = extent_count * Self::EXTENT_ENTRY_SIZE;
        if data.len() < expected_size {
            return Err(BlobDecodeError::Truncated);
        }

        let mut extents = Vec::with_capacity(extent_count);
        let mut prev_end: u64 = 0;

        for _ in 0..extent_count {
            let offset = data.get_u64_le();
            let length = data.get_u64_le();

            let mut extent_id = [0u8; 32];
            data.copy_to_slice(&mut extent_id);

            // Validate ordering
            if offset < prev_end {
                if offset + length > prev_end {
                    return Err(BlobDecodeError::Overlapping);
                }
                return Err(BlobDecodeError::NotSorted);
            }

            prev_end = offset + length;

            extents.push(BlobExtent {
                offset,
                length,
                extent_id,
            });
        }

        Ok(Self {
            total_bytes,
            extents,
        })
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
    fn test_encode_decode_roundtrip() {
        let layout = BlobLayout {
            total_bytes: 1024,
            extents: vec![
                BlobExtent {
                    offset: 0,
                    length: 256,
                    extent_id: [1u8; 32],
                },
                BlobExtent {
                    offset: 512,
                    length: 256,
                    extent_id: [2u8; 32],
                },
            ],
        };

        let encoded = layout.encode();
        let decoded = BlobLayout::decode(&encoded).unwrap();

        assert_eq!(decoded.total_bytes, layout.total_bytes);
        assert_eq!(decoded.extents.len(), layout.extents.len());
        assert_eq!(decoded.extents[0].offset, 0);
        assert_eq!(decoded.extents[0].length, 256);
        assert_eq!(decoded.extents[0].extent_id, [1u8; 32]);
        assert_eq!(decoded.extents[1].offset, 512);
        assert_eq!(decoded.extents[1].length, 256);
        assert_eq!(decoded.extents[1].extent_id, [2u8; 32]);
    }

    #[test]
    fn test_empty_layout() {
        let layout = BlobLayout {
            total_bytes: 0,
            extents: vec![],
        };

        let encoded = layout.encode();
        let decoded = BlobLayout::decode(&encoded).unwrap();

        assert_eq!(decoded.total_bytes, 0);
        assert!(decoded.extents.is_empty());
    }

    #[test]
    fn test_regions_with_holes() {
        let layout = BlobLayout {
            total_bytes: 1024,
            extents: vec![
                BlobExtent {
                    offset: 100,
                    length: 100,
                    extent_id: [1u8; 32],
                },
                BlobExtent {
                    offset: 500,
                    length: 200,
                    extent_id: [2u8; 32],
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
                    extent_id: [1u8; 32],
                },
                BlobExtent {
                    offset: 256,
                    length: 256,
                    extent_id: [2u8; 32],
                },
            ],
        };

        let regions = layout.regions();

        assert_eq!(regions.len(), 2);
        assert!(matches!(&regions[0], BlobRegion::Data(_)));
        assert!(matches!(&regions[1], BlobRegion::Data(_)));
    }

    #[test]
    fn test_decode_invalid_version() {
        let mut data = vec![0x02]; // Invalid version
        data.push(EXTENT_ID_SIZE);
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());

        let result = BlobLayout::decode(&data);
        assert!(matches!(result, Err(BlobDecodeError::InvalidVersion(0x02))));
    }

    #[test]
    fn test_decode_truncated() {
        let data = vec![BLOB_VERSION, EXTENT_ID_SIZE]; // Missing rest of header
        let result = BlobLayout::decode(&data);
        assert!(matches!(result, Err(BlobDecodeError::Truncated)));
    }

    #[test]
    fn test_decode_overlapping_extents() {
        let layout = BlobLayout {
            total_bytes: 1024,
            extents: vec![BlobExtent {
                offset: 0,
                length: 256,
                extent_id: [1u8; 32],
            }],
        };

        let mut encoded = layout.encode().to_vec();

        // Manually corrupt: change extent count and add overlapping extent
        // Header: version(1) + id_size(1) + total_bytes(8) + extent_count(8) = 18
        // Overwrite extent_count to 2
        encoded[10..18].copy_from_slice(&2u64.to_le_bytes());

        // Add second extent that overlaps (offset=100, but first extent ends at 256)
        encoded.extend_from_slice(&100u64.to_le_bytes()); // offset
        encoded.extend_from_slice(&200u64.to_le_bytes()); // length
        encoded.extend_from_slice(&[2u8; 32]); // extent_id

        let result = BlobLayout::decode(&encoded);
        assert!(matches!(result, Err(BlobDecodeError::Overlapping)));
    }
}
