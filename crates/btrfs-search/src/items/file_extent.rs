use deku::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, DekuRead)]
pub struct BtrfsFileExtentItem {
    #[deku(endian = "little")]
    pub generation: u64,
    #[deku(endian = "little")]
    pub ram_bytes: u64,
    pub compression: u8,
    pub encryption: u8,
    #[deku(endian = "little")]
    pub other_encoding: u16,
    pub kind: u8,
    #[deku(endian = "little")]
    pub disk_bytenr: u64,
    #[deku(endian = "little")]
    pub disk_num_bytes: u64,
    #[deku(endian = "little")]
    pub offset: u64,
    #[deku(endian = "little")]
    pub num_bytes: u64,
}

impl super::SizedItem for BtrfsFileExtentItem {
    const SIZE: usize = 53;
}
