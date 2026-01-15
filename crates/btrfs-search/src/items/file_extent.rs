use deku::{ctx::ReadExact, no_std_io, prelude::*};

#[derive(Debug, Clone, PartialEq, DekuRead)]
pub struct BtrfsFileExtentItem {
    pub header: BtrfsFileExtentItemHeader,
    #[deku(ctx = "header.kind, header.ram_bytes")]
    pub body: BtrfsFileExtentItemBody,
}

impl super::SizedItem for BtrfsFileExtentItem {
    // when body is on disk, this is 53
    // but when the content of the file is inline, it's up to 2048 by default
    // and that can be changed at runtime! with the max_inline mount option
    // so we set it to 4096 (the full size of a block) to be extra safe
    const SIZE: usize = 4096;
}

#[derive(Debug, Clone, PartialEq, DekuRead)]
pub struct BtrfsFileExtentItemHeader {
    #[deku(endian = "little")]
    pub generation: u64,
    #[deku(endian = "little")]
    pub ram_bytes: u64,
    pub compression: u8,
    pub encryption: u8,
    #[deku(endian = "little")]
    pub other_encoding: u16,
    pub kind: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BtrfsFileExtentItemBody {
    Inline(Vec<u8>),
    OnDisk(BtrfsFileExtentItemOnDisk),
}

impl<'a> DekuReader<'a, (u8, u64)> for BtrfsFileExtentItemBody {
    fn from_reader_with_ctx<R: no_std_io::Read + no_std_io::Seek>(
        reader: &mut Reader<R>,
        (kind, ram_bytes): (u8, u64),
    ) -> Result<Self, DekuError>
    where
        Self: Sized,
    {
        match kind {
            0 => DekuReader::from_reader_with_ctx(reader, ReadExact(ram_bytes as _))
                .map(Self::Inline),
            _ => DekuReader::from_reader_with_ctx(reader, ()).map(Self::OnDisk),
        }
    }
}

#[derive(Debug, Clone, PartialEq, DekuRead)]
pub struct BtrfsFileExtentItemOnDisk {
    #[deku(endian = "little")]
    pub disk_bytenr: u64,
    #[deku(endian = "little")]
    pub disk_num_bytes: u64,
    #[deku(endian = "little")]
    pub offset: u64,
    #[deku(endian = "little")]
    pub num_bytes: u64,
}
