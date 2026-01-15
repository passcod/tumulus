use deku::{ctx::ReadExact, no_std_io, prelude::*};

/// A BTRFS file extent item.
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

/// Common metadata for file extent items.
#[derive(Debug, Clone, PartialEq, DekuRead)]
pub struct BtrfsFileExtentItemHeader {
    /// Transaction ID that created this extent.
    #[deku(endian = "little")]
    pub generation: u64,

    /// Max number of bytes to hold this extent in RAM.
    ///
    /// When we split a compressed extent we can't know how big each of the resulting pieces will
    /// be. So, this is an upper limit on the size of the extent in RAM instead of an exact limit.
    #[deku(endian = "little")]
    pub ram_bytes: u64,

    /// Type of compression
    pub compression: BtrfsCompression,

    /// Type of encryption (currently not used)
    pub encryption: BtrfsEncryption,

    /// More bits that may be used later for expressing data encoding
    #[deku(pad_bytes_after = "2")]
    _other_encoding: (),

    /// Are we inline data or a real (on-disk) extent?
    pub kind: BtrfsExtentKind,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, DekuRead)]
#[deku(id_type = "u8", bytes = 1)]
pub enum BtrfsCompression {
    #[deku(id = 0)]
    #[default]
    None,
    #[deku(id = 1)]
    Zlib,
    #[deku(id = 2)]
    Lzo,
    #[deku(id = 3)]
    Zstd,
    #[deku(id_pat = "_")]
    Other { id: u8 },
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, DekuRead)]
#[deku(id_type = "u8", bytes = 1)]
pub enum BtrfsEncryption {
    #[deku(id = 0)]
    #[default]
    None,
    #[deku(id_pat = "_")]
    Other { id: u8 },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, DekuRead)]
#[deku(id_type = "u8", bytes = 1)]
pub enum BtrfsExtentKind {
    #[deku(id = 0)]
    Inline,
    #[deku(id = 1)]
    OnDisk,
    #[deku(id_pat = "_")]
    Other { id: u8 },
}

/// Either an inline file body, or its extent-on-disk metadata.
///
/// Note the inline buf may be a little longer than the actual file.
#[derive(Debug, Clone, PartialEq)]
pub enum BtrfsFileExtentItemBody {
    Inline(Vec<u8>),
    OnDisk(BtrfsFileExtentItemOnDisk),
}

impl<'a> DekuReader<'a, (BtrfsExtentKind, u64)> for BtrfsFileExtentItemBody {
    fn from_reader_with_ctx<R: no_std_io::Read + no_std_io::Seek>(
        reader: &mut Reader<R>,
        (kind, ram_bytes): (BtrfsExtentKind, u64),
    ) -> Result<Self, DekuError>
    where
        Self: Sized,
    {
        match kind {
            BtrfsExtentKind::Inline => {
                // the `as _` doesn't matter because this will always be a small number (<4kB)
                DekuReader::from_reader_with_ctx(reader, ReadExact(ram_bytes as _))
                    .map(Self::Inline)
            }
            BtrfsExtentKind::OnDisk => {
                DekuReader::from_reader_with_ctx(reader, ()).map(Self::OnDisk)
            }
            BtrfsExtentKind::Other { id } => {
                todo!("unknown extent type {id}, this program cannot safely interpret BTRFS data")
            }
        }
    }
}

/// The extent-on-disk metadata.
#[derive(Debug, Clone, PartialEq, DekuRead)]
pub struct BtrfsFileExtentItemOnDisk {
    /// Where the data starts on disk. This is relative to... somewhere, idk.
    ///
    /// "At this offset in the structure, the inline extent data start."
    #[deku(endian = "little")]
    pub disk_offset: u64,

    /// Disk space consumed by the extent.
    ///
    /// Checksum blocks are included in these numbers.
    #[deku(endian = "little")]
    pub disk_bytes: u64,

    /// The logical offset in file blocks (no checksums) this extent record is for.
    ///
    /// This allows a file extent to point into the middle of an existing extent on disk, sharing
    /// it between two snapshots (useful if some bytes in the middle of the extent have changed).
    #[deku(endian = "little")]
    pub logical_offset: u64,

    /// The logical number of file blocks (no checksums included).
    ///
    /// This always reflects the size uncompressed and without encoding.
    #[deku(endian = "little")]
    pub logical_bytes: u64,
}
