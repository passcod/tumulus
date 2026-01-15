use std::cmp::Ordering;

use deku::{no_std_io, prelude::*};
use linux_raw_sys::btrfs::{
    BTRFS_BALANCE_ITEM_KEY, BTRFS_BLOCK_GROUP_ITEM_KEY, BTRFS_CHUNK_ITEM_KEY, BTRFS_DEV_EXTENT_KEY,
    BTRFS_DEV_ITEM_KEY, BTRFS_DEV_REPLACE_KEY, BTRFS_DEV_STATS_KEY, BTRFS_DIR_INDEX_KEY,
    BTRFS_DIR_ITEM_KEY, BTRFS_DIR_LOG_INDEX_KEY, BTRFS_DIR_LOG_ITEM_KEY, BTRFS_EXTENT_CSUM_KEY,
    BTRFS_EXTENT_DATA_KEY, BTRFS_EXTENT_DATA_REF_KEY, BTRFS_EXTENT_ITEM_KEY,
    BTRFS_EXTENT_OWNER_REF_KEY, BTRFS_FREE_SPACE_BITMAP_KEY, BTRFS_FREE_SPACE_EXTENT_KEY,
    BTRFS_FREE_SPACE_INFO_KEY, BTRFS_INODE_EXTREF_KEY, BTRFS_INODE_ITEM_KEY, BTRFS_INODE_REF_KEY,
    BTRFS_METADATA_ITEM_KEY, BTRFS_ORPHAN_ITEM_KEY, BTRFS_PERSISTENT_ITEM_KEY,
    BTRFS_QGROUP_INFO_KEY, BTRFS_QGROUP_LIMIT_KEY, BTRFS_QGROUP_RELATION_KEY,
    BTRFS_QGROUP_STATUS_KEY, BTRFS_RAID_STRIPE_KEY, BTRFS_ROOT_BACKREF_KEY, BTRFS_ROOT_ITEM_KEY,
    BTRFS_ROOT_REF_KEY, BTRFS_SHARED_BLOCK_REF_KEY, BTRFS_SHARED_DATA_REF_KEY,
    BTRFS_STRING_ITEM_KEY, BTRFS_TEMPORARY_ITEM_KEY, BTRFS_TREE_BLOCK_REF_KEY,
    BTRFS_UUID_KEY_RECEIVED_SUBVOL, BTRFS_UUID_KEY_SUBVOL, BTRFS_VERITY_DESC_ITEM_KEY,
    BTRFS_VERITY_MERKLE_ITEM_KEY, BTRFS_XATTR_ITEM_KEY,
};

mod file_extent;

pub use file_extent::*;

#[derive(Debug, Clone, Copy, Default, PartialEq, DekuRead)]
pub struct BtrfsSearchResultHeader {
    pub transid: u64,
    pub objectid: u64,
    pub offset: u64,
    pub kind: BtrfsSearchKind,
    pub len: u32,
}
impl BtrfsSearchResultHeader {
    pub(crate) const SIZE: usize = 32;
}

pub(crate) trait SizedItem {
    const SIZE: usize;

    fn actual_len(&self) -> Option<usize> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct NotImplemented;

impl<Ctx> DekuReader<'_, Ctx> for NotImplemented {
    fn from_reader_with_ctx<R: no_std_io::Read + no_std_io::Seek>(
        _reader: &mut Reader<R>,
        _ctx: Ctx,
    ) -> Result<Self, DekuError>
    where
        Self: Sized,
    {
        todo!()
    }
}
impl SizedItem for NotImplemented {
    const SIZE: usize = 0;
}

const fn const_equal(lhs: &[u8], rhs: &[u8]) -> bool {
    if lhs.len() != rhs.len() {
        return false;
    }
    let mut i = 0;
    while i < lhs.len() {
        if lhs[i] != rhs[i] {
            return false;
        }
        i += 1;
    }
    true
}

macro_rules! kinds {
    (
        $( ($keyvarstr:literal / $keyvar:ident)($keyconststr:literal / $keyconst:path) => $itemvar:ident($item:path) ),* $(,)?
    ) => {
        // check that the stringification of the variants/constants are correct
        $(const _: () = assert!(
            const_equal(stringify!($keyconst).as_bytes(), $keyconststr.as_bytes()),
            concat!(
                "constant ",
                stringify!($keyconst),
                " must be identical to string ",
                $keyconststr,
            )
        );)*
        $(const _: () = assert!(
            const_equal(concat!("BtrfsSearchKind::", stringify!($keyvar)).as_bytes(), $keyvarstr.as_bytes()),
            concat!(
                "variant ",
                concat!("BtrfsSearchKind::", stringify!($keyvar)),
                " must be identical to string ",
                $keyvarstr,
            )
        );)*

        #[derive(Debug, Clone, Copy, Default, Eq, PartialEq, DekuRead)]
        #[deku(id_type = "u32", bytes = 4)]
        pub enum BtrfsSearchKind {
            #[deku(id = 0)]
            #[default]
            None,
            $(
                #[deku(id = $keyconststr)]
                $keyvar,
            )*
            #[deku(id_pat = "_")]
            Other { id: u32 },
        }

        impl BtrfsSearchKind {
            pub const fn as_key(self) -> u32 {
                match self {
                    Self::None => 0,
                    $(Self::$keyvar => $keyconst,)*
                    Self::Other { id } => id,
                }
            }

            pub const fn from_key(key: u32) -> Self {
                match key {
                    0 => Self::None,
                    $(n if n == $keyconst => Self::$keyvar,)*
                    id => Self::Other { id },
                }
            }

            pub const MIN_KEY: u32 = 1;
            pub const MAX_KEY: u32 = Self::max_key();
            const fn max_key() -> u32 {
                let mut max = Self::MIN_KEY;
                $(if $keyconst > max { max = $keyconst; })*
                max
            }

            pub const fn item_size(self) -> usize {
                match self {
                    Self::None | Self::Other { .. } => 0,
                    $(
                        Self::$keyvar => <$item as SizedItem>::SIZE,
                    )*
                }
            }
        }

        #[derive(Debug, Clone, PartialEq, DekuRead)]
        #[deku(
            ctx = "content_id: BtrfsSearchKind, content_size: u32",
            id = "content_id"
        )]
        pub enum BtrfsSearchResultItem {
            $(
                #[allow(private_interfaces, reason = "NotImplemented is more private")]
                #[deku(id = $keyvarstr)]
                $itemvar($item),
            )*
            #[deku(id_pat = "_")]
            Other(#[deku(bytes_read = "content_size")] Vec<u8>),
        }
        impl BtrfsSearchResultItem {
            pub(crate) fn len(&self) -> usize {
                match self {
                    $(
                        Self::$itemvar(item) => item.actual_len().unwrap_or(<$item as SizedItem>::SIZE),
                    )*
                    Self::Other(data) => data.len(),
                }
            }
        }
    };
}

impl Ord for BtrfsSearchKind {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_key().cmp(&other.as_key())
    }
}

impl PartialOrd for BtrfsSearchKind {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

kinds! {
    ("BtrfsSearchKind::InodeItem" / InodeItem)("BTRFS_INODE_ITEM_KEY" / BTRFS_INODE_ITEM_KEY) => _InodeItem(NotImplemented),
    ("BtrfsSearchKind::InodeRef" / InodeRef)("BTRFS_INODE_REF_KEY" / BTRFS_INODE_REF_KEY) => _InodeRef(NotImplemented),
    ("BtrfsSearchKind::InodeExtRef" / InodeExtRef)("BTRFS_INODE_EXTREF_KEY" / BTRFS_INODE_EXTREF_KEY) => _InodeExtRef(NotImplemented),
    ("BtrfsSearchKind::Xattr" / Xattr)("BTRFS_XATTR_ITEM_KEY" / BTRFS_XATTR_ITEM_KEY) => _Xattr(NotImplemented),
    ("BtrfsSearchKind::VerityDesc" / VerityDesc)("BTRFS_VERITY_DESC_ITEM_KEY" / BTRFS_VERITY_DESC_ITEM_KEY) => _VerityDesc(NotImplemented),
    ("BtrfsSearchKind::VerityMerkle" / VerityMerkle)("BTRFS_VERITY_MERKLE_ITEM_KEY" / BTRFS_VERITY_MERKLE_ITEM_KEY) => _VerityMerkle(NotImplemented),
    ("BtrfsSearchKind::Orphan" / Orphan)("BTRFS_ORPHAN_ITEM_KEY" / BTRFS_ORPHAN_ITEM_KEY) => _Orphan(NotImplemented),
    ("BtrfsSearchKind::DirLog" / DirLog)("BTRFS_DIR_LOG_ITEM_KEY" / BTRFS_DIR_LOG_ITEM_KEY) => _DirLog(NotImplemented),
    ("BtrfsSearchKind::DirLogIndex" / DirLogIndex)("BTRFS_DIR_LOG_INDEX_KEY" / BTRFS_DIR_LOG_INDEX_KEY) => _DirLogIndex(NotImplemented),
    ("BtrfsSearchKind::Dir" / Dir)("BTRFS_DIR_ITEM_KEY" / BTRFS_DIR_ITEM_KEY) => _Dir(NotImplemented),
    ("BtrfsSearchKind::DirIndex" / DirIndex)("BTRFS_DIR_INDEX_KEY" / BTRFS_DIR_INDEX_KEY) => _DirIndex(NotImplemented),
    ("BtrfsSearchKind::ExtentData" / ExtentData)("BTRFS_EXTENT_DATA_KEY" / BTRFS_EXTENT_DATA_KEY) => FileExtent(BtrfsFileExtentItem),
    ("BtrfsSearchKind::ExtentCsum" / ExtentCsum)("BTRFS_EXTENT_CSUM_KEY" / BTRFS_EXTENT_CSUM_KEY) => _ExtentCsum(NotImplemented),
    ("BtrfsSearchKind::Root" / Root)("BTRFS_ROOT_ITEM_KEY" / BTRFS_ROOT_ITEM_KEY) => _Root(NotImplemented),
    ("BtrfsSearchKind::RootBackref" / RootBackref)("BTRFS_ROOT_BACKREF_KEY" / BTRFS_ROOT_BACKREF_KEY) => _RootBackref(NotImplemented),
    ("BtrfsSearchKind::RootRef" / RootRef)("BTRFS_ROOT_REF_KEY" / BTRFS_ROOT_REF_KEY) => _RootRef(NotImplemented),
    ("BtrfsSearchKind::Extent" / Extent)("BTRFS_EXTENT_ITEM_KEY" / BTRFS_EXTENT_ITEM_KEY) => _Extent(NotImplemented),
    ("BtrfsSearchKind::Metadata" / Metadata)("BTRFS_METADATA_ITEM_KEY" / BTRFS_METADATA_ITEM_KEY) => _Metadata(NotImplemented),
    ("BtrfsSearchKind::ExtentOwnerRef" / ExtentOwnerRef)("BTRFS_EXTENT_OWNER_REF_KEY" / BTRFS_EXTENT_OWNER_REF_KEY) => _ExtentOwnerRef(NotImplemented),
    ("BtrfsSearchKind::TreeBlockRef" / TreeBlockRef)("BTRFS_TREE_BLOCK_REF_KEY" / BTRFS_TREE_BLOCK_REF_KEY) => _TreeBlockRef(NotImplemented),
    ("BtrfsSearchKind::ExtentDataRef" / ExtentDataRef)("BTRFS_EXTENT_DATA_REF_KEY" / BTRFS_EXTENT_DATA_REF_KEY) => _ExtentDataRef(NotImplemented),
    ("BtrfsSearchKind::SharedBlockRef" / SharedBlockRef)("BTRFS_SHARED_BLOCK_REF_KEY" / BTRFS_SHARED_BLOCK_REF_KEY) => _SharedBlockRef(NotImplemented),
    ("BtrfsSearchKind::SharedDataRef" / SharedDataRef)("BTRFS_SHARED_DATA_REF_KEY" / BTRFS_SHARED_DATA_REF_KEY) => _SharedDataRef(NotImplemented),
    ("BtrfsSearchKind::BlockGroupItem" / BlockGroupItem)("BTRFS_BLOCK_GROUP_ITEM_KEY" / BTRFS_BLOCK_GROUP_ITEM_KEY) => _BlockGroupItem(NotImplemented),
    ("BtrfsSearchKind::FreeSpaceInfo" / FreeSpaceInfo)("BTRFS_FREE_SPACE_INFO_KEY" / BTRFS_FREE_SPACE_INFO_KEY) => _FreeSpaceInfo(NotImplemented),
    ("BtrfsSearchKind::FreeSpaceExtent" / FreeSpaceExtent)("BTRFS_FREE_SPACE_EXTENT_KEY" / BTRFS_FREE_SPACE_EXTENT_KEY) => _FreeSpaceExtent(NotImplemented),
    ("BtrfsSearchKind::FreeSpaceBitmap" / FreeSpaceBitmap)("BTRFS_FREE_SPACE_BITMAP_KEY" / BTRFS_FREE_SPACE_BITMAP_KEY) => _FreeSpaceBitmap(NotImplemented),
    ("BtrfsSearchKind::DevExtent" / DevExtent)("BTRFS_DEV_EXTENT_KEY" / BTRFS_DEV_EXTENT_KEY) => _DevExtent(NotImplemented),
    ("BtrfsSearchKind::Dev" / Dev)("BTRFS_DEV_ITEM_KEY" / BTRFS_DEV_ITEM_KEY) => _Dev(NotImplemented),
    ("BtrfsSearchKind::Chunk" / Chunk)("BTRFS_CHUNK_ITEM_KEY" / BTRFS_CHUNK_ITEM_KEY) => _Chunk(NotImplemented),
    ("BtrfsSearchKind::RaidStripe" / RaidStripe)("BTRFS_RAID_STRIPE_KEY" / BTRFS_RAID_STRIPE_KEY) => _RaidStripe(NotImplemented),
    ("BtrfsSearchKind::QgroupStatus" / QgroupStatus)("BTRFS_QGROUP_STATUS_KEY" / BTRFS_QGROUP_STATUS_KEY) => _QgroupStatus(NotImplemented),
    ("BtrfsSearchKind::QgroupInfo" / QgroupInfo)("BTRFS_QGROUP_INFO_KEY" / BTRFS_QGROUP_INFO_KEY) => _QgroupInfo(NotImplemented),
    ("BtrfsSearchKind::QgroupLimit" / QgroupLimit)("BTRFS_QGROUP_LIMIT_KEY" / BTRFS_QGROUP_LIMIT_KEY) => _QgroupLimit(NotImplemented),
    ("BtrfsSearchKind::QgroupRelation" / QgroupRelation)("BTRFS_QGROUP_RELATION_KEY" / BTRFS_QGROUP_RELATION_KEY) => _QgroupRelation(NotImplemented),
    ("BtrfsSearchKind::Balance" / Balance)("BTRFS_BALANCE_ITEM_KEY" / BTRFS_BALANCE_ITEM_KEY) => _Balance(NotImplemented),
    ("BtrfsSearchKind::Temporary" / Temporary)("BTRFS_TEMPORARY_ITEM_KEY" / BTRFS_TEMPORARY_ITEM_KEY) => _Temporary(NotImplemented),
    ("BtrfsSearchKind::DevStats" / DevStats)("BTRFS_DEV_STATS_KEY" / BTRFS_DEV_STATS_KEY) => _DevStats(NotImplemented),
    ("BtrfsSearchKind::PersistentItem" / PersistentItem)("BTRFS_PERSISTENT_ITEM_KEY" / BTRFS_PERSISTENT_ITEM_KEY) => _PersistentItem(NotImplemented),
    ("BtrfsSearchKind::DevReplace" / DevReplace)("BTRFS_DEV_REPLACE_KEY" / BTRFS_DEV_REPLACE_KEY) => _DevReplace(NotImplemented),
    ("BtrfsSearchKind::UuidKeySubvol" / UuidKeySubvol)("BTRFS_UUID_KEY_SUBVOL" / BTRFS_UUID_KEY_SUBVOL) => _UuidKeySubvol(NotImplemented),
    ("BtrfsSearchKind::UuidKeyReceivedSubvol" / UuidKeyReceivedSubvol)("BTRFS_UUID_KEY_RECEIVED_SUBVOL" / BTRFS_UUID_KEY_RECEIVED_SUBVOL) => _UuidKeyReceivedSubvol(NotImplemented),
    ("BtrfsSearchKind::String" / String)("BTRFS_STRING_ITEM_KEY" / BTRFS_STRING_ITEM_KEY) => _String(NotImplemented),
}
