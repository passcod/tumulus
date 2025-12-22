use std::{
    fs::File,
    io::{Error, Result},
    os::{
        fd::{AsRawFd, RawFd},
        linux::fs::MetadataExt,
    },
};

use deku::prelude::*;
use libc::ioctl;
use linux_raw_sys::{
    btrfs::{
        BTRFS_BALANCE_ITEM_KEY, BTRFS_BLOCK_GROUP_ITEM_KEY, BTRFS_CHUNK_ITEM_KEY,
        BTRFS_DEV_EXTENT_KEY, BTRFS_DEV_ITEM_KEY, BTRFS_DEV_REPLACE_KEY, BTRFS_DEV_STATS_KEY,
        BTRFS_DIR_INDEX_KEY, BTRFS_DIR_ITEM_KEY, BTRFS_DIR_LOG_INDEX_KEY, BTRFS_DIR_LOG_ITEM_KEY,
        BTRFS_EXTENT_CSUM_KEY, BTRFS_EXTENT_DATA_KEY, BTRFS_EXTENT_DATA_REF_KEY,
        BTRFS_EXTENT_ITEM_KEY, BTRFS_EXTENT_OWNER_REF_KEY, BTRFS_FREE_SPACE_BITMAP_KEY,
        BTRFS_FREE_SPACE_EXTENT_KEY, BTRFS_FREE_SPACE_INFO_KEY, BTRFS_INODE_EXTREF_KEY,
        BTRFS_INODE_ITEM_KEY, BTRFS_INODE_REF_KEY, BTRFS_METADATA_ITEM_KEY, BTRFS_ORPHAN_ITEM_KEY,
        BTRFS_PERSISTENT_ITEM_KEY, BTRFS_QGROUP_INFO_KEY, BTRFS_QGROUP_LIMIT_KEY,
        BTRFS_QGROUP_RELATION_KEY, BTRFS_QGROUP_STATUS_KEY, BTRFS_RAID_STRIPE_KEY,
        BTRFS_ROOT_BACKREF_KEY, BTRFS_ROOT_ITEM_KEY, BTRFS_ROOT_REF_KEY,
        BTRFS_SHARED_BLOCK_REF_KEY, BTRFS_SHARED_DATA_REF_KEY, BTRFS_STRING_ITEM_KEY,
        BTRFS_TEMPORARY_ITEM_KEY, BTRFS_TREE_BLOCK_REF_KEY, BTRFS_UUID_KEY_RECEIVED_SUBVOL,
        BTRFS_UUID_KEY_SUBVOL, BTRFS_VERITY_DESC_ITEM_KEY, BTRFS_VERITY_MERKLE_ITEM_KEY,
        BTRFS_XATTR_ITEM_KEY,
    },
    ioctl::BTRFS_IOC_TREE_SEARCH_V2,
};

fn main() -> Result<()> {
    SearchKey::ensure_size();

    let path = std::env::args().nth(1).expect("USAGE: btrfs-extents PATH");
    let file = File::open(&path)?;

    let stat = file.metadata()?;
    let st_ino = stat.st_ino();

    let search_args = BtrfsSearch::for_inode(st_ino);
    let items = search_args.exec(file.as_raw_fd(), 1000)?;
    let items = items.collect::<std::result::Result<Vec<_>, _>>()?;
    dbg!(&items, items.len(), search_args.key.nr_items);

    Ok(())
}

#[repr(C)]
#[derive(Debug)]
struct BtrfsSearch {
    key: SearchKey,
}

#[derive(Debug, Copy, Clone, DekuWrite)]
pub struct SearchKey {
    pub tree_id: u64,
    pub min_objectid: u64,
    pub max_objectid: u64,
    pub min_offset: u64,
    pub max_offset: u64,
    pub min_transid: u64,
    pub max_transid: u64,
    pub min_kind: u32,
    pub max_kind: u32,
    pub nr_items: u32,

    #[deku(pad_bytes_after = "36")]
    reserved: (),
}
// This doesn't work because DekuSize doesn't work
// https://github.com/sharksforarms/deku/issues/635
// ensure the size is correct
// const _: [(); SearchKey::SIZE - SearchKey::SIZE_BYTES.unwrap()] = [];
// const _: [(); SearchKey::SIZE_BYTES.unwrap() - SearchKey::SIZE] = [];
impl SearchKey {
    const SIZE: usize = 104;

    fn ensure_size() {
        // runtime alternative to the DekuSize approach
        assert_eq!(
            Self::default().to_bytes().unwrap().len(),
            Self::SIZE,
            "BUG: search key length invalid"
        );
    }
}

impl Default for SearchKey {
    fn default() -> Self {
        SearchKey {
            tree_id: 0,
            min_objectid: 0,
            max_objectid: u64::MAX,
            min_offset: 0,
            max_offset: u64::MAX,
            min_transid: 0,
            max_transid: u64::MAX,
            min_kind: 0,
            max_kind: u32::MAX,
            nr_items: u32::MAX,

            reserved: (),
        }
    }
}

impl BtrfsSearch {
    fn exec(&self, fd: RawFd, buf_size: u64) -> Result<BtrfsSearchResults> {
        assert!(
            buf_size < 16 * 1024_u64.pow(3),
            "buf_size cannot be larger than 16MiB (kernel limit)"
        );

        let offset = SearchKey::SIZE + 8;

        let mut buf = vec![0u8; offset + buf_size as usize];
        self.key.to_slice(&mut buf)?;
        buf[SearchKey::SIZE..offset].copy_from_slice(&buf_size.to_ne_bytes()[..]);
        let mut buf = buf.into_boxed_slice();

        if unsafe { ioctl(fd, BTRFS_IOC_TREE_SEARCH_V2 as _, buf.as_mut_ptr()) } != 0 {
            return Err(Error::last_os_error());
        }

        Ok(BtrfsSearchResults { buf, offset })
    }

    fn new(key: SearchKey) -> Self {
        Self { key }
    }

    fn for_inode(st_ino: u64) -> Self {
        Self::new(SearchKey {
            min_objectid: st_ino,
            max_objectid: st_ino,
            min_kind: BTRFS_EXTENT_DATA_KEY,
            max_kind: BTRFS_EXTENT_DATA_KEY,
            ..Default::default()
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, DekuRead)]
pub struct BtrfsSearchResultHeader {
    pub transid: u64,
    pub objectid: u64,
    pub offset: u64,
    pub kind: BtrfsSearchResultKind,
    pub len: u32,
}
impl BtrfsSearchResultHeader {
    const SIZE: usize = 32;
}

#[derive(Debug, Clone, Copy, PartialEq, DekuRead)]
#[deku(id_type = "u32", bytes = 4)]
pub enum BtrfsSearchResultKind {
    #[deku(id = 0)]
    None,
    #[deku(id = "BTRFS_INODE_ITEM_KEY")]
    InodeItem,
    #[deku(id = "BTRFS_INODE_REF_KEY")]
    InodeRef,
    #[deku(id = "BTRFS_INODE_EXTREF_KEY")]
    InodeExtRef,
    #[deku(id = "BTRFS_XATTR_ITEM_KEY")]
    Xattr,
    #[deku(id = "BTRFS_VERITY_DESC_ITEM_KEY")]
    VerityDesc,
    #[deku(id = "BTRFS_VERITY_MERKLE_ITEM_KEY")]
    VerityMerkle,
    #[deku(id = "BTRFS_ORPHAN_ITEM_KEY")]
    Orphan,
    #[deku(id = "BTRFS_DIR_LOG_ITEM_KEY")]
    DirLog,
    #[deku(id = "BTRFS_DIR_LOG_INDEX_KEY")]
    DirLogIndex,
    #[deku(id = "BTRFS_DIR_ITEM_KEY")]
    Dir,
    #[deku(id = "BTRFS_DIR_INDEX_KEY")]
    DirIndex,
    #[deku(id = "BTRFS_EXTENT_DATA_KEY")]
    ExtentData,
    #[deku(id = "BTRFS_EXTENT_CSUM_KEY")]
    ExtentCsum,
    #[deku(id = "BTRFS_ROOT_ITEM_KEY")]
    Root,
    #[deku(id = "BTRFS_ROOT_BACKREF_KEY")]
    RootBackref,
    #[deku(id = "BTRFS_ROOT_REF_KEY")]
    RootRef,
    #[deku(id = "BTRFS_EXTENT_ITEM_KEY")]
    Extent,
    #[deku(id = "BTRFS_METADATA_ITEM_KEY")]
    Metadata,
    #[deku(id = "BTRFS_EXTENT_OWNER_REF_KEY")]
    ExtentOwnerRef,
    #[deku(id = "BTRFS_TREE_BLOCK_REF_KEY")]
    TreeBlockRef,
    #[deku(id = "BTRFS_EXTENT_DATA_REF_KEY")]
    ExtentDataRef,
    #[deku(id = "BTRFS_SHARED_BLOCK_REF_KEY")]
    SharedBlockRef,
    #[deku(id = "BTRFS_SHARED_DATA_REF_KEY")]
    SharedDataRef,
    #[deku(id = "BTRFS_BLOCK_GROUP_ITEM_KEY")]
    BlockGroupItem,
    #[deku(id = "BTRFS_FREE_SPACE_INFO_KEY")]
    FreeSpaceInfo,
    #[deku(id = "BTRFS_FREE_SPACE_EXTENT_KEY")]
    FreeSpaceExtent,
    #[deku(id = "BTRFS_FREE_SPACE_BITMAP_KEY")]
    FreeSpaceBitmap,
    #[deku(id = "BTRFS_DEV_EXTENT_KEY")]
    DevExtent,
    #[deku(id = "BTRFS_DEV_ITEM_KEY")]
    Dev,
    #[deku(id = "BTRFS_CHUNK_ITEM_KEY")]
    Chunk,
    #[deku(id = "BTRFS_RAID_STRIPE_KEY")]
    RaidStripe,
    #[deku(id = "BTRFS_QGROUP_STATUS_KEY")]
    QgroupStatus,
    #[deku(id = "BTRFS_QGROUP_INFO_KEY")]
    QgroupInfo,
    #[deku(id = "BTRFS_QGROUP_LIMIT_KEY")]
    QgroupLimit,
    #[deku(id = "BTRFS_QGROUP_RELATION_KEY")]
    QgroupRelation,
    #[deku(id = "BTRFS_BALANCE_ITEM_KEY")]
    Balance,
    #[deku(id = "BTRFS_TEMPORARY_ITEM_KEY")]
    Temporary,
    #[deku(id = "BTRFS_DEV_STATS_KEY")]
    DevStats,
    #[deku(id = "BTRFS_PERSISTENT_ITEM_KEY")]
    PersistentItem,
    #[deku(id = "BTRFS_DEV_REPLACE_KEY")]
    DevReplace,
    #[deku(id = "BTRFS_UUID_KEY_SUBVOL")]
    UuidKeySubvol,
    #[deku(id = "BTRFS_UUID_KEY_RECEIVED_SUBVOL")]
    UuidKeyReceivedSubvol,
    #[deku(id = "BTRFS_STRING_ITEM_KEY")]
    String,
    #[deku(id_pat = "_")]
    Other { id: u32 },
}

#[derive(Debug, Clone, PartialEq, DekuRead)]
#[deku(
    ctx = "content_id: BtrfsSearchResultKind, content_size: u32",
    id = "content_id"
)]
pub enum BtrfsSearchResultItem {
    #[deku(id = "BtrfsSearchResultKind::ExtentData")]
    FileExtent(BtrfsFileExtentItem),
    #[deku(id_pat = "_")]
    Other(#[deku(bytes_read = "content_size")] Vec<u8>),
}

impl BtrfsSearchResultItem {
    fn len(&self) -> usize {
        match self {
            Self::FileExtent(_) => BtrfsFileExtentItem::SIZE,
            Self::Other(data) => data.len(),
        }
    }
}

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
impl BtrfsFileExtentItem {
    const SIZE: usize = 53;
}

#[derive(Debug, Clone, PartialEq, DekuRead)]
pub struct BtrfsSearchResult {
    pub header: BtrfsSearchResultHeader,
    #[deku(ctx = "header.kind, header.len")]
    pub item: BtrfsSearchResultItem,
}

#[derive(Debug, Clone)]
struct BtrfsSearchResults {
    buf: Box<[u8]>,
    offset: usize,
}

impl Iterator for BtrfsSearchResults {
    type Item = std::result::Result<BtrfsSearchResult, DekuError>;

    fn next(&mut self) -> Option<Self::Item> {
        let buf = &self.buf[self.offset..];
        if buf.is_empty() {
            return None;
        }

        match BtrfsSearchResult::from_bytes((&buf, 0)) {
            Ok((_rest, result)) => {
                if result.header.kind == BtrfsSearchResultKind::None {
                    // we're done, all following parses will return zero
                    self.offset = self.buf.len();
                    return None;
                }

                self.offset += BtrfsSearchResultHeader::SIZE + result.item.len();
                Some(Ok(result))
            }
            Err(err) => {
                if *buf == [0] {
                    // list ends with a null byte?
                    return None;
                }

                Some(Err(err))
            }
        }
    }
}
