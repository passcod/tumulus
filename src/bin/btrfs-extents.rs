use std::{
    fs::File,
    io::{Error, Result},
    mem::take,
    os::{
        fd::{AsRawFd, RawFd},
        linux::fs::MetadataExt,
    },
    u32,
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
    BtrfsSearch::ensure_size();

    let path = std::env::args().nth(1).expect("USAGE: btrfs-extents PATH");
    let file = File::open(&path)?;

    let stat = file.metadata()?;
    let st_ino = stat.st_ino();

    let search_args = BtrfsSearch::default().only_extents().for_inode(st_ino);
    let items = search_args.exec_with_file_size(file.as_raw_fd(), stat.len() as _)?;
    let nr_items = items.search.nr_items;
    let items = items.collect::<std::result::Result<Vec<_>, _>>()?;
    dbg!(items.len(), nr_items);

    Ok(())
}

#[derive(Debug, Copy, Clone, DekuRead, DekuWrite)]
pub struct BtrfsSearch {
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
impl BtrfsSearch {
    const SIZE: usize = 104;
    const LEADING_OFFSET: usize = Self::SIZE + 8;
    const SENTINEL_SIZE: usize = 8; // u64

    fn ensure_size() {
        // runtime alternative to the DekuSize approach
        assert_eq!(
            Self::default().to_bytes().unwrap().len(),
            Self::SIZE,
            "BUG: search key length invalid"
        );
    }

    fn result_size(self) -> usize {
        // TODO: compute from self.min_kind through self.max_kind
        BtrfsSearchResultHeader::SIZE + BtrfsFileExtentItem::SIZE
    }

    fn minimum_buf_size(self) -> usize {
        Self::LEADING_OFFSET + self.result_size() + Self::SENTINEL_SIZE
    }

    fn exec_with_file_size(self, fd: RawFd, file_size: usize) -> Result<BtrfsSearchResults> {
        let buf_size = (file_size / (128 * 1024) * self.result_size())
            .max(3 * self.result_size())
            .min(1024_usize.pow(2));
        // there doesn't appear to be a real limit, but we pick
        // a 1MB maximum to avoid doing too large allocations.

        let box_size = Self::LEADING_OFFSET + buf_size + Self::SENTINEL_SIZE;
        debug_assert!(box_size >= self.minimum_buf_size());

        // vec![] allocates directly on the heap, avoiding stack overflows for large bufs
        let buf = vec![0u8; box_size].into_boxed_slice();
        self.exec_with_buf(fd, buf)
    }

    /// # Panics
    ///
    /// This method panics when given a buffer smaller than `self.minimum_buf_size()`. The
    /// `exec_with_file_size()` method automatically calculates the required buffer size,
    /// ensuring this panic is not possible, so it's recommended to use it instead of this.
    fn exec_with_buf(mut self, fd: RawFd, mut buf: Box<[u8]>) -> Result<BtrfsSearchResults> {
        let buf_len = buf.len();

        // SAFETY: we must always have enough buffer space for the search key, buf_size u64,
        // and at least one result header + item + sentinel. From experimentation, passing
        // shorter buffers doesn't result in UB (it errors cleanly), but better safe than sorry.
        assert!(
            buf_len >= self.minimum_buf_size(),
            "BUG: buffer passed to exec_with_buf is too short (wanted at least {}, got {})",
            self.minimum_buf_size(),
            buf_len,
        );

        // SAFETY: always zero the buffer before using it
        buf.fill(0);

        // SAFETY: we detect buffer overruns by writing a sentinel value at the back
        // and giving an 8-byte-smaller buf_size to the kernel, then checking the value
        // is still there after it's done with it.
        let sentinel = rand::random::<u64>().to_ne_bytes();
        debug_assert_eq!(sentinel.len(), Self::SENTINEL_SIZE);
        buf[(buf_len - Self::SENTINEL_SIZE)..].copy_from_slice(&sentinel[..]);

        // clear nr_items (set it to max) so we always grab
        // as many results as the kernel will give us
        self.nr_items = u32::MAX;
        self.to_slice(&mut buf)?;

        // SAFETY: buf_size passed to the kernel must always be <= the true available space in the box
        // where available space is what comes immediately after the buf_size u64 and until just before
        // the sentinel value
        let buf_size = (buf_len - Self::LEADING_OFFSET - Self::SENTINEL_SIZE) as u64;
        buf[BtrfsSearch::SIZE..Self::LEADING_OFFSET].copy_from_slice(&buf_size.to_ne_bytes()[..]);

        // SAFETY: the general lack of documentation for ioctls and this one in particular makes
        // validating this usage extremely annoying. Fortunately, the ioctl syscall is relatively
        // well-behaved: if you pass a bad pointer or undersized buffer, it will tell you so. The
        // kernel only uses this pointer for the duration of the syscall, and we zero the buffer
        // in this function prior to using it, ensuring it's always safe to pass any buffer, as
        // long as it's appropriately-sized, which is checked above.
        if unsafe { ioctl(fd, BTRFS_IOC_TREE_SEARCH_V2 as _, buf.as_mut_ptr()) } != 0 {
            return Err(Error::last_os_error());
        }

        // SAFETY: check the sentinel value before doing anything with the buffer
        assert_eq!(
            buf[(buf_len - Self::SENTINEL_SIZE)..],
            sentinel,
            "KERNEL BUG: overran our buffer"
        );

        let (_rest, search) = BtrfsSearch::from_bytes((&buf, 0))?;

        Ok(BtrfsSearchResults {
            buf,
            offset: Self::LEADING_OFFSET,
            search,
            next_search_offset: None,
            fd,
        })
    }

    fn only_extents(self) -> Self {
        Self {
            min_kind: BTRFS_EXTENT_DATA_KEY,
            max_kind: BTRFS_EXTENT_DATA_KEY,
            ..self
        }
    }

    fn for_inode(self, st_ino: u64) -> Self {
        Self {
            min_objectid: st_ino,
            max_objectid: st_ino,
            ..self
        }
    }

    fn with_offset(self, offset: u64) -> Self {
        Self {
            min_offset: offset,
            ..self
        }
    }
}

impl Default for BtrfsSearch {
    fn default() -> Self {
        BtrfsSearch {
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

#[derive(Debug, Clone, Copy, Default, PartialEq, DekuRead)]
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

#[derive(Debug, Clone, Copy, Default, PartialEq, DekuRead)]
#[deku(id_type = "u32", bytes = 4)]
pub enum BtrfsSearchResultKind {
    #[deku(id = 0)]
    #[default]
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
    search: BtrfsSearch,
    next_search_offset: Option<u64>,
    fd: RawFd,
}

impl Iterator for BtrfsSearchResults {
    type Item = std::result::Result<BtrfsSearchResult, DekuError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.search.nr_items == 0 {
            // the kernel says there's nothing more to see
            return None;
        }

        let buf = &self.buf[self.offset..];
        if buf.is_empty() {
            // should not happen (should be caught by other bits)
            // but let's handle it anyway to make sure
            return None;
        }

        // TODO: doing zero-copy interpretation would be nice for perf;
        // look into if there's something like deku for ergonomics tho
        match BtrfsSearchResult::from_bytes((&buf, 0)) {
            Err(err) => return Some(Err(err)),
            Ok((_rest, result)) => {
                // kind is never None in legitimate output, so we have to assume
                // we're reading unitialised space. don't interpret it as anything!
                if result.header.kind != BtrfsSearchResultKind::None {
                    self.offset += BtrfsSearchResultHeader::SIZE + result.item.len();
                    self.next_search_offset = Some(result.header.offset + 1);
                    return Some(Ok(result));
                }
            }
        }

        let Some(off) = self.next_search_offset else {
            // should not happen (should be caught by other bits)
            // but let's handle it anyway to make sure
            return None;
        };

        if dbg!(self.buf[self.offset..].len()) >= dbg!(self.search.result_size() * 2) {
            // if the buffer still has more than enough space in it for results
            // we don't need to do another read to know we're at the end!
            // note how this is checking for 2x while the minimum buf_size is 3x
            return None;
        }

        // we've arrived at the end of our buffer, but there's more data to be had!
        // iterate onwards but reuse the same buffer to avoid reallocating
        let buf = take(&mut self.buf);
        assert_ne!(buf.len(), 0, "BUG: the iterator buffer was take()n twice");
        match dbg!(self.search.with_offset(off)).exec_with_buf(self.fd, buf) {
            Err(err) => return Some(Err(err.into())),
            Ok(next) => {
                *self = next;

                // recursing in an iterator is not great, but this will be limited:
                // it will either return None or Some and should not itself recurse
                return self.next();
            }
        }
    }
}
