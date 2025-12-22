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
    btrfs::{__kernel_ulong_t, BTRFS_EXTENT_DATA_KEY, btrfs_ioctl_search_key},
    ioctl::BTRFS_IOC_TREE_SEARCH_V2,
};

const SEARCH_BUF_SIZE: u64 = 65536;

fn main() -> Result<()> {
    let path = std::env::args().nth(1).expect("USAGE: btrfs-extents PATH");
    let file = File::open(&path)?;

    let stat = file.metadata()?;
    let st_ino = stat.st_ino();

    let mut search_args = BtrfsSearch::for_inode(st_ino);
    search_args.exec(file.as_raw_fd())?;

    for result in search_args {
        dbg!(result.header, result.data.len());
    }

    Ok(())
}

#[repr(C)]
#[derive(Debug)]
struct BtrfsSearch {
    key: btrfs_ioctl_search_key,
    offset: u64, // used as buf_size by the ioctl, and then read offset by us
    buf: [u8; SEARCH_BUF_SIZE as _],
}

impl BtrfsSearch {
    fn exec(&mut self, fd: RawFd) -> Result<()> {
        let ptr = self as *mut _ as *mut u8;
        if unsafe { ioctl(fd, BTRFS_IOC_TREE_SEARCH_V2 as _, ptr) } != 0 {
            return Err(Error::last_os_error());
        }
        self.offset = 0;
        Ok(())
    }

    fn new(key: btrfs_ioctl_search_key) -> Self {
        Self {
            key,
            offset: SEARCH_BUF_SIZE as _,
            buf: [0; SEARCH_BUF_SIZE as _],
        }
    }

    fn for_inode(st_ino: __kernel_ulong_t) -> Self {
        Self::new(btrfs_ioctl_search_key {
            tree_id: 0,
            min_objectid: st_ino,
            max_objectid: st_ino,
            min_offset: 0,
            max_offset: u64::MAX,
            min_transid: 0,
            max_transid: u64::MAX,
            min_type: BTRFS_EXTENT_DATA_KEY,
            max_type: BTRFS_EXTENT_DATA_KEY,
            nr_items: u32::MAX,
            unused: 0,
            unused1: 0,
            unused2: 0,
            unused3: 0,
            unused4: 0,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, DekuRead, DekuWrite, DekuSize)]
pub struct BtrfsSearchResultHeader {
    pub transid: u64,
    pub objectid: u64,
    pub offset: u64,
    pub type_: u32,
    pub len: u32,
}

#[derive(Debug, Clone, PartialEq, DekuRead, DekuWrite)]
pub struct BtrfsSearchResult {
    pub header: BtrfsSearchResultHeader,
    #[deku(count = "header.len")]
    pub data: Vec<u8>,
}

impl Iterator for BtrfsSearch {
    type Item = BtrfsSearchResult;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= SEARCH_BUF_SIZE || self.key.nr_items == 0 {
            return None;
        }

        let bitoffset = self.offset as usize * 8;
        if let Ok((_rest, result)) = BtrfsSearchResult::from_bytes((&self.buf, bitoffset)) {
            if result.header.type_ == 0 {
                // we're done, all following parses will return zero
                self.offset = SEARCH_BUF_SIZE;
                return None;
            }

            self.offset +=
                (BtrfsSearchResultHeader::SIZE_BYTES.unwrap() + result.data.len()) as u64;
            Some(result)
        } else {
            None
        }
    }
}
