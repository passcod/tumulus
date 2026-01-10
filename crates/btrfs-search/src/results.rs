use std::{mem::take, os::fd::BorrowedFd};

use deku::prelude::*;

use crate::{BtrfsSearch, BtrfsSearchKind, BtrfsSearchResultHeader, BtrfsSearchResultItem};

#[derive(Debug, Clone, PartialEq, DekuRead)]
pub struct BtrfsSearchResult {
    pub header: BtrfsSearchResultHeader,
    #[deku(ctx = "header.kind, header.len")]
    pub item: BtrfsSearchResultItem,
}

#[derive(Debug)]
pub struct BtrfsSearchResults<'fd> {
    pub(crate) buf: Box<[u8]>,
    pub(crate) offset: usize,
    pub(crate) search: BtrfsSearch,
    pub(crate) next_search_offset: Option<u64>,
    pub(crate) fd: Option<BorrowedFd<'fd>>,
}

impl BtrfsSearchResults<'_> {
    /// Destroys this iterator but keep the buffer.
    ///
    /// It can be useful to re-use the buffer for another search instead of allocating a new one.
    pub fn into_buf(self) -> Box<[u8]> {
        self.buf
    }

    /// The number of items the kernel returned.
    ///
    /// This will vary when the iterator pages through the results.
    pub fn nr_items(&self) -> u32 {
        self.search.nr_items
    }
}

impl Iterator for BtrfsSearchResults<'_> {
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
                if result.header.kind != BtrfsSearchKind::None {
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

        if self.buf[self.offset..].len() >= self.search.result_size() * 2 {
            // if the buffer still has more than enough space in it for results
            // we don't need to do another read to know we're at the end!
            // note how this is checking for 2x while the minimum buf_size is 3x
            return None;
        }

        // we've arrived at the end of our buffer, but there's more data to be had!
        // iterate onwards but reuse the same buffer to avoid reallocating
        let buf = take(&mut self.buf);
        assert_ne!(buf.len(), 0, "BUG: the iterator buffer was take()n twice");
        let fd = take(&mut self.fd).expect("BUG: the iterator fd was take()n twice");

        match self.search.offset(off).with_buf(fd, buf) {
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
