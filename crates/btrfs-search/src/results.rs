use std::{mem::take, os::fd::BorrowedFd};

use deku::prelude::*;

use crate::{BtrfsSearch, BtrfsSearchKind, BtrfsSearchResultHeader, BtrfsSearchResultItem};

// TODO: doing zero-copy interpretation would be nice for perf;
// look into if there's something like deku for ergonomics tho
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
    pub(crate) items_remaining_in_buf: u32,
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
    /// This may vary when the iterator pages through the results.
    pub fn nr_items(&self) -> u32 {
        self.search.nr_items
    }
}

impl Iterator for BtrfsSearchResults<'_> {
    type Item = std::result::Result<BtrfsSearchResult, DekuError>;

    // most of this iterator will do more useless compute if you keep iterating
    // after you get a None. none of the normal iterator consumers will do that,
    // so if you find yourself with a bug here, report it but also consider fuse()
    fn next(&mut self) -> Option<Self::Item> {
        if self.search.nr_items == 0 {
            // the kernel says there's nothing more to see
            return None;
        }

        // there's some cases items_remaining is zero, but there's more data to get.
        if self.items_remaining_in_buf > 0 {
            let buf = self.buf.get(self.offset..).unwrap_or_default();
            if buf.is_empty() {
                // should not happen (should be caught by other bits)
                // but let's handle it anyway to make sure
                debug_assert!(!buf.is_empty(), "should not happen");
                return None;
            }

            match BtrfsSearchResult::from_bytes((&buf, 0)) {
                Ok((
                    _,
                    BtrfsSearchResult {
                        header:
                            BtrfsSearchResultHeader {
                                // kind is never None in legitimate output, so we have to assume
                                // we're reading unused zeroed space. don't interpret it as anything!
                                kind: BtrfsSearchKind::None,
                                ..
                            },
                        ..
                    },
                )) => {
                    // if we're reading zeroed space, we don't want to go forward on this page
                    self.items_remaining_in_buf = 0;

                    if buf.len() >= self.search.result_size() * 2 {
                        // if the buffer still has more than enough space in it for results
                        // we don't need to do another read to know we're at the end!
                        // note how this is checking for 2x while the minimum buf_size is 3x
                        return None;
                    }

                    // fall through to the code that decides whether to paginate or to quit
                }
                Ok((_, result)) => {
                    // this is what is actually used to continue the read
                    self.offset += BtrfsSearchResultHeader::SIZE + result.item.len();
                    self.next_search_offset = Some(result.header.offset + 1);

                    // this is used to know when to stop
                    self.items_remaining_in_buf = self.items_remaining_in_buf.saturating_sub(1);

                    return Some(Ok(result));
                }
                Err(err) => {
                    // if we fail the parse, we can't safely go forward on this page
                    self.items_remaining_in_buf = 0;

                    // return this error; the next iteration will either paginate or quit
                    return Some(Err(err));
                }
            }
        }

        let Some(off) = self.next_search_offset else {
            // should not happen (should be caught by other bits)
            // but let's handle it anyway to make sure
            debug_assert!(self.next_search_offset.is_none(), "should not happen");
            return None;
        };

        // we've arrived at the end of our buffer, but there's more data to be had!
        // iterate onwards but reuse the same buffer to avoid reallocating
        let buf = take(&mut self.buf);
        assert_ne!(buf.len(), 0, "BUG: the iterator buffer was take()n twice");
        let fd = take(&mut self.fd).expect("BUG: the iterator fd was take()n twice");

        match self.search.offset(off).with_buf(fd, buf) {
            Err(err) => {
                return Some(Err(err.into()));
            }
            Ok(next) => {
                *self = next;

                // recursing in an iterator is not great, but this will be limited:
                // it will either return None or Some and should not itself recurse
                return self.next();
            }
        }
    }
}
