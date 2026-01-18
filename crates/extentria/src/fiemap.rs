use std::{
    fs::File,
    io::{Error, Result},
    mem::{take, transmute},
    os::fd::{AsFd, AsRawFd, BorrowedFd},
    u32, u64,
};

use linux_raw_sys::ioctl::{
    FIEMAP_EXTENT_DATA_ENCRYPTED, FIEMAP_EXTENT_DATA_INLINE, FIEMAP_EXTENT_DATA_TAIL,
    FIEMAP_EXTENT_DELALLOC, FIEMAP_EXTENT_ENCODED, FIEMAP_EXTENT_LAST, FIEMAP_EXTENT_MERGED,
    FIEMAP_EXTENT_NOT_ALIGNED, FIEMAP_EXTENT_SHARED, FIEMAP_EXTENT_UNKNOWN,
    FIEMAP_EXTENT_UNWRITTEN, FIEMAP_FLAG_CACHE, FIEMAP_FLAG_SYNC, FIEMAP_FLAG_XATTR, FS_IOC_FIEMAP,
};
use zerocopy::{FromBytes, IntoBytes as _, KnownLayout};
use zerocopy_derive::*;

/// An extent lookup using FIEMAP.
#[derive(Debug, Copy, Clone)]
pub struct FiemapLookup {
    /// Byte offset (inclusive) at which to start mapping.
    pub start: u64,

    /// Logical length of mapping which userspace wants.
    pub length: u64,

    /// Flags for request.
    pub flags: u32,
}

/// A request to the FIEMAP ioctl.
#[derive(Debug, Copy, Clone, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct FiemapRequest {
    /// Byte offset (inclusive) at which to start mapping.
    pub start: u64,

    /// Logical length of mapping which userspace wants.
    pub length: u64,

    /// Flags for request.
    pub flags: u32,

    /// (out) number of extents that were mapped
    pub written: u32,

    /// (in) number of extents that can fit in provided buffer
    pub array_size: u32,

    /// (reserved)
    _reserved: u32,
}

#[derive(Debug, Clone, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct FiemapExtent {
    /// Byte offset of the extent in the file.
    pub logical_offset: u64,

    /// Byte offset of the extent on disk.
    pub physical_offset: u64,

    /// Length in byte for this extent.
    pub length: u64,

    _reserved1: [u64; 2],

    /// Flags for this extent.
    pub flags: u32,

    _reserved2: [u32; 3],
}

impl FiemapExtent {
    pub fn last(&self) -> bool {
        self.flags & FIEMAP_EXTENT_LAST == 1
    }

    pub fn location_unknown(&self) -> bool {
        self.flags & FIEMAP_EXTENT_UNKNOWN == 1
    }

    pub fn delayed_allocation(&self) -> bool {
        self.flags & FIEMAP_EXTENT_DELALLOC == 1
    }

    pub fn encoded(&self) -> bool {
        self.flags & FIEMAP_EXTENT_ENCODED == 1
    }

    pub fn encrypted(&self) -> bool {
        self.flags & FIEMAP_EXTENT_DATA_ENCRYPTED == 1
    }

    pub fn not_aligned(&self) -> bool {
        self.flags & FIEMAP_EXTENT_NOT_ALIGNED == 1
    }

    pub fn inline(&self) -> bool {
        self.flags & FIEMAP_EXTENT_DATA_INLINE == 1
    }

    pub fn packed(&self) -> bool {
        self.flags & FIEMAP_EXTENT_DATA_TAIL == 1
    }

    pub fn unwritten(&self) -> bool {
        self.flags & FIEMAP_EXTENT_UNWRITTEN == 1
    }

    pub fn simulated(&self) -> bool {
        self.flags & FIEMAP_EXTENT_MERGED == 1
    }

    pub fn shared(&self) -> bool {
        self.flags & FIEMAP_EXTENT_SHARED == 1
    }
}

/// The size of the request structure (exclusive of the results buf), in bytes.
fn request_size() -> usize {
    FiemapRequest::size_for_metadata(()).unwrap()
}

/// The size of one result item, in bytes.
pub fn result_size() -> usize {
    FiemapExtent::size_for_metadata(()).unwrap()
}

/// The minimum size a buffer can be, in bytes.
///
/// This is to be used alongside [`with_buf()`](Self::with_buf()). The `buf_size` passed to
/// [`with_buf_size()`](Self::with_buf_size()) should be lower-bounded by
/// [`result_size()`](Self::result_size()) instead, as `with_buf_size()` adds the necessary
/// structure sizes on top.
pub fn minimum_buf_size() -> usize {
    request_size() + result_size()
}

impl FiemapLookup {
    /// Lookup extents for a particular file.
    ///
    /// This is a shorthand for `FiemapLookup::for_file_size(size).with_buf_size(fd, buf_size)`
    /// which automatically calculates a best-guess for the size for the buffer and handles
    /// obtaining the FD etc. It's appropriate for one-off lookups and exploration.
    ///
    /// The buffer size calculation makes the assumption that it will on average be called on files:
    /// - that are not sparse or internally deduplicated (so they are uniformly extented)
    /// - that have a compressible first extent (so compression is enabled for the file)
    ///
    /// When both of these conditions hold, a file will most likely have SIZE/128KB extents or less.
    /// Thus we can allocate a buffer that will hold that many results and only perform a single
    /// lookup for most files under a gigabyte. The buffer is limited to 1 MiB to avoid allocating
    /// and zeroing too much memory repeatedly.
    ///
    /// When performing lookups in batches, you most likely will want to use your own buffer size
    /// appropriately for your application. See the [`with_buf_size()`](Self::with_buf_size())
    /// documentation for more details.
    pub fn extents_for_file(file: &File) -> Result<FiemapSearchResults<'_>> {
        let stat = file.metadata()?;
        let file_size = stat.len();

        // the upper limit is hardcoded to 16MB here:
        // https://github.com/torvalds/linux/blob/master/fs/btrfs/ioctl.c#L1705
        // but we set a 1MB maximum to avoid doing too large allocations.
        //
        // in between, calculate from file_size
        let buf_size = ((file_size as usize) / (128 * 1024) * result_size())
            .max(3 * result_size())
            .min(1024_usize.pow(2));

        Self::for_file_size(file_size).with_buf_size(file.as_fd(), buf_size)
    }

    /// Create a new lookup that covers the entire spread of a file, by the size of that file.
    pub fn for_file_size(file_size: u64) -> Self {
        Self {
            start: 0,
            length: file_size,
            flags: 0,
        }
    }

    /// Start the result set from an offset.
    ///
    /// This is mainly used internally for pagination.
    pub fn from_offset(self, offset: u64) -> Self {
        Self {
            start: offset,
            ..self
        }
    }

    /// Set the sync flag (syncs the filesystem before lookup).
    pub fn after_sync(self) -> Self {
        let mut this = self;
        this.flags |= FIEMAP_FLAG_SYNC;
        this
    }

    /// Set the xattr flag (searches the xattr tree instead).
    pub fn on_xattr_tree(self) -> Self {
        let mut this = self;
        this.flags |= FIEMAP_FLAG_XATTR;
        this
    }

    /// Set the cache flag (requests that returned extents be cached).
    pub fn and_cache(self) -> Self {
        let mut this = self;
        this.flags |= FIEMAP_FLAG_CACHE;
        this
    }

    /// Execute an extent lookup on the filesystem.
    ///
    /// The `buf_size` specifies the size of the buffer the kernel will write results to.
    /// Internally, the method allocates a buffer that is slightly larger, to accommodate the
    /// lookup request structures.
    ///
    /// The lookup is performed immediately when this method is called, but it may return less
    /// than the full amount of results available as it fills only the available buffer space.
    /// The iterator will detect that and issue additional search calls when reaching the end of
    /// result pages, re-using the buffer each time instead of creating new ones internally. You
    /// can retrieve the buffer for further re-use with [`with_buf()`](Self::with_buf()) once done
    /// with the iterator, see [`FiemapSearchResults::into_buf()`].
    ///
    /// Compared to calling [`with_buf()`](Self::with_buf()) with your own new buffer, this method
    /// is slightly more performant as it doesn't zero the buffer twice on initial allocation.
    ///
    /// Note that the `fd` borrow is passed to the iterator, as it must remain valid so that
    /// the iterator can execute further searches as required.
    ///
    /// # Panics
    ///
    /// This method panics when given a size smaller than `self.result_size()`. The panic message
    /// will reference a larger size, as it comes from [`with_buf()`](Self::with_buf()).
    ///
    /// This method also panics when `buf_size > isize::MAX` (as do all allocations), or when the
    /// allocation fails.
    pub fn with_buf_size<'fd>(
        self,
        fd: BorrowedFd<'fd>,
        buf_size: usize,
    ) -> Result<FiemapSearchResults<'fd>> {
        // SAFETY: box_size will never be zero
        let box_size = request_size() + buf_size;
        debug_assert_ne!(box_size, 0);

        // SAFETY: with_buf() immediately zeroes the buffer, so it's safe to construct uninit
        let buf = {
            // SAFETY: the requirements for calling this safely are:
            // - align must not be zero: we hardcode to 1
            // - align must be a power of two: 1 is a power of two
            // - size, when rounded up to the nearest multiple of align, must <= isize::MAX
            //
            // We allocate a region that can hold a `[u8]` of size buf_size: the alignment is 1
            // and every byte is contiguous without padding.
            assert!(box_size <= isize::MAX as usize);
            let layout = unsafe { std::alloc::Layout::from_size_align_unchecked(box_size, 1) };

            // SAFETY: we never read from this region before zeroing
            // SAFETY: box_size is never zero, which upholds the requirement that layout is non-zero
            let ptr = unsafe { std::alloc::alloc(layout) };
            if ptr.is_null() {
                panic!("Failed to allocate buffer");
            }

            // SAFETY:
            // - the allocation must be correct for the type (ensured above)
            // - the raw pointer points to a valid value of the right type (deliberately not done)
            // - the pointer has to be non-null (checked above)
            // - the pointer must be sufficiently aligned (alignment for u8 is 1)
            // - the pointer must not be used twice
            let raw = std::ptr::slice_from_raw_parts_mut(ptr, box_size);
            unsafe { Box::from_raw(raw) }
        };

        self.with_buf(fd, buf)
    }

    /// Execute an extent lookup on the filesystem, re-using a buffer.
    ///
    /// This is typically used after obtaining a buffer from the iterator of a previous search.
    /// You may also use it with a buffer from another source, but it's recommended to use
    /// [`with_buf_size()`](Self::with_buf_size()) if you were going to allocate a new buffer.
    /// The buffer is always immediately zeroed.
    ///
    /// The `buf` argument is used both to hold the search request and then for the kernel to write
    /// results to. It must be appropriately sized: the minimum for the current search is available
    /// with `minimum_buf_size()`, and `result_size()` can be used to size a buffer large enough
    /// for the desired amount of results. Note pagination as explained below.
    ///
    /// This method takes a buffer explicitly so that it can be re-used. The search is performed
    /// immediately when this method is called, but it may return less than the full amount of
    /// results available. The iterator will detect that and issue additional search calls when
    /// reaching the end of results, re-using the buffer each time instead of creating new ones
    /// internally. You can retrieve the buffer for further re-use once done with the iterator,
    /// see [`FiemapSearchResults::into_buf()`].
    ///
    /// Note that the `fd` borrow is passed to the iterator, as it must remain valid so that the
    /// iterator can execute further searches as required.
    ///
    /// When allocating a buffer, you should use something like this to avoid running into
    /// stack overflows at large buffer sizes (`vec![]` is specially constructed to allocate
    /// directly onto the heap):
    ///
    /// ```
    /// vec![0u8; 65536].into_boxed_slice();
    /// ```
    ///
    /// # Panics
    ///
    /// This method panics when given a buffer smaller than `minimum_buf_size()`.
    pub fn with_buf<'fd>(
        self,
        fd: BorrowedFd<'fd>,
        mut buf: Box<[u8]>,
    ) -> Result<FiemapSearchResults<'fd>> {
        let buf_len = buf.len();

        // FIXME: can we use .get_mut() / .get() instead of [] in this?
        // current should be safe, but eliminating potential panics is good?

        // SAFETY: we must always have enough buffer space for the search key, buf_size u64,
        // at least one result header + item, and the sentinel. From experimentation, passing
        // shorter buffers doesn't result in UB (it errors cleanly), but better safe than sorry.
        assert!(
            buf_len >= minimum_buf_size(),
            "BUG: buffer passed to with_buf is too short (wanted at least {}, got {})",
            minimum_buf_size(),
            buf_len,
        );

        // SAFETY: always zero the buffer before using it
        // SAFETY: this additionally forms part of the safety contract in with_buf_size()
        buf.fill(0);

        // SAFETY: since buf_len is >= minimum_buf_size, array_size is always >= 1
        // if an absurdly large buf is passed in, we can only write up to a u32 anyway
        let array_size =
            u32::try_from((buf_len - request_size()) / result_size()).unwrap_or(u32::MAX);
        debug_assert_ne!(array_size, 0);

        FiemapRequest {
            start: self.start,
            length: self.length,
            flags: self.flags,
            _reserved: 0,
            written: 0,
            array_size,
        }
        .write_to_prefix(&mut buf)
        .map_err(|err| std::io::Error::other(err.to_string()))?;

        // SAFETY: the general lack of documentation for ioctls and this one in particular makes
        // validating this usage extremely annoying. Fortunately, the ioctl syscall is relatively
        // well-behaved: if you pass a bad pointer or undersized buffer, it will tell you so. The
        // kernel only uses this pointer for the duration of the syscall, and we zero the buffer
        // in this function prior to using it, ensuring it's always safe to pass any buffer, as
        // long as it's appropriately-sized, which is checked above. This function borrows the FD,
        // so it's guaranteed safe to use.
        if {
            #[cfg(miri)]
            {
                // Miri doesn't support ioctl, but we still want to use these so Rust doesn't warn
                dbg!(fd.as_raw_fd(), FS_IOC_FIEMAP, buf.as_mut_ptr());
                // Returning 0 will essentially simulate the kernel successfully returning no results.
                0
            }
            #[cfg(not(miri))]
            unsafe {
                libc::ioctl(fd.as_raw_fd(), FS_IOC_FIEMAP as _, buf.as_mut_ptr())
            }
        } != 0
        {
            return Err(Error::last_os_error());
        }

        let (response, rest) = FiemapRequest::read_from_prefix(&buf)
            .map_err(|err| std::io::Error::other(err.to_string()))?;

        debug_assert_eq!(buf.len().saturating_sub(rest.len()), request_size());

        Ok(FiemapSearchResults {
            buf,
            offset: request_size(),
            items_remaining_in_buf: response.written,
            response,
            next_search_offset: None,
            fd: Some(fd),
            seen_last_extent: false,
        })
    }
}

#[derive(Debug)]
pub struct FiemapSearchResults<'fd> {
    buf: Box<[u8]>,
    offset: usize,
    items_remaining_in_buf: u32,
    response: FiemapRequest,
    next_search_offset: Option<u64>,
    fd: Option<BorrowedFd<'fd>>,
    seen_last_extent: bool,
}

impl FiemapSearchResults<'_> {
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
        self.response.written
    }

    /// Prevent further pagination.
    ///
    /// This has two purpose: first, obviously, to stop pagination; second it releases the borrowed
    /// FD and returns a result iterator that is `'static`.
    pub fn stop_paginating(self) -> FiemapSearchResults<'static> {
        let mut this = self;
        this.fd = None;

        // SAFETY: the fd is no longer held, so the only lifetime remaining is owned data ('static)
        unsafe { transmute(this) }
    }
}

impl<'f> Iterator for FiemapSearchResults<'f> {
    type Item = std::io::Result<&'f FiemapExtent>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.seen_last_extent {
            // the kernel says there's nothing more to see
            return None;
        }

        if self.items_remaining_in_buf > 0 {
            let buf = self.buf.get(self.offset..).unwrap_or_default();
            if buf.is_empty() {
                // should not happen (should be caught by other bits)
                // but let's handle it anyway to make sure
                debug_assert!(!buf.is_empty(), "should not happen");
                return None;
            }

            match FiemapExtent::ref_from_prefix(buf) {
                Ok((result, _)) => {
                    // this is what is actually used to continue the read
                    self.offset += result_size();

                    // this is used to paginate - use logical offset since fm_start is a file offset
                    self.next_search_offset = Some(result.logical_offset + result.length);

                    // this is used to know when to stop reading
                    self.items_remaining_in_buf = self.items_remaining_in_buf.saturating_sub(1);
                    if result.last() {
                        self.seen_last_extent = true;
                    }

                    // SAFETY: honestly this one I'm unsure about
                    return Some(Ok(unsafe { transmute(result) }));
                }
                Err(err) => {
                    // if we fail the parse, we can't safely go forward on this page
                    self.items_remaining_in_buf = 0;

                    // return this error; the next iteration will either paginate or quit
                    return Some(Err(std::io::Error::other(err.to_string())));
                }
            }
        }

        let Some(off) = self.next_search_offset else {
            // should not happen (should be caught by other bits)
            // but let's handle it anyway to make sure
            debug_assert!(self.next_search_offset.is_none(), "should not happen");
            return None;
        };

        let Some(fd) = take(&mut self.fd) else {
            // if the fd isn't available here, then we can't paginate
            return None;
        };

        // we've arrived at the end of our buffer, but there's more data to be had!
        // iterate onwards but reuse the same buffer to avoid reallocating
        let buf = take(&mut self.buf);
        assert_ne!(buf.len(), 0, "BUG: the iterator buffer was take()n twice");

        let lookup = FiemapLookup {
            start: off,
            length: self.response.length.saturating_sub(off),
            flags: self.response.flags,
        };

        match lookup.with_buf(fd, buf) {
            Err(err) => {
                // if we fail the fetch, we may be able to retry again, leave the decision to the caller.
                // but a caller should note that if errors aren't handled, an error here will probably spin
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
