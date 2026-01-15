use std::{
    fs::File,
    io::{Error, Result},
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd},
        linux::fs::MetadataExt,
    },
    u32, u64,
};

use deku::prelude::*;
use linux_raw_sys::ioctl::BTRFS_IOC_TREE_SEARCH_V2;

use crate::{BtrfsSearchKind, BtrfsSearchResultHeader, BtrfsSearchResults};

/// A query to perform a search on BTRFS trees.
#[derive(Debug, Copy, Clone, DekuRead, DekuWrite)]
pub struct BtrfsSearch {
    /// The tree to search in. Defaults to 0.
    pub tree_id: u64,

    /// See [`objects()`](Self::objects()).
    pub min_objectid: u64,
    /// See [`objects()`](Self::objects()).
    pub max_objectid: u64,

    /// See [`offset()`](Self::offset()).
    pub min_offset: u64,

    /// The max offset to return.
    pub max_offset: u64,

    /// See [`transactions()`](Self::transactions()).
    pub min_transid: u64,
    /// See [`transactions()`](Self::transactions()).
    pub max_transid: u64,

    /// See [`kinds()`](Self::kinds()).
    pub min_kind: u32,
    /// See [`kinds()`](Self::kinds()).
    pub max_kind: u32,

    /// The number of items returned by the kernel.
    ///
    /// The ioctl supports limiting the amount of items returns at request time by setting this,
    /// but this implementation always resets it to `u32::MAX` immediately before querying because
    /// it's a source of confusion.
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

    /// Call once in your main().
    ///
    /// This is a workaround to check that structures are correctly sized.
    /// Will be replaced by a const assert whenever possible.
    pub fn ensure_size() {
        // runtime alternative to the DekuSize approach
        assert_eq!(
            Self::default().to_bytes().unwrap().len(),
            Self::SIZE,
            "BUG: search key length invalid"
        );
    }

    /// The maximum result item size that can be obtained by this search query, in bytes.
    ///
    /// This is calculated from the `min_kind` / `max_kind` range, and statically-known result item
    /// sizes. It should be used to calculate how large a buffer to allocate.
    pub fn result_size(self) -> usize {
        let mut max_item_size = 0;
        for key in
            self.min_kind.min(BtrfsSearchKind::MAX_KEY)..self.max_kind.min(BtrfsSearchKind::MAX_KEY)
        {
            let kind = BtrfsSearchKind::from_key(key);
            max_item_size = max_item_size.max(kind.item_size());
        }

        BtrfsSearchResultHeader::SIZE + max_item_size
    }

    /// The minimum size a buffer can be.
    ///
    /// This is to be used alongside [`with_buf()`](Self::with_buf()). The `buf_size` passed to
    /// [`with_buf_size()`](Self::with_buf_size()) should be lower-bounded by
    /// [`result_size()`](Self::result_size()) instead, as `with_buf_size()` adds the necessary
    /// structure sizes on top.
    pub fn minimum_buf_size(self) -> usize {
        Self::LEADING_OFFSET + self.result_size() + Self::SENTINEL_SIZE
    }

    /// Lookup BTRFS extents for a particular file.
    ///
    /// This is a shorthand for `.objects(&[inode]).kinds(&[ExtentData]).with_buf_size(fd, size)`
    /// which automatically calculates a best-guess for the size for the buffer, and handles the
    /// file metadata to obtain inodes etc. It's appropriate for one-off lookups and exploration.
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
    /// appropriately for your application, and build your own search key. Additionally, using the
    /// low-level methods lets you decouple the lifetime of the file from the iterator.
    ///
    /// See the [`with_buf_size()`](Self::with_buf_size()) documentation for more details.
    pub fn extents_for_file(file: &File) -> Result<BtrfsSearchResults<'_>> {
        let stat = file.metadata()?;
        let file_size = stat.len() as usize;
        let st_ino = stat.st_ino();

        let search = BtrfsSearch::default()
            .kinds(&[BtrfsSearchKind::ExtentData])
            .objects(&[st_ino]);

        // the upper limit is hardcoded to 16MB here:
        // https://github.com/torvalds/linux/blob/master/fs/btrfs/ioctl.c#L1705
        // but we set a 1MB maximum to avoid doing too large allocations.
        //
        // also, experimentally setting this to <=1512 sometimes returns EOVERFLOW,
        // so we set the lower bound to 2kB to just guarantee we're good for it
        //
        // in between, calculate from file_size
        let buf_size = (file_size / (128 * 1024) * search.result_size())
            .max(3 * search.result_size())
            .min(1024_usize.pow(2));

        search.with_buf_size(file.as_fd(), buf_size)
    }

    /// Execute a search on a BTRFS filesystem.
    ///
    /// The `buf_size` specifies the size of the buffer the kernel will write results to.
    /// Internally, the method allocates a buffer that is slightly larger, to accommodate the
    /// search request structures.
    ///
    /// The search is performed immediately when this method is called, but it may return less
    /// than the full amount of results available as it fills only the available buffer space.
    /// The iterator will detect that and issue additional search calls when reaching the end of
    /// result pages, re-using the buffer each time instead of creating new ones internally. You
    /// can retrieve the buffer for further re-use with [`with_buf()`](Self::with_buf()) once done
    /// with the iterator, see [`BtrfsSearchResults::into_buf()`].
    ///
    /// Compared to calling [`with_buf()`](Self::with_buf()) with your own new buffer, this method
    /// is slightly more performant as it doesn't zero the buffer twice on initial allocation.
    ///
    /// The BTRFS filesystem is selected using the `fd` argument: that doesn't need to be the FD
    /// for the file being looked at e.g. with a `.objects(&[inode])` lookup, but it's convenient
    /// for one-off lookups where the file is already opened to obtain its inode. It can be useful
    /// to set the FD to some stable reference to the filesystem, so that lookups for files that
    /// are not on that particular filesystem return no results, and so that the lifetime of the FD
    /// is not the lifetime of the file being looked up.
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
    ) -> Result<BtrfsSearchResults<'fd>> {
        // SAFETY: box_size will never be zero
        let box_size = Self::LEADING_OFFSET + buf_size + Self::SENTINEL_SIZE;

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

    /// Execute a search on a BTRFS filesystem, re-using a buffer.
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
    /// see [`BtrfsSearchResults::into_buf()`].
    ///
    /// The BTRFS filesystem is selected using the `fd` argument: that doesn't need to be the FD
    /// for the file being looked at e.g. with a `.objects(&[inode])` lookup, but it's convenient
    /// for one-off lookups where the file is already opened to obtain its inode. It can be useful
    /// to set the FD to some stable reference to the filesystem, so that lookups for files that
    /// are not on that particular filesystem return no results, and so that the lifetime of the FD
    /// is not the lifetime of the file being looked up.
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
    /// This method panics when given a buffer smaller than `self.minimum_buf_size()`.
    pub fn with_buf<'fd>(
        mut self,
        fd: BorrowedFd<'fd>,
        mut buf: Box<[u8]>,
    ) -> Result<BtrfsSearchResults<'fd>> {
        let buf_len = buf.len();

        // FIXME: can we use .get_mut() / .get() instead of [] in this?
        // current should be safe, but eliminating potential panics is good?

        // SAFETY: we must always have enough buffer space for the search key, buf_size u64,
        // at least one result header + item, and the sentinel. From experimentation, passing
        // shorter buffers doesn't result in UB (it errors cleanly), but better safe than sorry.
        assert!(
            buf_len >= self.minimum_buf_size(),
            "BUG: buffer passed to with_buf is too short (wanted at least {}, got {})",
            self.minimum_buf_size(),
            buf_len,
        );

        // SAFETY: always zero the buffer before using it
        // SAFETY: this additionally forms part of the safety contract in with_buf_size()
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
        // long as it's appropriately-sized, which is checked above. This function borrows the FD,
        // so it's guaranteed safe to use.
        if {
            #[cfg(miri)]
            {
                // Miri doesn't support ioctl, but we still want to use these so Rust doesn't warn
                dbg!(fd.as_raw_fd(), BTRFS_IOC_TREE_SEARCH_V2, buf.as_mut_ptr());
                // Returning 0 will essentially simulate the kernel returning no results, except that
                // nr_items would be incorrectly set. So we later overwrite it just in case.
                0
            }
            #[cfg(not(miri))]
            unsafe {
                libc::ioctl(
                    fd.as_raw_fd(),
                    BTRFS_IOC_TREE_SEARCH_V2 as _,
                    buf.as_mut_ptr(),
                )
            }
        } != 0
        {
            return Err(Error::last_os_error());
        }

        // SAFETY: check the sentinel value before doing anything with the buffer
        assert_eq!(
            buf[(buf_len - Self::SENTINEL_SIZE)..],
            sentinel,
            "KERNEL BUG: overran our buffer"
        );

        let (_rest, mut search) = BtrfsSearch::from_bytes((&buf, 0))?;
        if cfg!(miri) {
            // When running within Miri, the ioctl is simulated to return successfully without
            // touching the buffer. The resulting empty result buffer is not a problem and is
            // expected behaviour, but the kernel would set nr_items to 0. We shouldn't be relying
            // on this value for safety, but just in case let's overwrite it anyway.
            //
            // This is not a #[cfg(miri)] to avoid the "unused mut" warning outside Miri.
            search.nr_items = 0;
        }

        Ok(BtrfsSearchResults {
            buf,
            offset: Self::LEADING_OFFSET,
            items_remaining_in_buf: search.nr_items,
            search,
            next_search_offset: None,
            fd: Some(fd),
        })
    }

    /// Search within a particular tree, by ID.
    pub fn tree(self, id: u64) -> Self {
        Self {
            tree_id: id,
            ..self
        }
    }

    /// Restrict the search to some item kinds.
    ///
    /// Note that this will calculate the item kind range to provide to the lookup, but will not
    /// filter the result set to exactly those kinds. Internally this is implemented by setting
    /// a "low kind" and "high kind" as per the kind discriminant, which is not very precise.
    ///
    /// Pass `&[]` to reset the search to all kinds.
    pub fn kinds(self, kinds: &[BtrfsSearchKind]) -> Self {
        if kinds.is_empty() {
            Self {
                min_kind: 0,
                max_kind: u32::MAX,
                ..self
            }
        } else {
            let mut kinds = kinds.to_vec();
            kinds.sort();

            // UNWRAPs: will always succeed since we've ensured kinds is not empty
            Self {
                min_kind: kinds.first().unwrap().as_key(),
                max_kind: kinds.last().unwrap().as_key(),
                ..self
            }
        }
    }

    /// Restrict the search to some objects.
    ///
    /// Note that this will calculate the object ID range to provide to the lookup, but will not
    /// filter the result set to exactly those objects. Internally this is implemented by setting
    /// a "low ID" and "high ID", which is not very precise.
    ///
    /// Pass `&[]` to reset the search to all objects.
    pub fn objects(self, ids: &[u64]) -> Self {
        if ids.is_empty() {
            Self {
                min_objectid: 0,
                max_objectid: u64::MAX,
                ..self
            }
        } else {
            let mut ids = ids.to_vec();
            ids.sort();

            Self {
                min_objectid: *ids.first().unwrap(),
                max_objectid: *ids.last().unwrap(),
                ..self
            }
        }
    }

    /// Start the result set from an offset.
    ///
    /// This is mainly used internally for pagination. The offset is the one from the
    /// [`BtrfsSearchResultHeader`] structure.
    pub fn offset(self, offset: u64) -> Self {
        Self {
            min_offset: offset,
            ..self
        }
    }

    /// Search within a subset of transactions.
    pub fn transactions(self, min: u64, max: u64) -> Self {
        Self {
            min_transid: min,
            max_transid: max,
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
