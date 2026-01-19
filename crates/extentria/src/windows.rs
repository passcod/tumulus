use std::fs::File;
use std::io;
use std::os::windows::io::AsRawHandle;

use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::System::IO::DeviceIoControl;
use windows_sys::Win32::System::Ioctl::{
    FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES,
};

use crate::types::DataRange;

/// Minimum buffer size: enough for the input struct plus at least a few results.
const MIN_BUFFER_SIZE: usize = std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>() * 16;

/// Range reader for Windows using FSCTL_QUERY_ALLOCATED_RANGES.
///
/// This implementation uses a raw byte buffer that can be reused across multiple
/// file lookups to minimize allocations. Results are yielded lazily via an iterator
/// that paginates through the kernel's results on demand.
pub struct RangeReader {
    buffer: Option<Box<[u8]>>,
    buffer_size: usize,
}

impl RangeReader {
    /// Create a new reader with default buffer size (64KB).
    pub fn new() -> Self {
        Self::with_buffer_size(64 * 1024)
    }

    /// Create a reader with a specific buffer size.
    ///
    /// The buffer size determines how many extent results can be held at once.
    /// Larger buffers mean fewer system calls for files with many extents.
    pub fn with_buffer_size(size: usize) -> Self {
        let size = size.max(MIN_BUFFER_SIZE);
        Self {
            buffer: None,
            buffer_size: size,
        }
    }

    /// Create a reader reusing an existing buffer.
    ///
    /// This allows buffer reuse across multiple files or even multiple `RangeReader`
    /// instances. The buffer will be used as-is for the next `read_ranges` call.
    pub fn with_buffer(buf: Box<[u8]>) -> Self {
        let buffer_size = buf.len().max(MIN_BUFFER_SIZE);
        Self {
            buffer: Some(buf),
            buffer_size,
        }
    }

    /// Consume the reader and return its buffer for reuse.
    ///
    /// Returns `None` if the buffer is currently in use by an active iterator
    /// (i.e., if `read_ranges` was called but the iterator wasn't fully consumed).
    pub fn into_buffer(self) -> Option<Box<[u8]>> {
        self.buffer
    }

    /// Read data ranges for a file.
    ///
    /// Returns an iterator that lazily fetches extent information from the kernel.
    /// The iterator will paginate through results as needed, reusing the internal
    /// buffer for each page.
    ///
    /// When the iterator is dropped or fully consumed, the buffer is returned to
    /// this `RangeReader` for reuse in subsequent calls.
    pub fn read_ranges<'a>(
        &'a mut self,
        file: &'a File,
    ) -> io::Result<impl Iterator<Item = io::Result<DataRange>> + 'a> {
        let file_size = file.metadata()?.len();
        let handle = file.as_raw_handle() as HANDLE;

        // Take ownership of the buffer, or allocate a new one
        let buffer = self
            .buffer
            .take()
            .unwrap_or_else(|| vec![0u8; self.buffer_size].into_boxed_slice());

        Ok(WindowsRangeIter {
            handle,
            file_size,
            buffer: Some(buffer),
            buffer_return: &mut self.buffer,
            query_offset: 0,
            current_pos: 0,
            buf_index: 0,
            items_in_buffer: 0,
            pending_data: None,
            done: false,
            needs_fetch: true,
        })
    }
}

impl Default for RangeReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over allocated ranges in a Windows file.
///
/// This iterator lazily fetches extent information from the kernel, yielding
/// `DataRange` items one at a time. It handles sparse holes automatically by
/// detecting gaps between allocated ranges.
pub struct WindowsRangeIter<'a> {
    handle: HANDLE,
    file_size: u64,
    buffer: Option<Box<[u8]>>,
    buffer_return: &'a mut Option<Box<[u8]>>,
    query_offset: u64,
    current_pos: u64,
    buf_index: usize,
    items_in_buffer: usize,
    pending_data: Option<DataRange>,
    done: bool,
    needs_fetch: bool,
}

impl WindowsRangeIter<'_> {
    /// Fetch the next page of results from the kernel.
    ///
    /// Returns `Ok(true)` if we got results, `Ok(false)` if there are no more,
    /// or `Err` on failure.
    fn fetch_page(&mut self) -> io::Result<bool> {
        if self.query_offset >= self.file_size {
            return Ok(false);
        }

        let buffer = self
            .buffer
            .as_mut()
            .expect("buffer should exist during iteration");

        let input = FILE_ALLOCATED_RANGE_BUFFER {
            FileOffset: self.query_offset as i64,
            Length: (self.file_size - self.query_offset) as i64,
        };

        let mut bytes_returned: u32 = 0;

        let result = unsafe {
            DeviceIoControl(
                self.handle,
                FSCTL_QUERY_ALLOCATED_RANGES,
                &input as *const _ as *const _,
                std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>() as u32,
                buffer.as_mut_ptr() as *mut _,
                buffer.len() as u32,
                &mut bytes_returned,
                std::ptr::null_mut(),
            )
        };

        if result == 0 {
            let err = io::Error::last_os_error();
            // ERROR_MORE_DATA (234) means buffer was too small, but we got some results
            if err.raw_os_error() != Some(234) {
                return Err(err);
            }
        }

        self.items_in_buffer =
            bytes_returned as usize / std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
        self.buf_index = 0;
        self.needs_fetch = false;

        Ok(self.items_in_buffer > 0)
    }

    /// Get a range entry from the buffer at the given index.
    fn get_range_at(&self, index: usize) -> Option<(u64, u64)> {
        let buffer = self.buffer.as_ref()?;

        if index >= self.items_in_buffer {
            return None;
        }

        let entry_size = std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
        let offset = index * entry_size;

        if offset + entry_size > buffer.len() {
            return None;
        }

        // SAFETY: We've verified the buffer has enough space and is properly aligned
        // for FILE_ALLOCATED_RANGE_BUFFER (which only contains i64 fields).
        let ptr = buffer.as_ptr().wrapping_add(offset) as *const FILE_ALLOCATED_RANGE_BUFFER;
        let entry = unsafe { &*ptr };

        Some((entry.FileOffset as u64, entry.Length as u64))
    }

    /// Handle the end of iteration, returning trailing sparse hole if needed.
    fn handle_end(&mut self) -> Option<io::Result<DataRange>> {
        if self.current_pos < self.file_size {
            let hole = DataRange::sparse(self.current_pos, self.file_size - self.current_pos);
            self.current_pos = self.file_size;
            self.done = true;
            Some(Ok(hole))
        } else {
            self.done = true;
            None
        }
    }
}

impl Iterator for WindowsRangeIter<'_> {
    type Item = io::Result<DataRange>;

    fn next(&mut self) -> Option<Self::Item> {
        // Return pending data range first (happens after returning a sparse hole)
        if let Some(range) = self.pending_data.take() {
            return Some(Ok(range));
        }

        // Already finished?
        if self.done {
            return None;
        }

        // Handle empty files
        if self.file_size == 0 {
            self.done = true;
            return None;
        }

        // Fetch first/next page if needed
        if self.needs_fetch || self.buf_index >= self.items_in_buffer {
            match self.fetch_page() {
                Ok(true) => {
                    // Got results, continue below
                }
                Ok(false) => {
                    // No more data from kernel
                    return self.handle_end();
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }

        // Get next range from buffer
        let Some((offset, length)) = self.get_range_at(self.buf_index) else {
            // Buffer exhausted, try to fetch more
            self.needs_fetch = true;
            return self.next();
        };

        self.buf_index += 1;

        // Update query_offset for next page (if we need one)
        self.query_offset = offset + length;

        // Check for sparse hole before this range
        if offset > self.current_pos {
            let hole = DataRange::sparse(self.current_pos, offset - self.current_pos);
            // Store the data range to return on next iteration
            self.pending_data = Some(DataRange::new(offset, length));
            self.current_pos = offset + length;
            return Some(Ok(hole));
        }

        // Return this extent as a data range
        self.current_pos = offset + length;
        Some(Ok(DataRange::new(offset, length)))
    }
}

impl Drop for WindowsRangeIter<'_> {
    fn drop(&mut self) {
        // Return the buffer to the RangeReader for reuse
        if let Some(buf) = self.buffer.take() {
            *self.buffer_return = Some(buf);
        }
    }
}
