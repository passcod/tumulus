use std::fs::File;
use std::io;
use std::os::windows::io::AsRawHandle;

use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES,
};
use windows_sys::Win32::System::IO::DeviceIoControl;

use crate::types::DataRange;

/// Range reader for Windows using FSCTL_QUERY_ALLOCATED_RANGES.
pub struct RangeReader {
    buffer: Vec<FILE_ALLOCATED_RANGE_BUFFER>,
}

impl RangeReader {
    pub fn new() -> Self {
        Self::with_buffer_size(64 * 1024)
    }

    pub fn with_buffer_size(size: usize) -> Self {
        let count = size / std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
        let count = count.max(16); // At least 16 entries
        Self {
            buffer: vec![
                FILE_ALLOCATED_RANGE_BUFFER {
                    FileOffset: 0,
                    Length: 0,
                };
                count
            ],
        }
    }

    pub fn with_buffer(buf: Box<[u8]>) -> Self {
        // Convert byte buffer to typed buffer
        let count = buf.len() / std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
        let count = count.max(16);
        Self {
            buffer: vec![
                FILE_ALLOCATED_RANGE_BUFFER {
                    FileOffset: 0,
                    Length: 0,
                };
                count
            ],
        }
    }

    pub fn into_buffer(self) -> Option<Box<[u8]>> {
        // Convert back to bytes (approximate)
        let bytes = self.buffer.len() * std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
        Some(vec![0u8; bytes].into_boxed_slice())
    }

    pub fn read_ranges(
        &mut self,
        file: &File,
    ) -> io::Result<impl Iterator<Item = io::Result<DataRange>>> {
        let file_size = file.metadata()?.len();
        let handle = file.as_raw_handle() as HANDLE;

        let mut ranges = Vec::new();
        let mut query_offset = 0u64;

        while query_offset < file_size {
            let input = FILE_ALLOCATED_RANGE_BUFFER {
                FileOffset: query_offset as i64,
                Length: (file_size - query_offset) as i64,
            };

            let mut bytes_returned: u32 = 0;
            let buffer_size =
                (self.buffer.len() * std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>()) as u32;

            let result = unsafe {
                DeviceIoControl(
                    handle,
                    FSCTL_QUERY_ALLOCATED_RANGES,
                    &input as *const _ as *const _,
                    std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>() as u32,
                    self.buffer.as_mut_ptr() as *mut _,
                    buffer_size,
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

            let count =
                bytes_returned as usize / std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();

            if count == 0 {
                break;
            }

            let mut current_pos = query_offset;
            for range in self.buffer.iter().take(count) {
                let offset = range.FileOffset as u64;
                let length = range.Length as u64;

                // Add sparse hole before this range
                if offset > current_pos {
                    ranges.push(DataRange::sparse(current_pos, offset - current_pos));
                }

                // Add the data range
                ranges.push(DataRange::new(offset, length));
                current_pos = offset + length;
            }

            query_offset = current_pos;

            // If we got fewer results than buffer capacity, we're done
            if count < self.buffer.len() {
                break;
            }
        }

        // Add trailing sparse hole if needed
        if query_offset < file_size {
            ranges.push(DataRange::sparse(query_offset, file_size - query_offset));
        }

        Ok(ranges.into_iter().map(Ok))
    }
}

impl Default for RangeReader {
    fn default() -> Self {
        Self::new()
    }
}
