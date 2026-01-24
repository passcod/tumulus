//! SEEK_HOLE/SEEK_DATA implementation for Unix systems.
//!
//! This module provides a shared implementation for platforms that support
//! the SEEK_HOLE and SEEK_DATA lseek operations (macOS, FreeBSD, etc.).

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;

use crate::types::DataRange;

/// Read data ranges using SEEK_HOLE and SEEK_DATA.
///
/// Returns an iterator of data ranges. Sparse holes are represented as
/// `DataRange` with `flags.sparse = true`.
pub fn read_ranges(file: &File) -> io::Result<SeekRangeIter> {
    let file_size = file.metadata()?.len();
    let fd = file.as_raw_fd();

    Ok(SeekRangeIter {
        fd,
        file_size,
        current_pos: 0,
        done: false,
    })
}

/// Iterator over data ranges using SEEK_HOLE/SEEK_DATA.
pub struct SeekRangeIter {
    fd: i32,
    file_size: u64,
    current_pos: u64,
    done: bool,
}

impl Iterator for SeekRangeIter {
    type Item = io::Result<DataRange>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.current_pos >= self.file_size {
            return None;
        }

        // Find the next data region
        let data_start = match seek_data(self.fd, self.current_pos) {
            Ok(pos) => pos,
            Err(e) if e.raw_os_error() == Some(libc::ENXIO) => {
                // No more data - rest is sparse or we're at EOF
                if self.current_pos < self.file_size {
                    let hole = DataRange::hole(self.current_pos, self.file_size - self.current_pos);
                    self.done = true;
                    return Some(Ok(hole));
                }
                return None;
            }
            Err(e) => return Some(Err(e)),
        };

        // If there's a hole before data, return it
        if data_start > self.current_pos {
            let hole = DataRange::hole(self.current_pos, data_start - self.current_pos);
            self.current_pos = data_start;
            return Some(Ok(hole));
        }

        // Find where the data ends (next hole)
        let data_end = match seek_hole(self.fd, data_start) {
            Ok(pos) => pos,
            Err(e) if e.raw_os_error() == Some(libc::ENXIO) => {
                // No hole found - data goes to end of file
                self.file_size
            }
            Err(e) => return Some(Err(e)),
        };

        let range = DataRange::new(data_start, data_end - data_start);
        self.current_pos = data_end;

        Some(Ok(range))
    }
}

/// Seek to the next data region at or after the given offset.
pub fn seek_data(fd: i32, offset: u64) -> io::Result<u64> {
    let result = unsafe { libc::lseek(fd, offset as i64, libc::SEEK_DATA) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as u64)
    }
}

/// Seek to the next hole at or after the given offset.
pub fn seek_hole(fd: i32, offset: u64) -> io::Result<u64> {
    let result = unsafe { libc::lseek(fd, offset as i64, libc::SEEK_HOLE) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as u64)
    }
}
