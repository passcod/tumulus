//! Cross-platform file extent/range information.
//!
//! This crate provides a unified API for reading how files are laid out
//! on disk, including detection of sparse holes and (on Linux) shared extents.

use std::fs::File;
use std::io;

mod types;
pub use types::{DataRange, RangeFlags};

// Platform-specific implementations
#[cfg(target_os = "linux")]
pub mod fiemap;
#[cfg(target_os = "linux")]
mod linux;

#[cfg(any(target_os = "macos", target_os = "freebsd"))]
mod unix_seek;

#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "freebsd")]
mod freebsd;

#[cfg(target_os = "windows")]
mod windows;

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    target_os = "windows"
)))]
mod fallback;

// Re-export the appropriate RangeReader
#[cfg(target_os = "linux")]
pub use linux::RangeReader;

#[cfg(target_os = "macos")]
pub use macos::RangeReader;

#[cfg(target_os = "freebsd")]
pub use freebsd::RangeReader;

#[cfg(target_os = "windows")]
pub use windows::RangeReader;

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    target_os = "windows"
)))]
pub use fallback::RangeReader;

/// Convenience function: get data ranges for a file using default settings.
///
/// For processing multiple files, consider using [`RangeReader`] directly
/// to reuse buffers between calls.
pub fn ranges_for_file(file: &File) -> io::Result<Vec<DataRange>> {
    let mut reader = RangeReader::new();
    reader.read_ranges(file)?.collect()
}

/// Returns true if this platform can detect shared/reflinked extents.
pub const fn can_detect_shared() -> bool {
    cfg!(target_os = "linux")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Check if an error indicates the filesystem doesn't support extent queries.
    /// This can happen on tmpfs, some network filesystems, etc.
    fn is_unsupported_error(err: &io::Error) -> bool {
        // EOPNOTSUPP = 95 on Linux, ENOTTY = 25 on Linux, EINVAL = 22 on Linux
        // EINVAL can happen on some filesystems that don't properly support FIEMAP
        // On Windows, we might get ERROR_NOT_SUPPORTED = 50
        #[cfg(unix)]
        {
            matches!(
                err.raw_os_error(),
                Some(libc::EOPNOTSUPP) | Some(libc::ENOTTY) | Some(libc::EINVAL)
            )
        }
        #[cfg(windows)]
        {
            matches!(err.raw_os_error(), Some(50)) // ERROR_NOT_SUPPORTED
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = err;
            false
        }
    }

    #[test]
    fn test_empty_file() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let file = temp.as_file();

        match ranges_for_file(file) {
            Ok(ranges) => {
                // Empty file should have no ranges or a single zero-length range
                assert!(ranges.is_empty() || ranges.iter().all(|r| r.length == 0));
            }
            Err(e) if is_unsupported_error(&e) => {
                // Skip test on filesystems that don't support extent queries
                eprintln!("Skipping test: filesystem doesn't support extent queries");
            }
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    #[test]
    fn test_regular_file() {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        temp.write_all(b"Hello, world!").unwrap();
        temp.flush().unwrap();

        let file = temp.as_file();
        match ranges_for_file(file) {
            Ok(ranges) => {
                // Regular file should have at least one data range
                assert!(!ranges.is_empty());

                // Total length should cover the file size
                // (may be larger due to filesystem block/cluster alignment)
                let total_len: u64 = ranges.iter().map(|r| r.length).sum();
                assert!(
                    total_len >= 13,
                    "total length {total_len} should be >= file size 13"
                );
            }
            Err(e) if is_unsupported_error(&e) => {
                // Skip test on filesystems that don't support extent queries
                eprintln!("Skipping test: filesystem doesn't support extent queries");
            }
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    #[test]
    fn test_range_reader_reuse() {
        let mut temp1 = tempfile::NamedTempFile::new().unwrap();
        temp1.write_all(b"File one").unwrap();
        temp1.flush().unwrap();

        let mut temp2 = tempfile::NamedTempFile::new().unwrap();
        temp2.write_all(b"File two").unwrap();
        temp2.flush().unwrap();

        let mut reader = RangeReader::new();

        match reader.read_ranges(temp1.as_file()) {
            Ok(iter) => {
                let ranges1: Vec<_> = iter.collect();
                assert!(!ranges1.is_empty());
            }
            Err(e) if is_unsupported_error(&e) => {
                eprintln!("Skipping test: filesystem doesn't support extent queries");
                return;
            }
            Err(e) => panic!("Unexpected error: {e}"),
        }

        match reader.read_ranges(temp2.as_file()) {
            Ok(iter) => {
                let ranges2: Vec<_> = iter.collect();
                assert!(!ranges2.is_empty());
            }
            Err(e) if is_unsupported_error(&e) => {
                eprintln!("Skipping test: filesystem doesn't support extent queries");
            }
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    #[test]
    fn test_can_detect_shared_const() {
        // Just verify this is a const fn and returns a bool
        const _: bool = can_detect_shared();
    }

    #[test]
    fn test_data_range_methods() {
        let range = DataRange::new(100, 50);
        assert_eq!(range.offset, 100);
        assert_eq!(range.length, 50);
        assert_eq!(range.end(), 150);
        assert!(!range.flags.sparse);
        assert!(!range.flags.shared);

        let sparse = DataRange::sparse(200, 100);
        assert_eq!(sparse.offset, 200);
        assert_eq!(sparse.length, 100);
        assert_eq!(sparse.end(), 300);
        assert!(sparse.flags.sparse);
        assert!(!sparse.flags.shared);
    }
}
