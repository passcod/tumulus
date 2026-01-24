//! Integration tests for cross-platform extent detection.
//!
//! These tests verify that extentria correctly detects file ranges
//! across different platforms and file types.

use std::fs::{self, File};
use std::io::{self, Seek, SeekFrom, Write};

use extentria::{RangeReader, RangeReaderImpl, ranges_for_file};

/// Helper to check if an error indicates unsupported filesystem.
fn is_unsupported_error(err: &io::Error) -> bool {
    #[cfg(unix)]
    {
        // EOPNOTSUPP = 95, ENOTTY = 25, EINVAL = 22 on Linux
        // EINVAL can happen on some filesystems that don't properly support FIEMAP
        matches!(
            err.raw_os_error(),
            Some(libc::EOPNOTSUPP) | Some(libc::ENOTTY) | Some(libc::EINVAL)
        )
    }
    #[cfg(windows)]
    {
        // ERROR_NOT_SUPPORTED = 50, ERROR_INVALID_FUNCTION = 1
        matches!(err.raw_os_error(), Some(50) | Some(1))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = err;
        false
    }
}

#[test]
fn test_empty_file_returns_no_ranges() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let file = temp.as_file();

    match ranges_for_file(file) {
        Ok(ranges) => {
            // Empty file should have no ranges
            assert!(
                ranges.is_empty(),
                "Expected no ranges for empty file, got {:?}",
                ranges
            );
        }
        Err(e) if is_unsupported_error(&e) => {
            eprintln!("Skipping: filesystem doesn't support extent queries");
        }
        Err(e) => panic!("Unexpected error: {e}"),
    }
}

#[test]
fn test_regular_file_returns_single_range() {
    let mut temp = tempfile::NamedTempFile::new().unwrap();
    let content = b"Hello, world! This is test content for extent detection.";
    temp.write_all(content).unwrap();
    temp.flush().unwrap();

    let file = temp.as_file();

    match ranges_for_file(file) {
        Ok(ranges) => {
            // Regular file should have at least one range
            assert!(!ranges.is_empty(), "Expected at least one range");

            // Total length should match file size
            let total_len: u64 = ranges.iter().map(|r| r.length).sum();
            assert_eq!(
                total_len,
                content.len() as u64,
                "Total range length should match file size"
            );

            // First range should start at offset 0
            assert_eq!(ranges[0].offset, 0, "First range should start at offset 0");

            // No ranges should be sparse for a regular written file
            for range in &ranges {
                assert!(
                    !range.hole,
                    "Regular file should not have sparse ranges: {:?}",
                    range
                );
            }
        }
        Err(e) if is_unsupported_error(&e) => {
            eprintln!("Skipping: filesystem doesn't support extent queries");
        }
        Err(e) => panic!("Unexpected error: {e}"),
    }
}

#[test]
fn test_larger_file_coverage() {
    let mut temp = tempfile::NamedTempFile::new().unwrap();

    // Write 1MB of data
    let chunk = vec![0xABu8; 64 * 1024];
    for _ in 0..16 {
        temp.write_all(&chunk).unwrap();
    }
    temp.flush().unwrap();

    let file = temp.as_file();
    let expected_size = 16 * 64 * 1024;

    match ranges_for_file(file) {
        Ok(ranges) => {
            assert!(!ranges.is_empty(), "Expected at least one range");

            // Total length should match file size
            let total_len: u64 = ranges.iter().map(|r| r.length).sum();
            assert_eq!(
                total_len, expected_size,
                "Total range length should match file size"
            );

            // Ranges should be contiguous and cover the whole file
            let mut expected_offset = 0u64;
            for range in &ranges {
                assert_eq!(range.offset, expected_offset, "Ranges should be contiguous");
                expected_offset = range.offset + range.length;
            }
            assert_eq!(
                expected_offset, expected_size,
                "Ranges should cover entire file"
            );
        }
        Err(e) if is_unsupported_error(&e) => {
            eprintln!("Skipping: filesystem doesn't support extent queries");
        }
        Err(e) => panic!("Unexpected error: {e}"),
    }
}

#[cfg(unix)]
#[test]
fn test_sparse_file_detection() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut file = temp.reopen().unwrap();

    // Create a sparse file by seeking past the end and writing
    // This creates a hole at the beginning
    let hole_size = 1024 * 1024; // 1MB hole
    let data = b"Data after the hole";

    file.seek(SeekFrom::Start(hole_size)).unwrap();
    file.write_all(data).unwrap();
    file.flush().unwrap();

    // Reopen for reading
    let file = temp.as_file();
    let expected_size = hole_size + data.len() as u64;

    match ranges_for_file(file) {
        Ok(ranges) => {
            // Total length should match file size
            let total_len: u64 = ranges.iter().map(|r| r.length).sum();
            assert_eq!(
                total_len, expected_size,
                "Total range length should match file size"
            );

            // Should have at least 2 ranges: a sparse hole and data
            // Note: some filesystems might not report the hole as sparse
            if ranges.len() >= 2 {
                // Check if we detected the sparse hole
                let has_sparse = ranges.iter().any(|r| r.hole);
                if has_sparse {
                    // First range should be the sparse hole
                    assert!(ranges[0].hole, "First range should be sparse hole");
                    assert_eq!(ranges[0].offset, 0, "Sparse hole should start at 0");
                }
            }

            // Verify ranges are contiguous
            let mut expected_offset = 0u64;
            for range in &ranges {
                assert_eq!(range.offset, expected_offset, "Ranges should be contiguous");
                expected_offset = range.offset + range.length;
            }
        }
        Err(e) if is_unsupported_error(&e) => {
            eprintln!("Skipping: filesystem doesn't support extent queries");
        }
        Err(e) => panic!("Unexpected error: {e}"),
    }
}

#[cfg(unix)]
#[test]
fn test_sparse_file_with_multiple_holes() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut file = temp.reopen().unwrap();

    // Create a file with pattern: data, hole, data, hole, data
    let chunk_size = 64 * 1024u64; // 64KB chunks
    let data = vec![0xABu8; chunk_size as usize];

    // Write first data chunk
    file.write_all(&data).unwrap();

    // Skip (hole)
    file.seek(SeekFrom::Current(chunk_size as i64)).unwrap();

    // Write second data chunk
    file.write_all(&data).unwrap();

    // Skip (hole)
    file.seek(SeekFrom::Current(chunk_size as i64)).unwrap();

    // Write third data chunk
    file.write_all(&data).unwrap();

    file.flush().unwrap();

    let file = temp.as_file();
    let expected_size = chunk_size * 5; // 3 data + 2 holes

    match ranges_for_file(file) {
        Ok(ranges) => {
            // Total length should match file size
            let total_len: u64 = ranges.iter().map(|r| r.length).sum();
            assert_eq!(
                total_len, expected_size,
                "Total range length should match file size"
            );

            // Count sparse vs data ranges
            let sparse_count = ranges.iter().filter(|r| r.hole).count();
            let data_count = ranges.iter().filter(|r| !r.hole).count();

            eprintln!(
                "Sparse file test: {} total ranges, {} sparse, {} data",
                ranges.len(),
                sparse_count,
                data_count
            );

            // We expect some data ranges at minimum
            assert!(data_count >= 1, "Should have at least one data range");
        }
        Err(e) if is_unsupported_error(&e) => {
            eprintln!("Skipping: filesystem doesn't support extent queries");
        }
        Err(e) => panic!("Unexpected error: {e}"),
    }
}

#[test]
fn test_range_reader_reuse_across_files() {
    let mut reader = RangeReader::new();

    // Create and read first file
    let mut temp1 = tempfile::NamedTempFile::new().unwrap();
    temp1.write_all(b"First file content").unwrap();
    temp1.flush().unwrap();

    {
        let result1 = reader.read_ranges(temp1.as_file());
        match result1 {
            Ok(iter) => {
                let ranges1: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
                assert!(!ranges1.is_empty());
            }
            Err(e) if is_unsupported_error(&e) => {
                eprintln!("Skipping: filesystem doesn't support extent queries");
                return;
            }
            Err(e) => panic!("Unexpected error on first file: {e}"),
        }
    }

    // Create and read second file with same reader
    let mut temp2 = tempfile::NamedTempFile::new().unwrap();
    temp2
        .write_all(b"Second file with different content")
        .unwrap();
    temp2.flush().unwrap();

    {
        let result2 = reader.read_ranges(temp2.as_file());
        match result2 {
            Ok(iter) => {
                let ranges2: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
                assert!(!ranges2.is_empty());
            }
            Err(e) => panic!("Unexpected error on second file: {e}"),
        }
    }

    // Create and read third file
    let mut temp3 = tempfile::NamedTempFile::new().unwrap();
    temp3.write_all(b"Third").unwrap();
    temp3.flush().unwrap();

    {
        let result3 = reader.read_ranges(temp3.as_file());
        match result3 {
            Ok(iter) => {
                let ranges3: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
                assert!(!ranges3.is_empty());
            }
            Err(e) => panic!("Unexpected error on third file: {e}"),
        }
    }
}

#[test]
fn test_range_reader_with_custom_buffer_size() {
    let mut reader = RangeReader::with_buffer_size(128 * 1024); // 128KB buffer

    let mut temp = tempfile::NamedTempFile::new().unwrap();
    temp.write_all(b"Test content").unwrap();
    temp.flush().unwrap();

    match reader.read_ranges(temp.as_file()) {
        Ok(iter) => {
            let ranges: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
            assert!(!ranges.is_empty());
        }
        Err(e) if is_unsupported_error(&e) => {
            eprintln!("Skipping: filesystem doesn't support extent queries");
        }
        Err(e) => panic!("Unexpected error: {e}"),
    }
}

#[cfg(target_os = "linux")]
mod fallback_tests {
    use super::*;

    /// Test that files on tmpfs (which doesn't support FIEMAP) fall back correctly.
    ///
    /// This test creates a file in /tmp (typically tmpfs on Linux) and verifies
    /// that we can still read its ranges even though FIEMAP isn't supported.
    #[test]
    fn tmpfs_fallback() {
        // /tmp is typically tmpfs on Linux
        let temp_dir = tempfile::Builder::new()
            .prefix("extentria-test-")
            .tempdir_in("/tmp")
            .unwrap();

        let test_file = temp_dir.path().join("test.txt");
        let content = b"Hello, tmpfs fallback test!";
        fs::write(&test_file, content).unwrap();

        let file = File::open(&test_file).unwrap();
        let mut reader = RangeReader::new();

        // This should now succeed via fallback instead of failing with EOPNOTSUPP
        let result = reader.read_ranges(&file);
        assert!(
            result.is_ok(),
            "read_ranges should succeed on tmpfs via fallback"
        );

        let ranges: Result<Vec<_>, _> = result.unwrap().collect();
        assert!(ranges.is_ok(), "Iterator should not produce errors");

        let ranges = ranges.unwrap();
        assert_eq!(
            ranges.len(),
            1,
            "Should have exactly one range (fallback treats file as single extent)"
        );

        let range = &ranges[0];
        assert_eq!(range.offset, 0, "Range should start at offset 0");
        assert_eq!(
            range.length,
            content.len() as u64,
            "Range length should match file size"
        );
        assert!(!range.hole, "Fallback range should not be marked sparse");
    }

    /// Test that the fallback works correctly for empty files on tmpfs.
    #[test]
    fn tmpfs_fallback_empty_file() {
        let temp_dir = tempfile::Builder::new()
            .prefix("extentria-test-")
            .tempdir_in("/tmp")
            .unwrap();

        let test_file = temp_dir.path().join("empty.txt");
        fs::write(&test_file, b"").unwrap();

        let file = File::open(&test_file).unwrap();
        let mut reader = RangeReader::new();

        match reader.read_ranges(&file) {
            Ok(iter) => {
                let ranges: Result<Vec<_>, _> = iter.collect();
                assert!(ranges.is_ok(), "Iterator should not produce errors");

                let ranges = ranges.unwrap();
                assert!(ranges.is_empty(), "Empty file should have no ranges");
            }
            Err(e) if is_unsupported_error(&e) => {
                eprintln!("Skipping: filesystem doesn't support extent queries");
            }
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    /// Test that RangeReader can be reused across files on tmpfs.
    #[test]
    fn tmpfs_fallback_reader_reuse() {
        let temp_dir = tempfile::Builder::new()
            .prefix("extentria-test-")
            .tempdir_in("/tmp")
            .unwrap();

        let file1_path = temp_dir.path().join("file1.txt");
        let file2_path = temp_dir.path().join("file2.txt");
        fs::write(&file1_path, b"First file content").unwrap();
        fs::write(&file2_path, b"Second file").unwrap();

        let mut reader = RangeReader::new();

        // Read first file
        let file1 = File::open(&file1_path).unwrap();
        let result1 = reader.read_ranges(&file1);
        assert!(result1.is_ok());
        let ranges1: Vec<_> = result1.unwrap().filter_map(|r| r.ok()).collect();
        assert_eq!(ranges1.len(), 1);
        assert_eq!(ranges1[0].length, 18); // "First file content"

        // Read second file with same reader
        let file2 = File::open(&file2_path).unwrap();
        let result2 = reader.read_ranges(&file2);
        assert!(result2.is_ok());
        let ranges2: Vec<_> = result2.unwrap().filter_map(|r| r.ok()).collect();
        assert_eq!(ranges2.len(), 1);
        assert_eq!(ranges2[0].length, 11); // "Second file"
    }
}
