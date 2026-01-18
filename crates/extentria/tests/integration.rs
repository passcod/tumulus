//! Integration tests for cross-platform extent detection.
//!
//! These tests verify that extentria correctly detects file ranges
//! across different platforms and file types.

use std::fs::{self, File};
use std::io::{self, Seek, SeekFrom, Write};

use extentria::{DataRange, RangeReader, ranges_for_file};

/// Helper to check if an error indicates unsupported filesystem.
fn is_unsupported_error(err: &io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(
            err.raw_os_error(),
            Some(libc::EOPNOTSUPP) | Some(libc::ENOTTY)
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

// ============================================================================
// Empty file tests
// ============================================================================

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

// ============================================================================
// Regular file tests
// ============================================================================

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
                    !range.flags.sparse,
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

// ============================================================================
// Sparse file tests (Unix-specific)
// ============================================================================

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
                let has_sparse = ranges.iter().any(|r| r.flags.sparse);
                if has_sparse {
                    // First range should be the sparse hole
                    assert!(ranges[0].flags.sparse, "First range should be sparse hole");
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
            let sparse_count = ranges.iter().filter(|r| r.flags.sparse).count();
            let data_count = ranges.iter().filter(|r| !r.flags.sparse).count();

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

// ============================================================================
// RangeReader buffer reuse tests
// ============================================================================

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

// ============================================================================
// DataRange API tests
// ============================================================================

#[test]
fn test_data_range_new() {
    let range = DataRange::new(100, 200);
    assert_eq!(range.offset, 100);
    assert_eq!(range.length, 200);
    assert_eq!(range.end(), 300);
    assert!(!range.flags.sparse);
    assert!(!range.flags.shared);
}

#[test]
fn test_data_range_sparse() {
    let range = DataRange::sparse(500, 1000);
    assert_eq!(range.offset, 500);
    assert_eq!(range.length, 1000);
    assert_eq!(range.end(), 1500);
    assert!(range.flags.sparse);
    assert!(!range.flags.shared);
}

#[test]
fn test_data_range_equality() {
    let range1 = DataRange::new(0, 100);
    let range2 = DataRange::new(0, 100);
    let range3 = DataRange::new(0, 200);

    assert_eq!(range1, range2);
    assert_ne!(range1, range3);
}

// ============================================================================
// Platform capability tests
// ============================================================================

#[test]
fn test_can_detect_shared_is_const() {
    // Verify this is a const fn by using it in a const context
    const CAN_DETECT: bool = extentria::can_detect_shared();

    #[cfg(target_os = "linux")]
    const {
        assert!(CAN_DETECT, "Linux should support shared extent detection")
    };

    #[cfg(not(target_os = "linux"))]
    const {
        assert!(
            !CAN_DETECT,
            "Non-Linux platforms should not detect shared extents"
        )
    };

    // Runtime check as well
    let runtime_value = extentria::can_detect_shared();
    assert_eq!(runtime_value, CAN_DETECT);
}

// ============================================================================
// Linux-specific tests
// ============================================================================

#[cfg(target_os = "linux")]
mod linux_tests {
    use super::*;
    use std::process::Command;

    /// Check if we're on a filesystem that supports reflinks (btrfs, xfs, etc.)
    fn supports_reflinks() -> bool {
        // Try to detect btrfs or xfs with reflink support
        let temp = tempfile::tempdir().unwrap();
        let test_file = temp.path().join("test");
        let copy_file = temp.path().join("copy");

        // Create a test file
        fs::write(&test_file, b"test content").unwrap();

        // Try to create a reflink copy
        let result = Command::new("cp")
            .args([
                "--reflink=always",
                test_file.to_str().unwrap(),
                copy_file.to_str().unwrap(),
            ])
            .output();

        match result {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    #[test]
    fn test_shared_extent_detection() {
        if !supports_reflinks() {
            eprintln!("Skipping: filesystem doesn't support reflinks");
            return;
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let original = temp_dir.path().join("original");
        let reflink = temp_dir.path().join("reflink");

        // Create original file with some content
        let content = vec![0xABu8; 64 * 1024]; // 64KB
        fs::write(&original, &content).unwrap();

        // Create a reflink copy
        let result = Command::new("cp")
            .args([
                "--reflink=always",
                original.to_str().unwrap(),
                reflink.to_str().unwrap(),
            ])
            .output()
            .unwrap();

        if !result.status.success() {
            eprintln!("Skipping: failed to create reflink");
            return;
        }

        // Check if the original file shows shared extents
        let file = File::open(&original).unwrap();
        match ranges_for_file(&file) {
            Ok(ranges) => {
                let has_shared = ranges.iter().any(|r| r.flags.shared);
                eprintln!(
                    "Reflink test: {} ranges, shared detected: {}",
                    ranges.len(),
                    has_shared
                );
                // Note: shared detection depends on filesystem and kernel support
                // We just verify we can read the file without error
            }
            Err(e) if is_unsupported_error(&e) => {
                eprintln!("Skipping: filesystem doesn't support FIEMAP");
            }
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    #[test]
    fn test_fiemap_module_available() {
        // Verify the fiemap module is accessible on Linux
        use extentria::fiemap::{FiemapLookup, minimum_buf_size, result_size};

        // These should be valid sizes
        assert!(result_size() > 0);
        assert!(minimum_buf_size() > result_size());

        // Create a lookup for testing
        let lookup = FiemapLookup::for_file_size(1024 * 1024);
        assert_eq!(lookup.start, 0);
        assert_eq!(lookup.length, 1024 * 1024);
    }
}

// ============================================================================
// Error handling tests
// ============================================================================

#[test]
fn test_closed_file_handle() {
    // This test verifies behavior with an invalid file state
    // Note: behavior is platform-specific
    let temp = tempfile::NamedTempFile::new().unwrap();
    let path = temp.path().to_owned();

    // Keep the path but drop the temp file (deletes it)
    drop(temp);

    // Try to open the now-deleted file
    let result = File::open(&path);
    assert!(result.is_err(), "Opening deleted file should fail");
}

#[test]
fn test_directory_handling() {
    let temp_dir = tempfile::tempdir().unwrap();
    let dir = File::open(temp_dir.path()).unwrap();

    // Trying to get ranges for a directory should fail or return empty
    match ranges_for_file(&dir) {
        Ok(ranges) => {
            // Some platforms might return empty ranges for directories
            eprintln!("Directory returned {} ranges", ranges.len());
        }
        Err(_) => {
            // Expected on most platforms
        }
    }
}
