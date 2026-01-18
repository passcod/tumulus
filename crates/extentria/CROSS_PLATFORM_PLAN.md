# Cross-Platform Extent Support for Extentria

## Overview

This document describes the plan to extend the `extentria` crate to support multiple platforms beyond Linux. The goal is to provide a unified API for reading file extent/range information across Linux, macOS, FreeBSD, and Windows.

## Design Principles

1. **No physical offsets needed**: We only care about logical fragmentation and sparse regions, not where data lives on disk.
2. **Buffer reuse**: The current FIEMAP API allows buffer reuse between files for performance; this pattern should be preserved where applicable.
3. **Linux keeps FIEMAP**: The existing FIEMAP implementation works well and provides shared extent detection; it stays as the Linux backend.
4. **Simple file naming**: Use `platform.rs` style rather than `platform/mod.rs`.
5. **No backwards compatibility concerns**: The crate is internal, so we can refactor freely.

## Platform-Specific APIs

| Platform | API | Buffer Needed | Detects Shared |
|----------|-----|---------------|----------------|
| Linux | FIEMAP ioctl | Yes | Yes |
| macOS | SEEK_HOLE/SEEK_DATA | No | No |
| FreeBSD | SEEK_HOLE/SEEK_DATA | No | No |
| Windows | FSCTL_QUERY_ALLOCATED_RANGES | Yes | No |
| Other | Fallback (whole file) | No | No |

## File Structure

```
extentria/src/
├── lib.rs              # Top-level API, platform selection, re-exports
├── types.rs            # DataRange, RangeFlags, common types
├── fiemap.rs           # Existing Linux FIEMAP (keep as-is, used by linux.rs)
├── linux.rs            # Linux RangeReader implementation (wraps fiemap)
├── unix_seek.rs        # SEEK_HOLE/SEEK_DATA implementation (shared code)
├── macos.rs            # macOS RangeReader (uses unix_seek)
├── freebsd.rs          # FreeBSD RangeReader (uses unix_seek)
├── windows.rs          # Windows RangeReader (FSCTL_QUERY_ALLOCATED_RANGES)
└── fallback.rs         # Whole-file fallback for unknown platforms
```

## Common Types (`types.rs`)

```rust
use std::io;

/// A contiguous range of data (or sparse hole) in a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataRange {
    /// Byte offset within the file.
    pub offset: u64,
    /// Length in bytes.
    pub length: u64,
    /// Properties of this range.
    pub flags: RangeFlags,
}

/// Flags describing properties of a data range.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RangeFlags {
    /// This range is a sparse hole (no data stored, reads as zeros).
    pub sparse: bool,
    /// This range is shared with other files (reflink/dedup).
    /// Only reliably detected on Linux via FIEMAP.
    pub shared: bool,
}

impl DataRange {
    /// Create a new data range.
    pub fn new(offset: u64, length: u64) -> Self {
        Self {
            offset,
            length,
            flags: RangeFlags::default(),
        }
    }

    /// Create a sparse hole range.
    pub fn sparse(offset: u64, length: u64) -> Self {
        Self {
            offset,
            length,
            flags: RangeFlags { sparse: true, shared: false },
        }
    }

    /// The end offset (exclusive) of this range.
    pub fn end(&self) -> u64 {
        self.offset + self.length
    }
}
```

## Top-Level API (`lib.rs`)

```rust
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
mod fiemap;
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
```

## Linux Implementation (`linux.rs`)

Wraps the existing FIEMAP implementation:

```rust
use std::fs::File;
use std::io;

use crate::fiemap::{FiemapLookup, FiemapExtent};
use crate::types::{DataRange, RangeFlags};

/// Range reader for Linux using FIEMAP.
pub struct RangeReader {
    buf_size: usize,
    buf: Option<Box<[u8]>>,
}

impl RangeReader {
    /// Create a new reader with default buffer size.
    pub fn new() -> Self {
        Self {
            buf_size: 64 * 1024, // 64KB default
            buf: None,
        }
    }

    /// Create a reader with a specific buffer size.
    pub fn with_buffer_size(size: usize) -> Self {
        Self {
            buf_size: size,
            buf: None,
        }
    }

    /// Create a reader reusing an existing buffer.
    pub fn with_buffer(buf: Box<[u8]>) -> Self {
        let buf_size = buf.len();
        Self {
            buf_size,
            buf: Some(buf),
        }
    }

    /// Consume the reader and return its buffer for reuse.
    pub fn into_buffer(self) -> Option<Box<[u8]>> {
        self.buf
    }

    /// Read data ranges for a file.
    pub fn read_ranges<'a>(&'a mut self, file: &'a File) 
        -> io::Result<impl Iterator<Item = io::Result<DataRange>> + 'a> 
    {
        let file_size = file.metadata()?.len();
        
        let results = if let Some(buf) = self.buf.take() {
            FiemapLookup::for_file_size(file_size)
                .with_buf(file.as_fd(), buf)?
        } else {
            FiemapLookup::for_file_size(file_size)
                .with_buf_size(file.as_fd(), self.buf_size)?
        };

        Ok(LinuxRangeIter {
            inner: results,
            file_size,
            current_pos: 0,
            done: false,
        })
    }
}

struct LinuxRangeIter<'a> {
    inner: crate::fiemap::FiemapSearchResults<'a>,
    file_size: u64,
    current_pos: u64,
    done: bool,
}

impl Iterator for LinuxRangeIter<'_> {
    type Item = io::Result<DataRange>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            match self.inner.next() {
                Some(Ok(extent)) => {
                    // Check for sparse hole before this extent
                    if extent.logical_offset > self.current_pos {
                        let hole = DataRange::sparse(
                            self.current_pos,
                            extent.logical_offset - self.current_pos,
                        );
                        self.current_pos = extent.logical_offset;
                        return Some(Ok(hole));
                    }

                    // Return this extent as a data range
                    let range = DataRange {
                        offset: extent.logical_offset,
                        length: extent.length,
                        flags: RangeFlags {
                            sparse: false,
                            shared: extent.shared(),
                        },
                    };
                    self.current_pos = extent.logical_offset + extent.length;
                    
                    if extent.last() {
                        // Check for trailing sparse hole
                        if self.current_pos < self.file_size {
                            // We'll return this range now, then the hole next iteration
                            self.done = false;
                        } else {
                            self.done = true;
                        }
                    }
                    
                    return Some(Ok(range));
                }
                Some(Err(e)) => return Some(Err(e)),
                None => {
                    // Check for trailing sparse hole
                    if self.current_pos < self.file_size {
                        let hole = DataRange::sparse(
                            self.current_pos,
                            self.file_size - self.current_pos,
                        );
                        self.current_pos = self.file_size;
                        self.done = true;
                        return Some(Ok(hole));
                    }
                    return None;
                }
            }
        }
    }
}

impl Default for RangeReader {
    fn default() -> Self {
        Self::new()
    }
}
```

## Unix SEEK_HOLE/SEEK_DATA (`unix_seek.rs`)

Shared implementation for macOS and FreeBSD:

```rust
use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;

use crate::types::DataRange;

/// Read data ranges using SEEK_HOLE and SEEK_DATA.
///
/// Returns an iterator of data ranges. Gaps between ranges are sparse holes.
pub fn read_ranges(file: &File) -> io::Result<impl Iterator<Item = io::Result<DataRange>>> {
    let file_size = file.metadata()?.len();
    let fd = file.as_raw_fd();

    Ok(SeekRangeIter {
        fd,
        file_size,
        current_pos: 0,
        in_data: false,
        done: false,
    })
}

struct SeekRangeIter {
    fd: i32,
    file_size: u64,
    current_pos: u64,
    in_data: bool,
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
                    let hole = DataRange::sparse(
                        self.current_pos,
                        self.file_size - self.current_pos,
                    );
                    self.done = true;
                    return Some(Ok(hole));
                }
                return None;
            }
            Err(e) => return Some(Err(e)),
        };

        // If there's a hole before data, return it
        if data_start > self.current_pos {
            let hole = DataRange::sparse(self.current_pos, data_start - self.current_pos);
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

fn seek_data(fd: i32, offset: u64) -> io::Result<u64> {
    let result = unsafe { libc::lseek(fd, offset as i64, libc::SEEK_DATA) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as u64)
    }
}

fn seek_hole(fd: i32, offset: u64) -> io::Result<u64> {
    let result = unsafe { libc::lseek(fd, offset as i64, libc::SEEK_HOLE) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as u64)
    }
}
```

## macOS Implementation (`macos.rs`)

```rust
use std::fs::File;
use std::io;

use crate::types::DataRange;
use crate::unix_seek;

/// Range reader for macOS using SEEK_HOLE/SEEK_DATA.
pub struct RangeReader {
    // No state needed for seek-based approach
}

impl RangeReader {
    pub fn new() -> Self {
        Self {}
    }

    /// Buffer size is ignored on macOS (no buffer used).
    pub fn with_buffer_size(_size: usize) -> Self {
        Self::new()
    }

    /// Buffer is ignored on macOS.
    pub fn with_buffer(_buf: Box<[u8]>) -> Self {
        Self::new()
    }

    /// Returns None (no buffer used on this platform).
    pub fn into_buffer(self) -> Option<Box<[u8]>> {
        None
    }

    pub fn read_ranges(&mut self, file: &File) 
        -> io::Result<impl Iterator<Item = io::Result<DataRange>>> 
    {
        unix_seek::read_ranges(file)
    }
}

impl Default for RangeReader {
    fn default() -> Self {
        Self::new()
    }
}
```

## FreeBSD Implementation (`freebsd.rs`)

Identical to macOS (uses same `unix_seek` backend):

```rust
use std::fs::File;
use std::io;

use crate::types::DataRange;
use crate::unix_seek;

/// Range reader for FreeBSD using SEEK_HOLE/SEEK_DATA.
pub struct RangeReader {}

impl RangeReader {
    pub fn new() -> Self { Self {} }
    pub fn with_buffer_size(_size: usize) -> Self { Self::new() }
    pub fn with_buffer(_buf: Box<[u8]>) -> Self { Self::new() }
    pub fn into_buffer(self) -> Option<Box<[u8]>> { None }

    pub fn read_ranges(&mut self, file: &File) 
        -> io::Result<impl Iterator<Item = io::Result<DataRange>>> 
    {
        unix_seek::read_ranges(file)
    }
}

impl Default for RangeReader {
    fn default() -> Self { Self::new() }
}
```

## Windows Implementation (`windows.rs`)

```rust
use std::fs::File;
use std::io;
use std::os::windows::io::AsRawHandle;

use windows_sys::Win32::Storage::FileSystem::{
    FSCTL_QUERY_ALLOCATED_RANGES,
    FILE_ALLOCATED_RANGE_BUFFER,
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
            buffer: vec![unsafe { std::mem::zeroed() }; count],
        }
    }

    pub fn with_buffer(buf: Box<[u8]>) -> Self {
        // Convert byte buffer to typed buffer
        let count = buf.len() / std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
        let count = count.max(16);
        Self {
            buffer: vec![unsafe { std::mem::zeroed() }; count],
        }
    }

    pub fn into_buffer(self) -> Option<Box<[u8]>> {
        // Convert back to bytes (approximate)
        let bytes = self.buffer.len() * std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
        Some(vec![0u8; bytes].into_boxed_slice())
    }

    pub fn read_ranges(&mut self, file: &File) 
        -> io::Result<impl Iterator<Item = io::Result<DataRange>>> 
    {
        let file_size = file.metadata()?.len();
        let handle = file.as_raw_handle();

        let mut ranges = Vec::new();
        let mut query_offset = 0u64;

        while query_offset < file_size {
            let input = FILE_ALLOCATED_RANGE_BUFFER {
                FileOffset: query_offset as i64,
                Length: (file_size - query_offset) as i64,
            };

            let mut bytes_returned: u32 = 0;
            let buffer_size = (self.buffer.len() * std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>()) as u32;

            let result = unsafe {
                DeviceIoControl(
                    handle as _,
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
                // ERROR_MORE_DATA means buffer was too small, but we got some results
                if err.raw_os_error() != Some(234) { // ERROR_MORE_DATA
                    return Err(err);
                }
            }

            let count = bytes_returned as usize / std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
            
            if count == 0 {
                break;
            }

            let mut current_pos = query_offset;
            for i in 0..count {
                let range = &self.buffer[i];
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
```

## Fallback Implementation (`fallback.rs`)

```rust
use std::fs::File;
use std::io;

use crate::types::DataRange;

/// Fallback range reader that treats the whole file as one extent.
pub struct RangeReader {}

impl RangeReader {
    pub fn new() -> Self { Self {} }
    pub fn with_buffer_size(_size: usize) -> Self { Self::new() }
    pub fn with_buffer(_buf: Box<[u8]>) -> Self { Self::new() }
    pub fn into_buffer(self) -> Option<Box<[u8]>> { None }

    pub fn read_ranges(&mut self, file: &File) 
        -> io::Result<impl Iterator<Item = io::Result<DataRange>>> 
    {
        let len = file.metadata()?.len();
        let range = if len > 0 {
            Some(DataRange::new(0, len))
        } else {
            None
        };
        Ok(range.into_iter().map(Ok))
    }
}

impl Default for RangeReader {
    fn default() -> Self { Self::new() }
}
```

## Cargo.toml Updates

```toml
[package]
name = "extentria"
version = "0.1.0"
edition = "2024"

[dependencies]
libc = "0.2"
zerocopy = "0.8"
zerocopy-derive = "0.8"

# Linux-specific
[target.'cfg(target_os = "linux")'.dependencies]
linux-raw-sys = { version = "0.9", features = ["ioctl"] }

# Windows-specific  
[target.'cfg(target_os = "windows")'.dependencies]
windows-sys = { version = "0.59", features = [
    "Win32_Storage_FileSystem",
    "Win32_System_IO",
    "Win32_Foundation",
] }
```

## Implementation Phases

### Phase 1: Refactor Types
1. Create `types.rs` with `DataRange`, `RangeFlags`
2. Update `lib.rs` with new module structure (but only Linux working initially)
3. Create `linux.rs` wrapping existing FIEMAP
4. Verify existing tumulus code still works

### Phase 2: Unix Seek Implementation
1. Create `unix_seek.rs` with SEEK_HOLE/SEEK_DATA
2. Create `macos.rs` using unix_seek
3. Create `freebsd.rs` using unix_seek
4. Test on macOS (CI)

### Phase 3: Windows Implementation
1. Add `windows-sys` dependency
2. Create `windows.rs` with FSCTL_QUERY_ALLOCATED_RANGES
3. Test on Windows (CI)

### Phase 4: Fallback and Polish
1. Create `fallback.rs`
2. Update tumulus to use new API
3. Documentation
4. CI for all platforms

## Testing Strategy

### Unit Tests (each platform module)
- Empty file returns empty/single-zero-length range
- Regular file returns expected ranges
- Sparse file returns data + sparse ranges correctly
- Error handling for invalid file descriptors

### Integration Tests
- Create sparse files programmatically, verify detection
- Create regular files, verify single range
- On Linux: create reflinked files, verify shared detection

### CI Matrix
```yaml
strategy:
  matrix:
    os: [ubuntu-latest, macos-latest, windows-latest]
```

## Migration for Tumulus

Current usage in `tumulus/crates/tumulus/src/extents.rs`:
```rust
use extentria::fiemap::FiemapLookup;

let extent_results: Result<Vec<_>, _> = FiemapLookup::extents_for_file(&file)?
    .map(|r| r.map(|extent| {
        // Use extent.logical_offset, extent.length
    }))
    .collect();
```

New usage:
```rust
use extentria::{RangeReader, DataRange};

let mut reader = RangeReader::new();
let ranges: Result<Vec<_>, _> = reader.read_ranges(&file)?
    .map(|r| r.map(|range| {
        // Use range.offset, range.length, range.flags
    }))
    .collect();
```

Or with the convenience function:
```rust
use extentria::ranges_for_file;

let ranges = ranges_for_file(&file)?;
```
