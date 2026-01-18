//! Compression utilities for catalog files.
//!
//! Provides functions to compress and decompress catalog files using zstd,
//! as well as utilities to open catalogs that may or may not be compressed.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use rusqlite::Connection;
use tempfile::NamedTempFile;
use tracing::debug;

/// The magic bytes at the start of a zstd compressed file.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Default compression level for zstd (1-22, higher = better compression but slower).
pub const DEFAULT_COMPRESSION_LEVEL: i32 = 19;

/// Check if a file is zstd compressed by reading its magic bytes.
pub fn is_zstd_compressed(path: &Path) -> io::Result<bool> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    match file.read_exact(&mut magic) {
        Ok(()) => Ok(magic == ZSTD_MAGIC),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

/// Compress a file using zstd.
///
/// Reads from `input_path` and writes compressed data to `output_path`.
/// Uses the default compression level.
pub fn compress_file(input_path: &Path, output_path: &Path) -> io::Result<()> {
    compress_file_with_level(input_path, output_path, DEFAULT_COMPRESSION_LEVEL)
}

/// Compress a file using zstd with a specific compression level.
///
/// Reads from `input_path` and writes compressed data to `output_path`.
pub fn compress_file_with_level(
    input_path: &Path,
    output_path: &Path,
    level: i32,
) -> io::Result<()> {
    debug!(?input_path, ?output_path, level, "Compressing file");

    let input_file = File::open(input_path)?;
    let input_reader = BufReader::new(input_file);

    let output_file = File::create(output_path)?;
    let output_writer = BufWriter::new(output_file);

    let mut encoder = zstd::stream::Encoder::new(output_writer, level)?;
    io::copy(&mut BufReader::new(input_reader), &mut encoder)?;
    encoder.finish()?;

    Ok(())
}

/// Decompress a zstd compressed file.
///
/// Reads from `input_path` and writes decompressed data to `output_path`.
pub fn decompress_file(input_path: &Path, output_path: &Path) -> io::Result<()> {
    debug!(?input_path, ?output_path, "Decompressing file");

    let input_file = File::open(input_path)?;
    let input_reader = BufReader::new(input_file);

    let output_file = File::create(output_path)?;
    let mut output_writer = BufWriter::new(output_file);

    let mut decoder = zstd::stream::Decoder::new(input_reader)?;
    io::copy(&mut decoder, &mut output_writer)?;
    output_writer.flush()?;

    Ok(())
}

/// Decompress a zstd compressed file to a temporary file.
///
/// Returns the temporary file handle. The file will be deleted when the handle is dropped.
pub fn decompress_to_tempfile(input_path: &Path) -> io::Result<NamedTempFile> {
    debug!(?input_path, "Decompressing to temporary file");

    let input_file = File::open(input_path)?;
    let input_reader = BufReader::new(input_file);

    let mut temp_file = NamedTempFile::new()?;
    let mut decoder = zstd::stream::Decoder::new(input_reader)?;
    io::copy(&mut decoder, &mut temp_file)?;
    temp_file.flush()?;

    Ok(temp_file)
}

/// Open a catalog database, automatically decompressing if necessary.
///
/// If the file is zstd compressed, it will be decompressed to a temporary file
/// and that file will be opened. The temporary file handle is returned along
/// with the connection so that it stays alive for the duration of use.
///
/// Returns `(Connection, Option<NamedTempFile>)` - the tempfile must be kept alive
/// as long as the connection is in use.
pub fn open_catalog(path: &Path) -> io::Result<(Connection, Option<NamedTempFile>)> {
    if is_zstd_compressed(path)? {
        debug!(?path, "Opening compressed catalog");
        let temp_file = decompress_to_tempfile(path)?;
        let conn = Connection::open(temp_file.path()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to open decompressed catalog: {}", e),
            )
        })?;
        Ok((conn, Some(temp_file)))
    } else {
        debug!(?path, "Opening uncompressed catalog");
        let conn = Connection::open(path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to open catalog: {}", e),
            )
        })?;
        Ok((conn, None))
    }
}

/// Compress a catalog file in-place.
///
/// The original file is replaced with the compressed version.
pub fn compress_catalog_in_place(path: &Path) -> io::Result<()> {
    let temp_output = NamedTempFile::new_in(path.parent().unwrap_or(Path::new(".")))?;
    compress_file(path, temp_output.path())?;
    temp_output.persist(path).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_is_zstd_compressed() {
        // Create a temp file with zstd magic
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(&ZSTD_MAGIC).unwrap();
        temp.write_all(b"some data").unwrap();
        temp.flush().unwrap();
        assert!(is_zstd_compressed(temp.path()).unwrap());

        // Create a temp file without zstd magic
        let mut temp2 = NamedTempFile::new().unwrap();
        temp2.write_all(b"not compressed").unwrap();
        temp2.flush().unwrap();
        assert!(!is_zstd_compressed(temp2.path()).unwrap());
    }

    #[test]
    fn test_compress_decompress_roundtrip() {
        let original_data = b"Hello, this is test data for compression!";

        // Create original file
        let mut original = NamedTempFile::new().unwrap();
        original.write_all(original_data).unwrap();
        original.flush().unwrap();

        // Compress
        let compressed = NamedTempFile::new().unwrap();
        compress_file(original.path(), compressed.path()).unwrap();

        // Verify it's compressed
        assert!(is_zstd_compressed(compressed.path()).unwrap());

        // Decompress
        let decompressed = NamedTempFile::new().unwrap();
        decompress_file(compressed.path(), decompressed.path()).unwrap();

        // Verify content matches
        let mut result = Vec::new();
        File::open(decompressed.path())
            .unwrap()
            .read_to_end(&mut result)
            .unwrap();
        assert_eq!(result, original_data);
    }
}
