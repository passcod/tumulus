//! Blake3 ID type for content-addressed identifiers.
//!
//! This module provides the `B3Id` type, a newtype wrapper around `blake3::Hash`
//! used for extent IDs, blob IDs, and other content-addressed identifiers.

use std::{array::TryFromSliceError, ops::Deref};

/// Newtype for blake3 hashes used as IDs (extent IDs, blob IDs, etc.)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct B3Id(pub blake3::Hash);

impl B3Id {
    /// Create a B3Id by hashing the given data.
    pub fn hash(data: &[u8]) -> Self {
        Self(blake3::hash(data))
    }

    /// Get the underlying bytes as a slice.
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_bytes().as_slice()
    }

    /// Get the hex-encoded representation of this ID.
    pub fn as_hex(&self) -> String {
        self.0.to_hex().to_string()
    }
}

impl AsRef<[u8]> for B3Id {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes().as_slice()
    }
}

impl Deref for B3Id {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        self.0.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for B3Id {
    type Error = TryFromSliceError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(blake3::Hash::from_bytes(bytes.as_slice().try_into()?)))
    }
}

impl From<[u8; 32]> for B3Id {
    fn from(value: [u8; 32]) -> Self {
        B3Id(blake3::Hash::from_bytes(value))
    }
}

impl From<blake3::Hash> for B3Id {
    fn from(value: blake3::Hash) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for B3Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_hex() {
        let id = B3Id::hash(b"hello world");
        let hex = id.as_hex();
        assert_eq!(hex.len(), 64); // 32 bytes = 64 hex chars
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn display_matches_as_hex() {
        let id = B3Id::hash(b"test data");
        assert_eq!(id.to_string(), id.as_hex());
    }

    #[test]
    fn from_bytes() {
        let bytes = [0x42u8; 32];
        let id = B3Id::from(bytes);
        assert_eq!(*id, bytes);
    }

    #[test]
    fn try_from_vec() {
        let bytes = vec![0x42u8; 32];
        let id = B3Id::try_from(bytes).unwrap();
        assert_eq!(*id, [0x42u8; 32]);

        // Wrong size should fail
        let bad_bytes = vec![0x42u8; 16];
        assert!(B3Id::try_from(bad_bytes).is_err());
    }

    #[test]
    fn as_slice() {
        let id = B3Id::hash(b"slice test");
        let slice = id.as_slice();
        assert_eq!(slice.len(), 32);
    }
}
