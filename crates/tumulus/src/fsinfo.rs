//! Filesystem information utilities.
//!
//! Provides functions to get filesystem type, UUID, and hostname.

use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use nix::sys::statfs::statfs;

/// Information about a filesystem.
#[derive(Debug, Clone)]
pub struct FsInfo {
    /// The filesystem type (e.g., "btrfs", "ext4", "xfs")
    pub fs_type: Option<String>,
    /// The filesystem UUID if available
    pub fs_id: Option<String>,
}

/// Get the hostname of the current machine.
pub fn get_hostname() -> Option<String> {
    hostname::get().ok().and_then(|h| h.into_string().ok())
}

/// Get filesystem information for a path.
pub fn get_fs_info(path: &Path) -> io::Result<FsInfo> {
    let stat = statfs(path).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    // Get filesystem type from the magic number
    let fs_type = get_fs_type_name(stat.filesystem_type().0 as u64);

    // Try to get the filesystem UUID
    let fs_id = get_fs_uuid(path).ok().flatten();

    Ok(FsInfo { fs_type, fs_id })
}

/// Convert a filesystem magic number to a human-readable name.
fn get_fs_type_name(magic: u64) -> Option<String> {
    // Common filesystem magic numbers (from statfs.h / magic.h)
    let name = match magic {
        0x9123683E => "btrfs",
        0xEF53 => "ext4", // Also ext2/ext3
        0x58465342 => "xfs",
        0x6969 => "nfs",
        0x01021994 => "tmpfs",
        0x28cd3d45 => "cramfs",
        0x137F | 0x138F | 0x2468 | 0x2478 => "minix",
        0x4d44 => "vfat",
        0x52654973 => "reiserfs",
        0x9fa0 => "proc",
        0x62656572 => "sysfs",
        0x64626720 => "debugfs",
        0x73636673 => "securityfs",
        0x858458f6 => "ramfs",
        0x794c7630 => "overlayfs",
        0x65735546 => "fuse",
        0x5346544e => "ntfs",
        0x6165676C => "pstorefs",
        0x19800202 => "mqueue",
        0xcafe4a11 => "bpf",
        0x27e0eb => "cgroup",
        0x63677270 => "cgroup2",
        0x1cd1 => "devpts",
        0x2fc12fc1 => "zfs",
        0xf15f => "ecryptfs",
        0x4244 => "hfs",
        0x482b => "hfsplus",
        0xf2f52010 => "f2fs",
        0xaad7aaea => "panfs",
        0x7461636f => "ocfs2",
        0xfe534d42 => "smb2",
        0xff534d42 => "cifs",
        0x47504653 => "gpfs",
        0x013111a8 => "ibrix",
        0x24051905 => "ubifs",
        0x786f4256 => "vboxsf",
        0x61756673 => "aufs",
        0x73717368 => "squashfs",
        0xde5e81e4 => "efivarfs",
        0x00011954 => "ufs",
        0x15013346 => "udf",
        0x4006 => "fat",
        _ => return None,
    };
    Some(name.to_string())
}

/// Try to get the filesystem UUID from /sys/dev/block.
fn get_fs_uuid(path: &Path) -> io::Result<Option<String>> {
    // Get the device ID from the path's metadata
    let metadata = fs::metadata(path)?;
    let dev = metadata.dev();

    // Split into major/minor
    let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
    let minor = (dev & 0xff) | ((dev >> 12) & !0xff);

    // Try to read UUID from sysfs
    let uuid_path = format!("/sys/dev/block/{}:{}/uuid", major, minor);
    if let Ok(uuid) = fs::read_to_string(&uuid_path) {
        let uuid = uuid.trim();
        if !uuid.is_empty() {
            return Ok(Some(uuid.to_string()));
        }
    }

    // Try the parent device (for partitions)
    let device_path = format!("/sys/dev/block/{}:{}/device/../uuid", major, minor);
    if let Ok(uuid) = fs::read_to_string(&device_path) {
        let uuid = uuid.trim();
        if !uuid.is_empty() {
            return Ok(Some(uuid.to_string()));
        }
    }

    // Try reading from /dev/disk/by-uuid by scanning for matching device
    if let Ok(entries) = fs::read_dir("/dev/disk/by-uuid") {
        for entry in entries.flatten() {
            if let Ok(target) = fs::read_link(entry.path()) {
                if let Ok(target_metadata) = fs::metadata(&target) {
                    if target_metadata.dev() == dev || target_metadata.rdev() == dev {
                        if let Some(uuid) = entry.file_name().to_str() {
                            return Ok(Some(uuid.to_string()));
                        }
                    }
                }
                // Also check if the symlink resolves to our device
                if let Ok(canonical) = fs::canonicalize(entry.path()) {
                    if let Ok(canonical_meta) = fs::metadata(&canonical) {
                        // Check rdev for block devices
                        if canonical_meta.rdev() == dev {
                            if let Some(uuid) = entry.file_name().to_str() {
                                return Ok(Some(uuid.to_string()));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(None)
}

/// Check if a path is on a read-only filesystem.
pub fn is_readonly(path: &Path) -> io::Result<bool> {
    let stat = statfs(path).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    // Check if ST_RDONLY flag is set
    // The flags field contains mount flags
    Ok(stat.flags().contains(nix::sys::statvfs::FsFlags::ST_RDONLY))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_hostname() {
        let hostname = get_hostname();
        assert!(hostname.is_some());
        assert!(!hostname.unwrap().is_empty());
    }

    #[test]
    fn test_get_fs_info() {
        let info = get_fs_info(Path::new("/")).unwrap();
        // Root filesystem should have a type
        assert!(info.fs_type.is_some());
    }

    #[test]
    fn test_is_readonly() {
        // Root is typically writable
        let readonly = is_readonly(Path::new("/tmp")).unwrap();
        assert!(!readonly);
    }
}
