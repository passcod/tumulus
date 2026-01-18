//! Filesystem information utilities.
//!
//! Provides functions to get filesystem type, UUID, and hostname.

use std::fs::{self, File};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use nix::libc;
use nix::sys::statfs::statfs;

/// BTRFS ioctl magic number
const BTRFS_IOCTL_MAGIC: u8 = 0x94;

/// BTRFS_IOC_SUBVOL_GETFLAGS = _IOR(0x94, 25, u64)
/// Formula: (2 << 30) | (type << 8) | nr | (size << 16)
const BTRFS_IOC_SUBVOL_GETFLAGS: libc::c_ulong =
    (2 << 30) | ((BTRFS_IOCTL_MAGIC as libc::c_ulong) << 8) | 25 | (8 << 16);

/// BTRFS subvolume read-only flag
const BTRFS_SUBVOL_RDONLY: u64 = 1 << 1;

/// BTRFS filesystem magic number
const BTRFS_SUPER_MAGIC: u64 = 0x9123683E;

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

/// Check if a path is on a read-only filesystem or btrfs read-only snapshot.
///
/// This checks both the mount flags (ST_RDONLY) and, for btrfs filesystems,
/// the subvolume read-only property which is used for read-only snapshots.
pub fn is_readonly(path: &Path) -> io::Result<bool> {
    let stat = statfs(path).map_err(|e| io::Error::other(e))?;

    // Check if ST_RDONLY mount flag is set
    if stat.flags().contains(nix::sys::statvfs::FsFlags::ST_RDONLY) {
        return Ok(true);
    }

    // For btrfs, also check the subvolume read-only flag
    if stat.filesystem_type().0 as u64 == BTRFS_SUPER_MAGIC {
        if let Ok(readonly) = is_btrfs_subvol_readonly(path) {
            return Ok(readonly);
        }
    }

    Ok(false)
}

/// Check if a btrfs subvolume is marked read-only.
///
/// This uses the BTRFS_IOC_SUBVOL_GETFLAGS ioctl to check the subvolume's
/// read-only property, which is set on read-only snapshots.
fn is_btrfs_subvol_readonly(path: &Path) -> io::Result<bool> {
    let file = File::open(path)?;
    let fd = file.as_raw_fd();

    let mut flags: u64 = 0;

    // SAFETY: We're calling ioctl with a valid fd and a pointer to a u64.
    // The ioctl reads flags into the provided buffer.
    let result = unsafe { libc::ioctl(fd, BTRFS_IOC_SUBVOL_GETFLAGS, &mut flags as *mut u64) };

    if result < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok((flags & BTRFS_SUBVOL_RDONLY) != 0)
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
