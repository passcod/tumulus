## Snapshot Catalog

This is a sqlite database. It is built from a directory tree on a real filesystem (probably).
This can be thought of as a Tar or Zip archive, but without the actual file data. Whenever
possible, the catalog should be built from a read-only snapshot.

The catalog has three design goals:

- be readable and writeable on any platform
- be maximally efficient when running on BTRFS (or similar)
- require no client-side long-term caching of data

The idea of the catalog-tree-file-blob-extent structure is deliberate mimicry of BTRFS. The catalog
should be able to be built by reading the extent metadata of every file in a BTRFS tree, encoding
the details, and then uploading the extents. That is, instead of doing our own chunking, we let the
filesystem do it. Additionally, at restore time, if we see a file with matching extents to what we
are trying to restore, we can directly reference the existing BTRFS extents instead of downloading
and writing the actual data.

A catalog is expected to be less than 0.5% of the size of the data it's referencing, even with very
high amounts of files and fragmentation of said files.

`fs_` fields are filled in when possible but are not required for the catalog to function; they are
used to speed up a subsequent run, when the machine and filesystem IDs match.

### `metadata` table

Columns: `key` (text), `value` (jsonb)

Mandatory keys:

- `protocol`: the catalog protocol/schema version (currently 1)
- `id`: the catalog UUID in lowercase hex
- `machine`: the machine ID in lowercase hex
- `tree`: the tree hash (described below) in lowercase hex
- `created`: when the catalog was created (milliseconds since the epoch)

Optional keys:

- `name`: the friendly name of the catalog
- `machine_hostname`: the hostname or FQDN of the machine
- `source_path`: the source path that was saved in this catalog
- `started`: when the process of creating the catalog started
- `fs_type`: type of filesystem
- `fs_id`: UUID of the filesystem
- `from_writeable`: present and `true` if the catalog was created from a writeable tree
- Any other arbitrary data, prefixed with `extra.`

### `extents` table

Columns:

- `extent_id` (blob) primary key: BLAKE3 hash of the contents
- `bytes` (integer): size of the extent in bytes
- `fs_object_id` (integer, optional): in BTRFS, the object ID of the extent
- `fs_checksum_type` (integer, optional): in BTRFS, the `csum_type`
- `fs_checksum` (blob, optional): in BTRFS, the `csum`

Indexes:

- `extent_id`

### `blob_extents` table

Columns:

- `blob_id` (blob)
- `extent_id` (blob)
- `offset` (integer): offset in bytes
- `bytes` (integer): size of the extent in bytes

Indexes:

- `blob_id`
- `extent_id`
- `(blob_id, extent_id)` primary key
- `(blob_id, offset)`

### `blobs` table

Columns:

- `blob_id` (blob): BLAKE3 hash of the extent map (described below)
- `bytes` (integer): total size in bytes of the blob
- `extents` (integer): amount of extents in this blob

Indexes:

- `blob_id` primary key

### `files` table

Columns:

- `file_id` (integer): auto-incremented, primary key
- `path` (blob): normalised path of the file
- `blob_id` (blob, optional)
- `ts_created` (date, optional)
- `ts_changed` (date, optional)
- `ts_modified` (date, optional)
- `ts_accessed` (date, optional)
- `attributes` (jsonb, optional)
- `unix_mode` (integer, optional)
- `unix_owner_id` (integer, optional)
- `unix_owner_name` (text, optional)
- `unix_group_id` (integer, optional)
- `unix_group_name` (text, optional)
- `special` (jsonb, optional): if this is a special file (symlink, hardlink, device, etc), this info
- `fs_inode` (integer, optiona): the inode of the file on the machine
- `extra` (jsonb, optional): any additional data

Paths are normalised in that folder separators are always forward slashes (unix style), and Windows
paths are re-encoded in UTF-8 (instead of UTF-16).

Indexes:

- `path`
- `blob_id`
- all the timestamps

## Server Layout

This is how the data is stored on the server (which is generally an object store like S3).

- `extents/ab/cd/ef9134ab509048b78cfe6f444215`: the actual data
- `blobs/ab/cd/ef6a38ed9a50922d3db39ecfb1c4`: extent map for this blob
- `catalogs/10/b6/6bbfeb4e4a3bbe02986ff6c5e28f`: the actual sqlite catalog file
- `catalog.idx`: sqlite file containing best-effort indexes of catalog metadata and tree hashes to IDs

### Extent data

This is the raw data.

In storage backends that support metadata, that may indicate a content type which indicates that
the stored extent is actually compressed. The extent ID must always be the hash of the uncompressed
content. If there's no support for that sideband metadata, the extent must always be uncompressed.

Path shape:

- `extents`
- first byte of ID
- second byte of ID
- remaining bytes of ID

The ID is a BLAKE3 hash of the contents, lowercase hex encoded.

(The format is designed to allow flexible hash choice, but right now everything is BLAKE3.)

### Blob layout

This is how actual file contents are described. Files are zero or one blobs. Blobs are one or more
extents. Zero-sized blobs are not special, but if you see the zero-size blob ID you can skip
actually reading it; it does exist, though, if you have a zero-sized file in your data.

Header:

- 1 byte: version (0x01)
- 1 byte: size of the extent ID (0x20) (H)
- 8 bytes (u64 LE): total size of the blob's contents in bytes
- 8 bytes (u64 LE): amount of extents in the blob (N)

Map (repeated N times):

- 8 bytes: offset into the blob
- H bytes: extent ID

Path shape:

- `blobs`
- first byte of ID
- second byte of ID
- remaining bytes of ID

The ID is a BLAKE3 hash of the full concatenated contents (every extent in order), lowercase hex encoded.

### Tree hashes

This is a BLAKE3 hash of a rigidly-structured entire snapshot's file tree, mapping each file to its
blob (which maps to its extents). The tree _map_ is never written anywhere. It's computed from the
catalog, and then immediately hashed and stored in the catalog (and then in the catalog index). The
real purpose is as an optimisation when storing a new snapshot: if the file contents of the new
snapshot is identical to another snapshot, then their trees will hash to exactly the same thing, and
thus we can skip writing (and uploading) all the data.

_Technically_ if you actually had the tree data, you could take it and restore the snapshot, but you
would have lost all special files and all of the metadata except filenames.

Header:

- 1 byte: version (0x01)
- 1 byte: size of the blob ID (0x20) (H)
- 8 bytes (u64 LE): total size of the all files in the tree
- 8 bytes (u64 LE): total size of the all blobs in the tree
- 8 bytes (u64 LE): amount of files in the tree (N)
- 8 bytes (u64 LE): amount of blobs in the tree

Map (repeated N times):

- 4 bytes (u32 LE): size of the filepath (P)
- P bytes: filepath in bytes with unix slashes
- H bytes: blob ID

Map is _sorted_ byte-wise by the byte content of each entry.

Files that don't have any content (not zero-sized files, but special files like links) are not
listed in the tree map, since it's only used to cheaply skip writing any extent data.

### Catalog files

The internal structure of the files is described in the "Snapshot Catalog" section above.

Path shape:

- `catalogs`
- first byte of the catalog ID
- second byte of the catalog ID
- remaining bytes of the catalog ID

The ID is a UUID in lowercase hex without any punctuation.

### Catalog index

This is a sqlite database that contains a two tables:

- `catalogs`, which has:
  - `id` (blob): primary key, the catalog ID
  - `machine` (blob): the machine ID
  - `tree` (blob): the tree hash (described above)
  - `name` (text, optional): the friendly name of the catalog
  - `date` (integer): when the catalog was created (milliseconds since the epoch)
- `metadata`, which has:
  - `key` (text)
  - `value` (jsonb)

Metadata has keys:

- `started`, date building this index started
- `completed`, date building this index ended
- `worker`, text, some identifier for the worker that build this

And `catalogs` has a btree index for every column, which is the real indexing part.

