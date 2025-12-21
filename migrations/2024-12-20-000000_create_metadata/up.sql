CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL
);

CREATE TABLE extents (
    blob_id BLOB NOT NULL,
    extent_id BLOB NOT NULL,
    offset INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    PRIMARY KEY (blob_id, extent_id)
);

CREATE INDEX idx_extents_blob_id ON extents(blob_id);
CREATE INDEX idx_extents_extent_id ON extents(extent_id);
CREATE INDEX idx_extents_blob_offset ON extents(blob_id, offset);

CREATE TABLE blobs (
    blob_id BLOB PRIMARY KEY,
    bytes INTEGER NOT NULL,
    extents INTEGER NOT NULL
);

CREATE TABLE files (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    path BLOB NOT NULL,
    blob_id BLOB,
    ts_created DATE,
    ts_changed DATE,
    ts_modified DATE,
    ts_accessed DATE,
    attributes JSONB,
    unix_mode INTEGER,
    unix_owner_id INTEGER,
    unix_owner_name TEXT,
    unix_group_id INTEGER,
    unix_group_name TEXT,
    special JSONB,
    extra JSONB
);

CREATE INDEX idx_files_path ON files(path);
CREATE INDEX idx_files_blob_id ON files(blob_id);
CREATE INDEX idx_files_ts_created ON files(ts_created);
CREATE INDEX idx_files_ts_changed ON files(ts_changed);
CREATE INDEX idx_files_ts_modified ON files(ts_modified);
CREATE INDEX idx_files_ts_accessed ON files(ts_accessed);
