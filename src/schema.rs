diesel::table! {
    metadata (key) {
        key -> Text,
        value -> Blob,
    }
}

diesel::table! {
    extents (blob_id, extent_id) {
        blob_id -> Binary,
        extent_id -> Binary,
        offset -> BigInt,
        bytes -> BigInt,
    }
}

diesel::table! {
    blobs (blob_id) {
        blob_id -> Binary,
        bytes -> BigInt,
        extents -> BigInt,
    }
}

diesel::table! {
    files (file_id) {
        file_id -> BigInt,
        path -> Binary,
        blob_id -> Nullable<Binary>,
        ts_created -> Nullable<Text>,
        ts_changed -> Nullable<Text>,
        ts_modified -> Nullable<Text>,
        ts_accessed -> Nullable<Text>,
        attributes -> Nullable<Blob>,
        unix_mode -> Nullable<Integer>,
        unix_owner_id -> Nullable<Integer>,
        unix_owner_name -> Nullable<Text>,
        unix_group_id -> Nullable<Integer>,
        unix_group_name -> Nullable<Text>,
        special -> Nullable<Blob>,
        extra -> Nullable<Blob>,
    }
}
