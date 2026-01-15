use std::{
    fs::File,
    io::{Read, Seek},
};

use btrfs_search::{
    BtrfsCompression, BtrfsFileExtentItemBody, BtrfsSearch, BtrfsSearchKind, BtrfsSearchResult,
    BtrfsSearchResultHeader, BtrfsSearchResultItem,
};

fn main() -> std::io::Result<()> {
    karen::pkexec().unwrap();
    BtrfsSearch::ensure_size();

    let path = std::env::args().nth(1).expect("USAGE: debug-extents PATH");
    let file = File::open(&path)?;
    let mut seeker = File::open(&path)?;
    let mut file_hash = blake3::Hasher::new();

    let mut start = 0;
    for item in BtrfsSearch::extents_for_file(&file)? {
        match item {
            Err(err) => eprintln!("error reading {file:?}: {err}"),
            Ok(BtrfsSearchResult {
                header:
                    BtrfsSearchResultHeader {
                        kind: BtrfsSearchKind::ExtentData,
                        ..
                    },
                item: BtrfsSearchResultItem::FileExtent(extent),
            }) => {
                let size = match extent.body {
                    BtrfsFileExtentItemBody::Inline(buf) => {
                        print!(
                            "extent\tramsize={:7}\tcompr={}\tinline\t\t",
                            extent.header.ram_bytes,
                            extent.header.compression != BtrfsCompression::None
                        );
                        buf.len()
                    }
                    BtrfsFileExtentItemBody::OnDisk(meta) => {
                        print!(
                            "extent\tramsize={:7}\tcompr={}\tstored={:7}\t",
                            extent.header.ram_bytes,
                            extent.header.compression != BtrfsCompression::None,
                            meta.disk_bytes
                        );
                        meta.logical_bytes as usize
                    }
                };
                print!("start={start:7}\tend={:7}\tsize={size:7}\t", start + size);

                let mut hash = blake3::Hasher::new();
                seeker.seek(std::io::SeekFrom::Start(start as _))?;
                let mut segment = seeker.by_ref().take(size as _);
                file_hash.update_reader(segment.by_ref())?;
                segment.rewind()?;
                hash.update_reader(segment)?;
                println!("hash={}", hash.finalize().to_hex());

                start = start + size;
            }
            Ok(item) => eprintln!("unexpected item in search: {:?}", item.header.kind),
        }
    }

    println!(
        "file\tsize={start}\ttrue={}\thash={}",
        file.metadata()?.len(),
        file_hash.finalize().to_hex()
    );

    Ok(())
}
