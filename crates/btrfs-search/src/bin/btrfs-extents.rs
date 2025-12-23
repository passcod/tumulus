use std::fs::File;

use btrfs_search::BtrfsSearch;

fn main() -> std::io::Result<()> {
    BtrfsSearch::ensure_size();

    let path = std::env::args().nth(1).expect("USAGE: btrfs-extents PATH");
    let file = File::open(&path)?;

    for item in BtrfsSearch::extents_for_file(&file)? {
        let item = item?;
        dbg!(item);
    }

    Ok(())
}
