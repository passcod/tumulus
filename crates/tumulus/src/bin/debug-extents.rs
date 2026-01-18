use std::{
    convert::identity,
    fs::File,
    io::{Read, Seek},
};

use extentria::fiemap::FiemapLookup;

fn main() -> std::io::Result<()> {
    let path = std::env::args().nth(1).expect("USAGE: debug-extents PATH");
    let file = File::open(&path)?;
    let mut seeker = File::open(&path)?;
    let mut file_hash = blake3::Hasher::new();

    let mut total_length = 0;
    for item in FiemapLookup::extents_for_file(&file)? {
        match item {
            Err(err) => eprintln!("error reading {file:?}: {err}"),
            Ok(extent) => {
                print!(
                    "extent start={:7}\tend={:7}\tsize={:7}\tflags={}\t",
                    extent.logical_offset,
                    extent.logical_offset + extent.length,
                    extent.length,
                    [
                        extent.encrypted().then_some("encrypted"),
                        extent.encoded().then_some("encoded"),
                        extent.inline().then_some("inline"),
                        extent.shared().then_some("shared"),
                        extent.delayed_allocation().then_some("delayed"),
                        extent.location_unknown().then_some("unknown"),
                        extent.not_aligned().then_some("unaligned"),
                        extent.packed().then_some("packed"),
                        extent.simulated().then_some("sim"),
                        extent.unwritten().then_some("unwritten"),
                        extent.last().then_some("last"),
                    ]
                    .into_iter()
                    .filter_map(identity)
                    .collect::<Vec<_>>()
                    .join(",")
                );

                let mut hash = blake3::Hasher::new();
                seeker.seek(std::io::SeekFrom::Start(extent.logical_offset))?;
                let mut segment = seeker.by_ref().take(extent.length);
                file_hash.update_reader(segment.by_ref())?;
                segment.rewind()?;
                hash.update_reader(segment)?;
                println!("hash={}", hash.finalize().to_hex());

                total_length += extent.length;
            }
        }
    }

    println!(
        "file\tsize={total_length}\ttrue={}\thash={}",
        file.metadata()?.len(),
        file_hash.finalize().to_hex()
    );

    Ok(())
}
