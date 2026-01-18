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

    let mut total_extents = 0;
    let mut total_length = 0;
    let mut buf = [0u8; 16384];
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
                let mut bytes_read = 0u64;
                loop {
                    let n = segment.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    bytes_read += n as u64;
                    file_hash.update(&buf[..n]);
                    hash.update(&buf[..n]);
                }
                println!("read={bytes_read}\thash={}", hash.finalize().to_hex());

                total_extents += extent.length;
                total_length += bytes_read;
            }
        }
    }

    println!(
        "file\tsize={total_length}\textsum={total_extents}\ttrue={}\thash={}",
        file.metadata()?.len(),
        file_hash.finalize().to_hex()
    );

    Ok(())
}
