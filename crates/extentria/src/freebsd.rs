use std::{fs::File, io};

use crate::{
    types::{RangeIter, RangeReaderImpl, private::Sealed},
    unix_seek,
};

/// Range reader for FreeBSD using SEEK_HOLE/SEEK_DATA.
#[derive(Debug, Default)]
pub struct RangeReader;

impl Sealed for RangeReader {}

impl RangeReaderImpl for RangeReader {
    fn new() -> Self {
        Self
    }

    fn read_ranges<'a>(&'a mut self, file: &'a File) -> io::Result<RangeIter<'a>> {
        Ok(Box::new(unix_seek::read_ranges(file)?))
    }
}
