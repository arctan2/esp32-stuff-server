use crate::{as_ref};
use crate::types::{PageBuffer};
use crate::page_rw::{PageRW, PAGE_SIZE};
use crate::db::{Error, FixedPages};
use embedded_sdmmc::{BlockDevice, TimeSource, File, Directory};
use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;

pub const WAL_FILE_NAME: &'static str = "DB_WAL";
pub const WAL_MAGIC: [u8; 8] = *b"_stufff_";

#[repr(C, packed)]
struct WalHeader {
    magic: [u8; 3],
    page_size: u32,
    page_count: u32,
}

#[repr(C, packed)]
struct WalPageDetails {
    page: u32,
    data: [u8; PAGE_SIZE]
}

pub struct WalHandler<
    'a, D: BlockDevice, T: TimeSource,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize
> {
    dir: &'a Directory<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    page_rw: &'a PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
}

impl <
    'a, D: BlockDevice, T: TimeSource,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize
> WalHandler<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES> {
    pub fn new(
        dir: &'a Directory<D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
        page_rw: &'a PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> Self {
        Self {
            dir: dir,
            page_rw: page_rw
        }
    }

    fn read_header<A: Allocator + Clone>(
        &mut self,
        buf: &mut PageBuffer<A>,
        f: &File<D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> &WalHeader {
        f.read(buf.as_mut());
        unsafe { as_ref!(buf, WalHeader) }
    }

    pub fn check_restore_wal<A: Allocator + Clone>(
        &mut self,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<D::Error>> {
        let db_file = match self.dir.open_file_in_dir(WAL_FILE_NAME, embedded_sdmmc::Mode::ReadWriteCreateOrAppend) {
            Ok(f) => f,
            Err(e) => match e {
                embedded_sdmmc::Error::NotFound => return Ok(()),
                other => return Err(Error::SdmmcErr(other))
            }
        };
        Ok(())
    }

    pub fn begin<A: Allocator + Clone>(
        &mut self,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<D::Error>> {
        println!("begin_wal");
        Ok(())
    }

    pub fn append_pages_vec<A: Allocator + Clone>(
        &mut self,
        pages: &Vec<u32, A>,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<D::Error>> {
        println!("append_pages_vec = {:?}", pages);
        Ok(())
    }

    pub fn append_page<A: Allocator + Clone>(
        &mut self,
        page: u32,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<D::Error>> {
        println!("append_page = {}", page);
        Ok(())
    }

    pub fn end(&mut self) -> Result<(), Error<D::Error>> {
        println!("end_wal");
        Ok(())
    }
}

