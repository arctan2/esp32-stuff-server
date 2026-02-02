use crate::{as_ref};
use crate::types::{PageBuffer};
use crate::page_rw::{PageRW, PAGE_SIZE};
use crate::db::{Error, FixedPages};
use embedded_sdmmc::{BlockDevice, TimeSource, File, Directory};
use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;

pub const WAL_FILE_NAME: &'static str = "DB_WAL";
pub const WAL_MAGIC: [u8; 8] = *b"WAL_FILE";
pub const WAL_TRAILER: [u8; 12] = *b"WAL_FILE_END";

#[derive(Debug)]
#[repr(C, packed)]
struct WalHeader {
    magic: [u8; 8],
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
    cur_header: Option<WalHeader>,
    dir: &'a Directory<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    opened_file: Option<File<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>>,
    page_rw: &'a PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
}

impl WalHeader {
    fn default() -> Self {
        Self {
            magic: WAL_MAGIC,
            page_size: PAGE_SIZE as u32,
            page_count: 0
        }
    }
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
            opened_file: None,
            cur_header: None,
            page_rw: page_rw
        }
    }

    fn read_header<A: Allocator + Clone>(
        &mut self,
        buf: &mut PageBuffer<A>,
        f: &File<D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> Result<&WalHeader, Error<D::Error>> {
        let _ = f.read(buf.as_mut())?;
        Ok(unsafe { as_ref!(buf, WalHeader) })
    }

    pub fn check_restore_wal<A: Allocator + Clone>(
        &mut self,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<D::Error>> {
        let wal_file = match self.dir.open_file_in_dir(WAL_FILE_NAME, embedded_sdmmc::Mode::ReadOnly) {
            Ok(f) => f,
            Err(e) => match e {
                embedded_sdmmc::Error::NotFound => return Ok(()),
                other => return Err(Error::SdmmcErr(other))
            }
        };
        let header = self.read_header(buf, &wal_file)?;
        println!("header = {:?}", header);
        Ok(())
    }

    fn write_header_to_wal_file(&mut self) -> Result<(), Error<D::Error>> {
        match &self.opened_file {
            Some(f) => {
                let header = self.cur_header.as_mut().unwrap();
                f.seek_from_start(0)?;
                f.write(&header.magic)?;
                f.write(&header.page_size.to_le_bytes())?;
                f.write(&header.page_count.to_le_bytes())?;
            },
            None => ()
        }

        Ok(())
    }

    fn read_write_page_to_wal_file<A: Allocator + Clone>(
        &mut self,
        page: u32,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<D::Error>> {
        match &self.opened_file {
            Some(f) => {
                let header = self.cur_header.as_mut().unwrap();
                let _ = self.page_rw.read_page(FixedPages::Header as u32, buf.as_mut())?;
                f.write(&(FixedPages::Header as u32).to_le_bytes())?;
                f.write(buf.as_ref())?;
                header.page_count += 1;
            },
            None => ()
        }

        Ok(())
    }

    pub fn begin_write<A: Allocator + Clone>(
        &mut self,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<D::Error>> {
        let wal_file = self.dir.open_file_in_dir(WAL_FILE_NAME, embedded_sdmmc::Mode::ReadWriteCreate)?;
        self.opened_file = Some(wal_file);
        self.cur_header = Some(WalHeader::default());

        self.write_header_to_wal_file()?;

        self.read_write_page_to_wal_file(FixedPages::Header as u32, buf)?;
        self.read_write_page_to_wal_file(FixedPages::FreeList as u32, buf)?;
        self.read_write_page_to_wal_file(FixedPages::DbCat as u32, buf)?;

        Ok(())
    }

    pub fn append_pages_vec<A: Allocator + Clone>(
        &mut self,
        pages: &Vec<u32, A>,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<D::Error>> {
        for page in pages.iter() {
            self.read_write_page_to_wal_file(*page, buf)?;
        }
        Ok(())
    }

    pub fn append_page<A: Allocator + Clone>(
        &mut self,
        page: u32,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<D::Error>> {
        self.read_write_page_to_wal_file(page, buf)?;
        Ok(())
    }

    pub fn write_trailer_to_wal_file(&self) -> Result<(), Error<D::Error>> {
        let f = self.opened_file.as_ref().unwrap();
        f.write(&WAL_TRAILER)?;
        Ok(())
    }

    pub fn end_write(&mut self) -> Result<(), Error<D::Error>> {
        self.write_header_to_wal_file()?;
        self.write_trailer_to_wal_file()?;
        self.opened_file.as_ref().unwrap().flush()?;
        Ok(())
    }

    pub fn delete_wal(&mut self) -> Result<(), Error<D::Error>> {
        if let Some(f) = self.opened_file.take() {
            f.close()?;
        }
        self.dir.delete_file_in_dir(WAL_FILE_NAME)?;
        self.opened_file = None;
        self.cur_header = None;
        Ok(())
    }
}

