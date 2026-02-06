use crate::{as_ref};
use crate::fs::{DbDir, Mode, PageFile};
use crate::page_buf::{PageBuffer};
use crate::page_rw::{PageRW, PAGE_SIZE};
use crate::db::{Error, FixedPages};
use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;

#[cfg(feature = "std")]
extern crate std;

pub const WAL_FILE_NAME: &'static str = "DB_WAL";
pub const DB_FILE_NAME: &'static str = "DB";
pub const WAL_MAGIC: [u8; 8] = *b"WAL_FILE";
pub const WAL_TRAILER: [u8; 12] = *b"WAL_FILE_END";

#[derive(Debug)]
#[repr(C, packed)]
pub struct WalHeader {
    magic: [u8; 8],
    page_size: u32,
    page_count: u32,
}

pub struct FileHandler<F: PageFile> {
    cur_header: Option<WalHeader>,
    wal_file: Option<F>,
    pub page_rw: Option<PageRW<F>>
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

impl <F> FileHandler<F>
where
    F: PageFile,
{
    pub fn new() -> Result<Self, Error<F::Error>> {
        Ok(Self {
            wal_file: None,
            cur_header: None,
            page_rw: None
        })
    }

    pub fn open_with_wal_check<'a, A: Allocator + Clone, D: DbDir<'a, Error = F::Error, File<'a> = F>>(
        &mut self,
        dir: &'a D,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<F::Error>> {
        let db_file = dir.open_file_in_dir(DB_FILE_NAME, Mode::ReadWriteCreateOrAppend)?;
        let wal_file = dir.open_file_in_dir(WAL_FILE_NAME, Mode::ReadWriteCreateOrAppend)?;
        self.page_rw = Some(PageRW::new(db_file));
        self.wal_file = Some(wal_file);
        match self.wal_check_restore(buf) {
            Err(Error::InvalidWalFile) => (),
            Err(other) => return Err(other),
            Ok(_) => ()
        };
        Ok(())
    }

    pub fn close<'a, D: DbDir<'a, Error = F::Error, File<'a> = F>>(&mut self, dir: &'a D) -> Result<(), Error<F::Error>> {
        if let Some(f) = self.wal_file.take() {
            f.close()?;
        }
        dir.delete_file_in_dir(WAL_FILE_NAME)?;
        if let Some(page_rw) = self.page_rw.take() {
            page_rw.file.close()?;
        }
        dir.delete_file_in_dir(DB_FILE_NAME)?;
        self.wal_file = None;
        self.page_rw = None;
        self.cur_header = None;
        Ok(())
    }

    fn wal_check_restore<A: Allocator + Clone>(
        &mut self,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<F::Error>> {
        {
            let wal_header = self.wal_read_header(buf)?;
            let is_magic = wal_header.magic == WAL_MAGIC;
            if !is_magic || !self.wal_verify_trailer()? {
                self.cur_header = None;
                return Ok(());
            }
        }

        let wal_header = self.wal_read_header(buf)?;

        if wal_header.page_size as usize != PAGE_SIZE {
            return Err(Error::WalNotSupported);
        }

        let page_count = wal_header.page_count;
        let page_rw = self.page_rw.as_ref().ok_or(Error::InitError)?;

        for _ in 0..page_count {
            let page = self.wal_read_u32()?;
            self.wal_read_buf(buf)?;
            page_rw.write_page(page, buf.as_mut())?;
        }

        self.cur_header = Some(WalHeader::default());
        self.wal_write_header_to_file()?;

        Ok(())
    }

    fn wal_read_u32(&self) -> Result<u32, Error<F::Error>> {
        let mut buf = [0u8; 4];
        let wal_file = self.wal_file.as_ref().ok_or(Error::InitError)?;
        wal_file.read(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn wal_read_buf<A: Allocator + Clone>(
        &self,
        buf: &mut PageBuffer<A>
    ) -> Result<usize, Error<F::Error>> {
        let wal_file = self.wal_file.as_ref().ok_or(Error::InitError)?;
        Ok(wal_file.read(buf.as_mut())?)
    }

    pub fn wal_read_header<A: Allocator + Clone>(
        &mut self,
        buf: &mut PageBuffer<A>,
    ) -> Result<&WalHeader, Error<F::Error>> {
        let wal_file = self.wal_file.as_ref().ok_or(Error::InitError)?;
        wal_file.seek_from_start(0)?;
        let _ = wal_file.read(&mut buf.as_mut()[0..core::mem::size_of::<WalHeader>()])?;
        Ok(unsafe { as_ref!(buf, WalHeader) })
    }

    pub fn wal_verify_trailer(&mut self) -> Result<bool, Error<F::Error>> {
        let mut trailer_buf: [u8; WAL_TRAILER.len()] = [0; WAL_TRAILER.len()];
        let wal_file = self.wal_file.as_ref().ok_or(Error::InitError)?;
        match wal_file.seek_from_end(WAL_TRAILER.len() as u32) {
            Ok(_) => (),
            Err(_) => return Ok(false)
        };
        wal_file.read(&mut trailer_buf)?;
        Ok(trailer_buf == WAL_TRAILER)
    }

    fn wal_write_header_to_file(&mut self) -> Result<(), Error<F::Error>> {
        let wal_file = self.wal_file.as_ref().ok_or(Error::InitError)?;
        let header = self.cur_header.as_mut().unwrap();
        wal_file.seek_from_start(0)?;
        wal_file.write(&header.magic)?;
        wal_file.write(&header.page_size.to_le_bytes())?;
        wal_file.write(&header.page_count.to_le_bytes())?;

        Ok(())
    }

    #[allow(unused)]
    fn wal_read_write_page_to_file<A: Allocator + Clone>(
        &mut self,
        page: u32,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<F::Error>> {
        let wal_file = self.wal_file.as_ref().ok_or(Error::InitError)?;
        let header = self.cur_header.as_mut().unwrap();
        let _ = self.page_rw.as_ref().ok_or(Error::InitError)?.read_page(page, buf.as_mut())?;
        wal_file.write(&page.to_le_bytes())?;
        wal_file.write(buf.as_ref())?;
        header.page_count += 1;
        Ok(())
    }

    pub fn wal_begin_write<A: Allocator + Clone>(
        &mut self,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<F::Error>> {
        self.cur_header = Some(WalHeader::default());
        self.wal_write_header_to_file()?;
        self.wal_read_write_page_to_file(FixedPages::Header as u32, buf)?;
        self.wal_read_write_page_to_file(FixedPages::FreeList as u32, buf)?;
        self.wal_read_write_page_to_file(FixedPages::DbCat as u32, buf)?;

        Ok(())
    }

    pub fn wal_append_pages_vec<A: Allocator + Clone>(
        &mut self,
        pages: &Vec<u32, A>,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<F::Error>> {
        for page in pages.iter() {
            self.wal_read_write_page_to_file(*page, buf)?;
        }
        Ok(())
    }

    pub fn wal_append_page<A: Allocator + Clone>(
        &mut self,
        page: u32,
        buf: &mut PageBuffer<A>
    ) -> Result<(), Error<F::Error>> {
        self.wal_read_write_page_to_file(page, buf)?;
        Ok(())
    }

    pub fn wal_write_trailer_to_file(&self) -> Result<(), Error<F::Error>> {
        let f = self.wal_file.as_ref().unwrap();
        f.seek_from_end(0)?;
        f.write(&WAL_TRAILER)?;
        Ok(())
    }

    pub fn wal_end_write(&mut self) -> Result<(), Error<F::Error>> {
        self.wal_write_header_to_file()?;
        self.wal_write_trailer_to_file()?;
        self.wal_file.as_ref().unwrap().flush()?;
        Ok(())
    }

    pub fn end_wal(&mut self) -> Result<(), Error<F::Error>> {
        self.cur_header = Some(WalHeader::default());
        self.wal_write_header_to_file()?;
        Ok(())
    }
}

