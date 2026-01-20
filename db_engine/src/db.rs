use embedded_sdmmc::{File, BlockDevice, TimeSource};
use allocator_api2::alloc::Allocator;
use crate::PageRW;
use crate::types::PageBuffer;
use crate::PageFreeList;

pub struct Database<'a, D, T, const MAX_DIRS: usize, const MAX_FILES: usize, const MAX_VOLUMES: usize, A: Allocator + Clone>
where
    D: BlockDevice,
    T: TimeSource,
{
    page_rw: PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    buf: PageBuffer<A>,
}

pub const MAGIC: [u8; 8] = *b"_stufff_";

#[derive(Debug)]
#[repr(packed)]
#[allow(unused)]
pub struct DBHeader {
    magic: [u8; 8],
    page_count: u32,
    free_list_head_page: u32,
}

impl Default for DBHeader {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            page_count: 0,
            free_list_head_page: 0,
        }
    }
}

#[derive(Debug)]
pub enum Error<E: core::fmt::Debug> {
    SdmmcErr(embedded_sdmmc::Error<E>),
    FreeListNotFound,
    HeaderNotFound
}

impl<DErr> From<embedded_sdmmc::Error<DErr>> for Error<DErr> where DErr: core::fmt::Debug {
    fn from(err: embedded_sdmmc::Error<DErr>) -> Self {
        Error::SdmmcErr(err)
    }
}

impl <'a, D, T, const MAX_DIRS: usize, const MAX_FILES: usize, const MAX_VOLUMES: usize, A: Allocator + Clone>
Database<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES, A>
where
    D: BlockDevice,
    T: TimeSource,
{
    pub fn new(file: File<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>, allocator: A) -> Self {
        return Self {
            page_rw: PageRW::new(file),
            buf: PageBuffer::new(allocator),
        };
    }

    fn get_or_create_header(&mut self) -> Result<DBHeader, Error<D::Error>> {
        let count = self.page_rw.read_page(0, self.buf.as_mut())?;
        if count == 0 {
            let header = DBHeader::default();
            unsafe { 
                self.buf.write(0, &header);
            }
            self.page_rw.write_page(0, self.buf.as_ref())?;
        }
        return unsafe {
            Ok(self.buf.read::<DBHeader>(0))
        };
    }

    pub fn inc_page_count(
        buf: &mut [u8; crate::page_rw::PAGE_SIZE],
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> Result<(), Error<D::Error>> {
        let _ = page_rw.read_page(0, buf)?;
        let header = buf.as_mut_ptr() as *mut DBHeader;
        unsafe {
            (*header).page_count += 1;
        }
        let _ = page_rw.write_page(0, buf)?;
        Ok(())
    }

    pub fn init(&mut self) -> Result<(), Error<D::Error>> {
        let mut header = self.get_or_create_header()?;
        if header.page_count == 0 {
            header.page_count = 2;
            header.free_list_head_page = 1;
            unsafe {
                self.buf.write(0, &header);
                let _ = self.page_rw.write_page(0, self.buf.as_ref())?;
            }
            let _ = self.page_rw.extend_file_by_pages(1, self.buf.as_mut())?;
        }
        Ok(())
    }
}
