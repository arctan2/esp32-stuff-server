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
    header: Option<DBHeader>,
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
            header: None,
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

    fn get_free_list_ptr(&mut self) -> Result<*mut PageFreeList, Error<D::Error>> {
        match &self.header {
            Some(header) => {
                if header.free_list_head_page != 1 {
                    return Err(Error::FreeListNotFound);
                }
                self.page_rw.read_page(1, self.buf.as_mut())?;
                unsafe {
                    let free_list_head: *mut PageFreeList = self.buf.as_ptr_mut(0);
                    return Ok(free_list_head);
                }
            },
            None => Err(Error::HeaderNotFound)
        }
    }

    pub fn init(&mut self) -> Result<(), Error<D::Error>> {
        self.header = Some(self.get_or_create_header()?);

        match &mut self.header {
            Some(header) => {
                if header.page_count == 0 {
                    header.page_count = 2;
                    header.free_list_head_page = 1;
                    unsafe {
                        self.buf.write(0, &header);
                        let _ = self.page_rw.write_page(0, self.buf.as_ref());
                    }
                    let _ = self.page_rw.extend_file_by_pages(1, self.buf.as_mut());
                }

                unsafe {
                    for _ in 0..10 {
                        let free_page_num = PageFreeList::get_free_page(self.buf.as_mut(), &self.page_rw)?;
                        let _ = PageFreeList::add_to_list(self.buf.as_mut(), free_page_num, &self.page_rw)?;
                    }
                }
            },
            None => return Err(Error::HeaderNotFound)
        }

        Ok(())
    }
}
