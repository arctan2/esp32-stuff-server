use embedded_sdmmc::{File, BlockDevice, TimeSource};
use allocator_api2::alloc::Allocator;
use crate::table::{Table, Column, ColumnType, Flags, TableErr, ToName};
use crate::PageRW;
use crate::types::PageBuffer;
use crate::PageFreeList;

pub struct Database<'a, D, T, const MAX_DIRS: usize, const MAX_FILES: usize, const MAX_VOLUMES: usize, A: Allocator + Clone>
where
    D: BlockDevice,
    T: TimeSource,
{
    page_rw: PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    buf1: PageBuffer<A>,
    buf2: PageBuffer<A>,
}

pub const MAGIC: [u8; 8] = *b"_stufff_";

#[derive(Debug)]
#[repr(packed)]
#[allow(unused)]
pub struct DBHeader {
    magic: [u8; 8],
    page_count: u32,
}

impl Default for DBHeader {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            page_count: 0,
        }
    }
}

#[derive(Debug)]
pub enum Error<E: core::fmt::Debug> {
    SdmmcErr(embedded_sdmmc::Error<E>),
    TableErr(TableErr),
    FreeListNotFound,
    HeaderNotFound
}

impl<DErr> From<embedded_sdmmc::Error<DErr>> for Error<DErr> where DErr: core::fmt::Debug {
    fn from(err: embedded_sdmmc::Error<DErr>) -> Self {
        Error::SdmmcErr(err)
    }
}

impl<E> From<TableErr> for Error<E> where E: core::fmt::Debug {
    fn from(err: TableErr) -> Self {
        Error::TableErr(err)
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
            buf1: PageBuffer::new(allocator.clone()),
            buf2: PageBuffer::new(allocator),
        };
    }

    fn get_or_create_header(&mut self) -> Result<DBHeader, Error<D::Error>> {
        let count = self.page_rw.read_page(0, self.buf1.as_mut())?;
        if count == 0 {
            let header = DBHeader::default();
            unsafe { 
                self.buf1.write(0, &header);
            }
            self.page_rw.write_page(0, self.buf1.as_ref())?;
        }
        return unsafe {
            Ok(self.buf1.read::<DBHeader>(0))
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

    fn create_new_db(&mut self, mut header: DBHeader) -> Result<(), Error<D::Error>> {
        header.page_count = 2;
        unsafe {
            self.buf1.write(0, &header);
            let _ = self.page_rw.write_page(0, self.buf1.as_ref())?;
        }
        let _ = self.page_rw.extend_file_by_pages(1, self.buf1.as_mut())?;
        let db_name = Column::new("db_name".to_name(), ColumnType::Chars, Flags::Primary);
        let page = Column::new("page".to_name(), ColumnType::Int, Flags::None);
        Table::create("db_cat".to_name())
            .add_column(db_name)?
            .add_column(page)?
            .write_to_buf(&mut self.buf1);

        unsafe {
            let free_page = PageFreeList::get_free_page::<D, T, A, MAX_DIRS, MAX_FILES, MAX_VOLUMES>(self.buf2.as_mut(), &self.page_rw)?;
            let _ = self.page_rw.write_page(free_page, self.buf1.as_ref())?;
        }

        Ok(())
    }

    pub fn init(&mut self) -> Result<(), Error<D::Error>> {
        let header = self.get_or_create_header()?;
        if header.page_count == 0 {
            self.create_new_db(header)?;
        }
        Ok(())
    }
}
