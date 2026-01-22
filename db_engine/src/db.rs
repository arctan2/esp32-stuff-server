use embedded_sdmmc::{File, BlockDevice, TimeSource};
use allocator_api2::alloc::Allocator;
use crate::btree::{BtreeLeaf};
use crate::table::{Table, Column, ColumnType, Flags, TableErr, ToName, Row, Value, Name};
use crate::PageRW;
use crate::types::PageBuffer;
use crate::PageFreeList;
use crate::{as_ref_mut, as_ref};

macro_rules! get_free_page {
    ($self:ident, $buf:expr) => {
        PageFreeList::get_free_page::<D, T, A, MAX_DIRS, MAX_FILES, MAX_VOLUMES>(
            $buf, 
            &$self.page_rw
        )
    };
}

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

#[repr(u32)]
pub enum FixedPages {
    Header = 0,
    FreeList = 1,
    DbCat = 2
}

#[derive(Debug)]
pub enum InsertErr<E: core::fmt::Debug> {
    SdmmcErr(embedded_sdmmc::Error<E>),
    ColCountDoesNotMatch,
    CannotBeNull,
    TypeDoesNotMatch,
    CharsTooLong,
}

impl<DErr> From<embedded_sdmmc::Error<DErr>> for InsertErr<DErr> where DErr: core::fmt::Debug {
    fn from(err: embedded_sdmmc::Error<DErr>) -> Self {
        InsertErr::SdmmcErr(err)
    }
}

impl From<FixedPages> for u32 {
    fn from(page: FixedPages) -> Self {
        page as u32
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
        let count = self.page_rw.read_page(FixedPages::Header.into(), self.buf1.as_mut())?;
        if count == 0 {
            let header = DBHeader::default();
            unsafe { 
                self.buf1.write(0, &header);
            }
            self.page_rw.write_page(FixedPages::Header.into(), self.buf1.as_ref())?;
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
            let _ = self.page_rw.write_page(FixedPages::Header.into(), self.buf1.as_ref())?;
        }
        let _ = self.page_rw.extend_file_by_pages(1, self.buf1.as_mut())?;
        let tbl_name = Column::new("tbl_name".to_name(), ColumnType::Chars, Flags::Primary);
        let page = Column::new("page".to_name(), ColumnType::Int, Flags::None);
        Table::create("db_cat".to_name())
            .add_column(tbl_name)?
            .add_column(page)?
            .write_to_buf(&mut self.buf1);

        unsafe {
            let free_page = get_free_page!(self, self.buf2.as_mut())?;
            self.page_rw.write_page(free_page, self.buf1.as_ref())?;
        }

        Ok(())
    }

    fn verify_row(col: &Column, val: &Value) -> Result<(), InsertErr<D::Error>> {
        match val {
            Value::Null => {
                if col.flags.is_set(Flags::Primary) || !col.flags.is_set(Flags::Nullable) {
                    return Err(InsertErr::CannotBeNull);
                }
                if col.flags.is_set(Flags::Foreign) {
                    todo!("foreign key check");
                }
            },
            Value::Int(_) => {
                if col.col_type != ColumnType::Int {
                    return Err(InsertErr::TypeDoesNotMatch);
                }
                if col.flags.is_set(Flags::Foreign) {
                    todo!("foreign key check");
                }
            },
            Value::Float(_) => {
                if col.col_type != ColumnType::Float {
                    return Err(InsertErr::TypeDoesNotMatch);
                }
                if col.flags.is_set(Flags::Foreign) {
                    todo!("foreign key check");
                }
            },
            Value::Chars(_) => {
                if col.col_type != ColumnType::Chars {
                    return Err(InsertErr::TypeDoesNotMatch);
                }
                if col.flags.is_set(Flags::Foreign) {
                    todo!("foreign key check");
                }
            }
        }

        Ok(())
    }

    pub fn insert_to_table(&mut self, table: u32, row: Row<'_, A>, allocator: A) -> Result<(), InsertErr<D::Error>> {
        unsafe {
            let table = self.page_rw.read_page(table, self.buf1.as_mut());
            let table = as_ref!(self.buf1, Table);

            if table.col_count as usize != row.len() {
                return Err(InsertErr::ColCountDoesNotMatch);
            }

            let mut i: usize = 0;

            while i < (table.col_count as usize) {
                Self::verify_row(&table.columns[i], &row[i])?;
                i += 1;
            }
            todo!("do a lazy serializer while inserting");
        }
        Ok(())
    }

    pub fn create_table(&mut self, name: Name, table: &[Column], allocator: A) -> Result<(), Error<D::Error>> {
        unsafe {
            let _ = self.page_rw.read_page(FixedPages::DbCat.into(), self.buf1.as_mut())?;
            let mut db_cat = as_ref_mut!(self.buf1, Table);

            if db_cat.rows_btree_page == 0 {
                let free_page = get_free_page!(self, self.buf2.as_mut())?;
                db_cat.rows_btree_page = free_page;
                self.page_rw.write_page(FixedPages::DbCat.into(), self.buf1.as_ref())?;
                let btree_leaf = self.buf2.as_ptr_mut::<BtreeLeaf>(0);
                BtreeLeaf::init_ref(btree_leaf);
                self.page_rw.write_page(free_page, self.buf2.as_ref())?;
            }
            let free_page = get_free_page!(self, self.buf2.as_mut())?;
            let mut row = Row::new_in(allocator.clone());
            row.push(Value::Chars(name.as_ref()));
            row.push(Value::Int(free_page as i64));
            self.insert_to_table(2, row, allocator).unwrap();
        }
        Ok(())
    }

    pub fn init(&mut self, allocator: A) -> Result<(), Error<D::Error>> {
        let header = self.get_or_create_header()?;
        if header.page_count == 0 {
            self.create_new_db(header)?;
        }

        let path = Column::new("path".to_name(), ColumnType::Chars, Flags::Primary);
        let size = Column::new("size".to_name(), ColumnType::Int, Flags::None);
        let name = Column::new("name".to_name(), ColumnType::Chars, Flags::None);
        let _ = self.create_table("cool_table".to_name(), &[path, size, name], allocator)?;
        Ok(())
    }
}
