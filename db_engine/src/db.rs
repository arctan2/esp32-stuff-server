use embedded_sdmmc::{File, BlockDevice, TimeSource};
use allocator_api2::alloc::Allocator;
use crate::btree::{BtreeLeaf};
use crate::table::{Table, Column, ColumnType, Flags, TableErr, ToName, Row, Value, Name, SerializedRow};
use crate::PageRW;
use crate::types::PageBuffer;
use crate::PageFreeList;
use crate::{as_ref_mut, as_ref};
use allocator_api2::vec::Vec;

macro_rules! get_free_page {
    ($self:ident, $buf:expr) => {
        PageFreeList::get_free_page::<D, T, A, MAX_DIRS, MAX_FILES, MAX_VOLUMES>(
            $buf, 
            &$self.page_rw
        )
    };
}

macro_rules! add_page_to_free_list {
    ($self:ident, $page_num:expr, $buf:expr) => {
        PageFreeList::add_page_to_list::<D, T, A, MAX_DIRS, MAX_FILES, MAX_VOLUMES>(
            $buf, 
            $page_num,
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

impl DBHeader {
    pub fn inc_page_count<
        'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    >(
        buf: &mut [u8; crate::page_rw::PAGE_SIZE],
        page_rw: &PageRW<D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> Result<(), Error<D::Error>> {
        let _ = page_rw.read_page(0, buf)?;
        let header = buf.as_mut_ptr() as *mut DBHeader;
        unsafe {
            (*header).page_count += 1;
        }
        let _ = page_rw.write_page(0, buf)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum Error<E: core::fmt::Debug> {
    SdmmcErr(embedded_sdmmc::Error<E>),
    TableErr(TableErr<E>),
    FreeListNotFound,
    HeaderNotFound
}

impl<DErr> From<embedded_sdmmc::Error<DErr>> for Error<DErr> where DErr: core::fmt::Debug {
    fn from(err: embedded_sdmmc::Error<DErr>) -> Self {
        Error::SdmmcErr(err)
    }
}

impl<E> From<TableErr<E>> for Error<E> where E: core::fmt::Debug {
    fn from(err: TableErr<E>) -> Self {
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
    TableErr(TableErr<E>),
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

impl<E> From<TableErr<E>> for InsertErr<E> where E: core::fmt::Debug {
    fn from(err: TableErr<E>) -> Self {
        InsertErr::TableErr(err)
    }
}

impl From<FixedPages> for u32 {
    fn from(page: FixedPages) -> Self {
        page as u32
    }
}

impl <'a, D, T, const MAX_DIRS: usize, const MAX_FILES: usize, const MAX_VOLUMES: usize, A: Allocator + Clone + core::fmt::Debug>
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
            let free_page = get_free_page!(self, &mut self.buf2)?;
            self.page_rw.write_page(free_page, self.buf1.as_ref())?;
        }

        Ok(())
    }

    fn serialized_row(&self, table: &Table, row: &Row<'a, A>, allocator: A) -> Result<SerializedRow<A>, InsertErr<D::Error>> {
        let mut payload: Vec<u8, A> = Vec::new_in(allocator.clone());
        let mut null_flags: u64 = 0;
        let mut i = 0;
        let mut key: Vec<u8, A> = Vec::new_in(allocator.clone());

        while i < row.len() {
            let col = &table.columns[i];
            match &row[i] {
                Value::Null => {
                    if col.flags.is_set(Flags::Primary) || !col.flags.is_set(Flags::Nullable) {
                        return Err(InsertErr::CannotBeNull);
                    }
                    if col.flags.is_set(Flags::Foreign) {
                        todo!("foreign key check");
                    }
                    null_flags |= 1 << i;
                },
                Value::Int(val) => {
                    if col.col_type != ColumnType::Int {
                        return Err(InsertErr::TypeDoesNotMatch);
                    }
                    if col.flags.is_set(Flags::Foreign) {
                        todo!("foreign key check");
                    }
                    payload.extend_from_slice(&val.to_be_bytes()); 
                },
                Value::Float(val) => {
                    if col.col_type != ColumnType::Float {
                        return Err(InsertErr::TypeDoesNotMatch);
                    }
                    if col.flags.is_set(Flags::Foreign) {
                        todo!("foreign key check");
                    }
                    payload.extend_from_slice(&val.to_be_bytes());
                },
                Value::Chars(val) => {
                    if col.col_type != ColumnType::Chars {
                        return Err(InsertErr::TypeDoesNotMatch);
                    }
                    if col.flags.is_set(Flags::Foreign) {
                        todo!("foreign key check");
                    }
                    let length = val.len() as u8; 
                    payload.push(length);
                    payload.extend_from_slice(val);
                }

            }

            if table.columns[i].flags.is_set(Flags::Primary) {
                row[i].to_bytes_vec(&mut key);
            }

            i += 1;
        }

        let num_cols = table.col_count;
        let num_bytes = ((num_cols + 7) / 8) as usize;
        let all_bytes = null_flags.to_le_bytes();
        let mut null_flags = Vec::with_capacity_in(num_bytes, allocator.clone());
        null_flags.extend_from_slice(&all_bytes[..num_bytes]);

        Ok(SerializedRow {
            key: key,
            null_flags: null_flags,
            payload: payload,
        })
    }

    pub fn insert_to_table(&mut self, table: u32, row: Row<'_, A>, allocator: A) -> Result<(), InsertErr<D::Error>> {
        unsafe {
            let table = self.page_rw.read_page(table, self.buf1.as_mut());
            let table = as_ref!(self.buf1, Table);

            if table.col_count as usize != row.len() {
                return Err(InsertErr::ColCountDoesNotMatch);
            }

            let serialized_row = self.serialized_row(table, &row, allocator.clone())?;
            // let leaf_page = table.traverse_to_leaf(&mut self.buf2, row[primary_key as usize].to_bytes_vec(&mut key), &self.page_rw)?;
            // let _ = self.page_rw.read_page(leaf_page, self.buf2.as_mut())?;
            // let leaf = as_ref_mut!(self.buf2, BtreeLeaf);
            println!("serialized_row = {:?}", serialized_row);
            // let is_duplicate = leaf.check_duplicate_by_primary_key(primary_key as usize, &row[primary_key as usize]);
            // println!("is_duplicate = {}", is_duplicate);
        }
        Ok(())
    }

    pub fn create_table(&mut self, name: Name, table: &[Column], allocator: A) -> Result<(), Error<D::Error>> {
        unsafe {
            let _ = self.page_rw.read_page(FixedPages::DbCat.into(), self.buf1.as_mut())?;
            let db_cat = as_ref_mut!(self.buf1, Table);

            if db_cat.rows_btree_page == 0 {
                let free_page = get_free_page!(self, &mut self.buf2)?;
                db_cat.rows_btree_page = free_page;
                self.page_rw.write_page(FixedPages::DbCat.into(), self.buf1.as_ref())?;
                let btree_leaf = as_ref_mut!(self.buf2, BtreeLeaf);
                btree_leaf.init();
                self.page_rw.write_page(free_page, self.buf2.as_ref())?;
            }
            let free_page = get_free_page!(self, &mut self.buf2)?;
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
