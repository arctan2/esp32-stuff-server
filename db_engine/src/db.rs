use embedded_sdmmc::{File, BlockDevice, TimeSource};
use allocator_api2::alloc::Allocator;
use crate::btree;
use crate::btree::{BtreeLeaf, PayloadCellView, Key};
use crate::table::{Table, Column, ColumnType, Flags, TableErr, ToName, Name};
use crate::PageRW;
use crate::types::PageBufferWriter;
use crate::types::PageBuffer;
use crate::overflow::OverflowPage;
use crate::{PageFreeList, as_ref_mut, as_ref, get_free_page, add_page_to_free_list};
use allocator_api2::vec::Vec;
use crate::serde_row;
use crate::serde_row::{Value, Row};

pub struct Database<'a, D, T, const MAX_DIRS: usize, const MAX_FILES: usize, const MAX_VOLUMES: usize, A: Allocator + Clone>
where
    D: BlockDevice,
    T: TimeSource,
{
    page_rw: PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    table_buf: PageBuffer<A>,
    buf1: PageBuffer<A>,
    buf2: PageBuffer<A>,
    buf3: PageBuffer<A>,
    buf4: PageBuffer<A>,
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

#[repr(u32)]
pub enum FixedPages {
    Header = 0,
    FreeList = 1,
    DbCat = 2
}

#[derive(Debug)]
pub enum InsertErr {
    ColCountDoesNotMatch,
    CannotBeNull,
    TypeDoesNotMatch,
    CharsTooLong,
    DuplicateKey
}

impl From<FixedPages> for u32 {
    fn from(page: FixedPages) -> Self {
        page as u32
    }
}

#[derive(Debug)]
pub enum Error<E: core::fmt::Debug> {
    SdmmcErr(embedded_sdmmc::Error<E>),
    Insert(InsertErr),
    TableErr(TableErr<E>),
    FreeListNotFound,
    HeaderNotFound,
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

impl<E> From<InsertErr> for Error<E> where E: core::fmt::Debug {
    fn from(err: InsertErr) -> Self {
        Error::Insert(err)
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
            table_buf: PageBuffer::new(allocator.clone()),
            buf1: PageBuffer::new(allocator.clone()),
            buf2: PageBuffer::new(allocator.clone()),
            buf3: PageBuffer::new(allocator.clone()),
            buf4: PageBuffer::new(allocator),
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
            let free_page = get_free_page!(&self.page_rw, &mut self.buf2)?;
            self.page_rw.write_page(free_page, self.buf1.as_ref())?;
        }

        Ok(())
    }

    pub fn insert_to_table(&mut self, table_page: u32, row: Row<'_, A>, allocator: A) -> Result<(), Error<D::Error>> {
        unsafe {
            let _ = self.page_rw.read_page(table_page, self.table_buf.as_mut())?;
            let table = as_ref_mut!(self.table_buf, Table);

            if table.col_count as usize != row.len() {
                return Err(Error::Insert(InsertErr::ColCountDoesNotMatch));
            }

            let serialized_row = serde_row::serialize(table, &row, allocator.clone())?;
            PayloadCellView::new_to_buf(table, &self.page_rw, serialized_row, &mut self.buf2, &mut self.buf3)?;
            let payload_cell = PayloadCellView::new(table, self.buf2.as_ref(), 0);
            let mut path = Vec::new_in(allocator.clone());
            let leaf_page = btree::traverse_to_leaf(table, &mut self.buf3, payload_cell.key(), &self.page_rw, &mut path)?;
            btree::insert_payload_to_leaf(
                &mut self.buf2, &mut self.buf3,
                &mut self.buf1, &mut self.buf4,
                leaf_page, table,
                &self.page_rw, path,
                allocator.clone()
            )?;
            self.page_rw.write_page(table_page, self.table_buf.as_ref())?;
        }
        Ok(())
    }

    pub fn get_table(&mut self, name: Name, allocator: A) -> Result<u32, Error<D::Error>> {
        unsafe {
            let _ = self.page_rw.read_page(FixedPages::DbCat.into(), self.table_buf.as_mut())?;
            let table = as_ref!(self.table_buf, Table);
            let mut buf_writer = PageBufferWriter::new(&mut self.buf1);
            let len = name.len();
            buf_writer.write(&(len as u8));
            buf_writer.write_slice(&name);
            let key = as_ref!(self.buf1, Key);
            let _ = btree::traverse_to_leaf_no_path(table, &mut self.buf2, key, &self.page_rw)?;
            let leaf = as_ref_mut!(self.buf2, BtreeLeaf);
            let cell = match leaf.find_payload_by_key(table, key) {
                Some(c) => c,
                None => return Err(Error::TableErr(TableErr::NotFound))
            };
            let mut payload: Vec<u8, A> = Vec::with_capacity_in(cell.header.payload_total_len as usize, allocator.clone());
            let mut row = Row::new_in(allocator.clone());
            payload.extend_from_slice(cell.payload(table.get_null_flags_width_bytes()));
            if cell.header.payload_overflow > 0 {
                 OverflowPage::read_all(&self.page_rw, cell.header.payload_overflow, &mut payload, &mut self.buf3)?;
            }
            serde_row::deserialize(table, &mut row, &mut payload);
            return Ok(row[1].to_int().unwrap() as u32);
        }
    }

    pub fn create_table(&mut self, table: Table, allocator: A) -> Result<(), Error<D::Error>> {
        unsafe {
            table.write_to_buf(&mut self.table_buf);
            let _ = self.page_rw.read_page(FixedPages::DbCat.into(), self.buf1.as_mut())?;
            let db_cat = as_ref_mut!(self.buf1, Table);

            if db_cat.rows_btree_page == 0 {
                let free_page = get_free_page!(&self.page_rw, &mut self.buf2)?;
                db_cat.rows_btree_page = free_page;
                self.page_rw.write_page(FixedPages::DbCat.into(), self.buf1.as_ref())?;
                let btree_leaf = as_ref_mut!(self.buf2, BtreeLeaf);
                btree_leaf.init();
                self.page_rw.write_page(free_page, self.buf2.as_ref())?;
            }
            let free_page = get_free_page!(&self.page_rw, &mut self.buf2)?;
            let mut row = Row::new_in(allocator.clone());
            row.push(Value::Chars(&table.name));
            row.push(Value::Int(free_page as i64));
            self.insert_to_table(FixedPages::DbCat.into(), row, allocator)?;
            self.page_rw.write_page(free_page, self.table_buf.as_ref())?;
        }
        Ok(())
    }

    pub fn init(&mut self, allocator: A) -> Result<(), Error<D::Error>> {
        let header = self.get_or_create_header()?;
        if header.page_count == 0 {
            self.create_new_db(header)?;
        }

        for i in 0..1 {
            let path = Column::new("path".to_name(), ColumnType::Chars, Flags::Primary);
            let size = Column::new("size".to_name(), ColumnType::Int, Flags::None);
            let name = Column::new("name".to_name(), ColumnType::Chars, Flags::None);
            let table = Table::create(format!("table_{}", i + 1).to_name())
                .add_column(path)?
                .add_column(size)?
                .add_column(name)?;
            let _ = self.create_table(table, allocator.clone())?;
        }

        println!("table = {}", self.get_table("table_5".to_name(), allocator.clone()).unwrap());
        
        Ok(())
    }
}
