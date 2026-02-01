use embedded_sdmmc::{File, BlockDevice, TimeSource};
use allocator_api2::alloc::Allocator;
use crate::btree;
use crate::btree::{BtreeLeaf, PayloadCellView, Key};
use crate::table::{Table, Column, ColumnType, ToName, Name};
use crate::PageRW;
use crate::types::PageBuffer;
use crate::{PageFreeList, as_ref_mut, as_ref, get_free_page};
use allocator_api2::vec::Vec;
use crate::serde_row;
use crate::serde_row::{Value, Row};
use crate::query::{Query, QueryExecutor};

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

    pub fn default() -> Self {
        Self {
            magic: MAGIC,
            page_count: 0,
        }
    }
}

#[repr(u32)]
pub enum FixedPages {
    Header = 0,
    FreeList = 1,
    DbCat = 2
}

impl From<FixedPages> for u32 {
    fn from(page: FixedPages) -> Self {
        page as u32
    }
}

#[derive(Debug)]
pub enum Error<E: core::fmt::Debug> {
    SdmmcErr(embedded_sdmmc::Error<E>),
    FreeListNotFound,
    HeaderNotFound,
    // query errors
    EndOfRecords,
    ColumnNotFound,
    InvalidOperands,
    MissingOperands,
    //table errors
    MaxColumnsReached,
    NotFound,
    TableEmpty,
    //insert errors
    MissingPrimaryKey,
    ColCountDoesNotMatch,
    CannotBeNull,
    TypeDoesNotMatch,
    CharsTooLong,
    DuplicateKey,
}

impl<DErr> From<embedded_sdmmc::Error<DErr>> for Error<DErr> where DErr: core::fmt::Debug {
    fn from(err: embedded_sdmmc::Error<DErr>) -> Self {
        Error::SdmmcErr(err)
    }
}

impl <
    'a, D, T, A,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize,
    
> Database<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES, A>
where
    D: BlockDevice,
    T: TimeSource,
    A: Allocator + Clone + core::fmt::Debug
{
    pub fn new(file: File<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>, allocator: A) -> Self {
        return Self {
            page_rw: PageRW::new(file),
            table_buf: PageBuffer::new(allocator.clone()),
            buf1: PageBuffer::new(allocator.clone()),
            buf2: PageBuffer::new(allocator.clone()),
            buf3: PageBuffer::new(allocator.clone()),
            buf4: PageBuffer::new(allocator.clone()),
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
        let tbl_name = Column::new("tbl_name".to_name(), ColumnType::Chars).primary();
        let page = Column::new("page".to_name(), ColumnType::Int);
        self.new_table_begin("db_cat".to_name());
        self.add_column(tbl_name)?;
        self.add_column(page)?;

        unsafe {
            let free_page = get_free_page!(&self.page_rw, &mut self.buf1)?;
            self.page_rw.write_page(free_page, self.table_buf.as_ref())?;
        }

        Ok(())
    }

    pub fn insert_to_table(&mut self, table_page: u32, row: Row<'_, A>, allocator: A) -> Result<(), Error<D::Error>> {
        let _ = self.page_rw.read_page(table_page, self.table_buf.as_mut())?;
        let table = unsafe { as_ref_mut!(self.table_buf, Table) }; 

        if table.col_count as usize != row.len() {
            return Err(Error::ColCountDoesNotMatch);
        }

        if table.rows_btree_page == 0 {
            let free_page = unsafe { get_free_page!(&self.page_rw, &mut self.buf1)? };
            table.rows_btree_page = free_page;
            self.page_rw.write_page(table_page, self.table_buf.as_ref())?;
            let btree_leaf = unsafe { as_ref_mut!(self.buf1, BtreeLeaf) }; 
            btree_leaf.init();
            self.page_rw.write_page(free_page, self.buf1.as_ref())?;
        }

        let serialized_row = serde_row::serialize(table, &row, allocator.clone())?;

        PayloadCellView::new_to_buf(table, &self.page_rw, serialized_row, &mut self.buf1, &mut self.buf2)?;
        let payload_cell = PayloadCellView::new(table, self.buf1.as_ref(), 0);
        let mut path = Vec::new_in(allocator.clone());
        let leaf_page = btree::traverse_to_leaf_with_path(table, &mut self.buf2, payload_cell.key(), &self.page_rw, &mut path)?;
        btree::insert_payload_to_leaf(
            &mut self.buf1, &mut self.buf2,
            &mut self.buf3, &mut self.buf4,
            leaf_page, table,
            &self.page_rw, path,
            allocator.clone()
        )?;
        self.page_rw.write_page(table_page, self.table_buf.as_ref())?;
        Ok(())
    }

    pub fn delete_from_table(&mut self, table_page: u32, key: Value<'_>, allocator: A) -> Result<(), Error<D::Error>> {
        let _ = self.page_rw.read_page(table_page, self.table_buf.as_mut())?;
        let table = unsafe { as_ref_mut!(self.table_buf, Table) };

        if table.rows_btree_page == 0 {
            return Err(Error::NotFound);
        }

        key.to_key(&mut self.buf1);
        let key = unsafe { as_ref!(self.buf1, Key) };
        let mut path: Vec<u32, A> = Vec::new_in(allocator.clone());
        let leaf_page = btree::traverse_to_leaf_with_path(table, &mut self.buf2, key, &self.page_rw, &mut path)?;

        btree::delete_payload_from_leaf(
            &mut self.buf1, &mut self.buf2,
            &mut self.buf3, &mut self.buf4,
            leaf_page, table,
            &self.page_rw, path,
            allocator.clone()
        )?;
        self.page_rw.write_page(table_page, self.table_buf.as_ref())?;
        Ok(())
    }

    pub fn get_col_idx(&mut self, table: u32, name: Name) -> Option<u16> {
        let _ = self.page_rw.read_page(table, self.table_buf.as_mut());
        let table = unsafe { as_ref!(self.table_buf, Table) };
        match table.get_col_idx_by_name(name) {
            Some(v) => Some(v as u16),
            None => None
        }
    }

    pub fn get_table(&mut self, name: Name, allocator: A) -> Result<u32, Error<D::Error>> {
        let db_cat_page = FixedPages::DbCat.into();
        let query = Query::new(db_cat_page, allocator.clone()).key(Value::Chars(&name));

        let mut exec = QueryExecutor::new(query, &mut self.table_buf, &mut self.buf1, &mut self.buf2, &self.page_rw)?;

        let row = exec.next()?;

        return match row[1] {
            Value::Int(page) => Ok(page as u32),
            _ => Err(Error::NotFound)
        };
    }

    pub fn print_all_table(&mut self, allocator: A) {
        let db_cat_page = FixedPages::DbCat.into();
        let query = Query::new(db_cat_page, allocator.clone());

        let mut exec = QueryExecutor::new(query, &mut self.table_buf, &mut self.buf2, &mut self.buf1, &self.page_rw).unwrap();
        while let Ok(row) = exec.next() {
            println!("table = {:?}", row);
        }
    }

    pub fn create_table(&mut self, allocator: A) -> Result<u32, Error<D::Error>> {
        unsafe {
            let table = as_ref_mut!(self.table_buf, Table);

            let free_page = get_free_page!(&self.page_rw, &mut self.buf1)?;
            self.page_rw.write_page(free_page, self.table_buf.as_ref())?;

            let mut row = Row::new_in(allocator.clone());
            let name = table.name.clone();
            row.push(Value::Chars(&name));
            row.push(Value::Int(free_page as i64));
            self.insert_to_table(FixedPages::DbCat.into(), row, allocator)?;

            Ok(free_page)
        }
    }

    pub fn new_table_begin(&mut self, name: Name) {
        self.table_buf.as_mut().fill(0);
        let table = unsafe { as_ref_mut!(self.table_buf, Table) };
        table.name = name;
    }

    pub fn add_column(&mut self, col: Column) -> Result<(), Error<D::Error>> {
        let table = unsafe { as_ref_mut!(self.table_buf, Table) };
        table.add_column(col)
    }

    pub fn init(&mut self, allocator: A) -> Result<(), Error<D::Error>> {
        let header = self.get_or_create_header()?;
        if header.page_count == 0 {
            self.create_new_db(header)?;
        }

        {
            let path = Column::new("path".to_name(), ColumnType::Chars).primary();
            let size = Column::new("size".to_name(), ColumnType::Int);
            let name = Column::new("name".to_name(), ColumnType::Chars);
            self.new_table_begin("files".to_name());
            self.add_column(path)?;
            self.add_column(size)?;
            self.add_column(name)?;
            let _ = self.create_table(allocator.clone())?;
        }

        {
            let path = Column::new("cool_path".to_name(), ColumnType::Chars).primary();
            self.new_table_begin("fav".to_name());
            self.add_column(path)?;
            let fav = self.create_table(allocator.clone())?;
            let mut row = Row::new_in(allocator.clone());
            row.push(Value::Chars(b"/some/file.txt"));
            self.insert_to_table(fav, row, allocator.clone())?;
        }

        let files = self.get_table("files".to_name(), allocator.clone()).unwrap();

        {
            use rand::{SeedableRng, seq::SliceRandom};
            use rand::rngs::StdRng;

            let to = 1000;
            let mut rng = StdRng::seed_from_u64(42);
            let mut ids: Vec<usize> = (0..to).collect();
            ids.shuffle(&mut rng);

            for i in ids.iter() {
                let path = format!("/some/file_{}.txt", i);
                let mut row = Row::new_in(allocator.clone());
                row.push(Value::Chars(path.as_bytes()));
                row.push(Value::Int(*i as i64));
                row.push(Value::Chars(b"file.txt"));
                self.insert_to_table(files, row, allocator.clone())?;
            }

            for i in 0..900 {
                let path = format!("/some/file_{}.txt", i);
                self.delete_from_table(files, Value::Chars(path.as_bytes()), allocator.clone())?;
            }
        }

        // {
        //     let files = self.get_table("files".to_name(), allocator.clone()).unwrap();
        //     let query = Query::new(files, allocator.clone());
        //     let mut exec = QueryExecutor::new(query, &mut self.table_buf, &mut self.buf1, &self.page_rw)?;
        //     let mut payload: Vec<u8, A> = Vec::new_in(allocator.clone());
        //     let mut row: Row<A> = Row::new_in(allocator.clone());
        //     while let Ok(_) = exec.next(&mut self.buf2, &mut payload, &mut row, &self.page_rw) {
        //         println!("row = {:?}", row);
        //     }
        // }

        println!("after deleting all: ");

        {
            let files = self.get_table("files".to_name(), allocator.clone()).unwrap();
            let query = Query::new(files, allocator.clone());
            let mut exec = QueryExecutor::new(query, &mut self.table_buf, &mut self.buf1, &mut self.buf2, &self.page_rw)?;

            while let Ok(row) = exec.next() {
                println!("row = {:?}", row);
            }
        }

        Ok(())
    }
}
