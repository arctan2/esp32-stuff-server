#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "std")]
use std::println;

use allocator_api2::alloc::Allocator;
use crate::btree;
use crate::btree::{BtreeLeaf, PayloadCellView, Key};
use crate::table::{Table, Column, ColumnType, ToName};
use crate::page_rw::{PageRW};
use crate::fs::{DbDir, PageFile};
use crate::page_buf::{PageBuffer};
use crate::{as_ref_mut, as_ref, get_free_page};
use allocator_api2::vec::Vec;
use crate::serde_row;
use crate::serde_row::{Value, Row};
use crate::query::{Query, QueryExecutor};
use crate::file_handler::FileHandler;
use crate::page_free_list::PageFreeList;

pub struct Database<F, A>
where
    F: PageFile,
    A: Allocator + Clone
{
    pub file_handler: FileHandler<F>,
    pub table_buf: PageBuffer<A>,
    pub buf1: PageBuffer<A>,
    pub buf2: PageBuffer<A>,
    pub buf3: PageBuffer<A>,
    pub buf4: PageBuffer<A>,
}

pub const MAGIC: [u8; 8] = *b"_stufff_";

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
#[repr(packed)]
#[allow(unused)]
pub struct DBHeader {
    magic: [u8; 8],
    page_count: u32,
}

impl DBHeader {
    pub fn inc_page_count<F: PageFile, A: Allocator + Clone>(
        buf: &mut PageBuffer<A>,
        page_rw: &PageRW<F>
    ) -> Result<(), Error<F::Error>> {
        let _ = page_rw.read_page(FixedPages::Header.into(), buf.as_mut())?;
        let header = unsafe { as_ref_mut!(buf, DBHeader) };
        header.page_count += 1;
        let _ = page_rw.write_page(FixedPages::Header.into(), buf.as_mut())?;
        Ok(())
    }

    pub fn get_page_count<F: PageFile, A: Allocator + Clone>(
        buf: &mut PageBuffer<A>,
        page_rw: &PageRW<F>
    ) -> Result<u32, Error<F::Error>> {
        let _ = page_rw.read_page(FixedPages::Header.into(), buf.as_mut())?;
        let header = unsafe { as_ref!(buf, DBHeader) };
        Ok(header.page_count)
    }

    pub fn default() -> Self {
        Self {
            magic: MAGIC,
            page_count: 0,
        }
    }
}

#[derive(Debug)]
pub enum Error<E: core::fmt::Debug> {
    FsError(E),
    InitError,
    InvalidWalFile,
    WalNotSupported,
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

impl<E: core::fmt::Debug> From<E> for Error<E> {
    fn from(err: E) -> Self {
        Error::FsError(err)
    }
}

impl <F, A> Database<F, A>
where
    F: PageFile,
    A: Allocator + Clone
{
    pub fn new_init<'a, D: DbDir<'a, Error = F::Error, File<'a> = F> + 'a>(
        dir: &'a D,
        allocator: A
    ) -> Result<Self, Error<F::Error>> {
        let file_handler = FileHandler::new()?;
        let mut db = Self {
            file_handler: file_handler,
            table_buf: PageBuffer::new(allocator.clone()),
            buf1: PageBuffer::new(allocator.clone()),
            buf2: PageBuffer::new(allocator.clone()),
            buf3: PageBuffer::new(allocator.clone()),
            buf4: PageBuffer::new(allocator.clone()),
        };

        db.file_handler.open_with_wal_check(dir, &mut db.buf1)?;

        let header = db.get_or_create_header()?;
        if header.page_count == 0 {
            db.create_new_db(header)?;
        }

        Ok(db)
    }

    pub fn close<'a, D: DbDir<'a, Error = F::Error, File<'a> = F> + 'a>(
        &mut self,
        dir: &'a D
    ) -> Result<(), Error<F::Error>> {
        self.file_handler.close(dir)
    }

    fn get_or_create_header(&mut self) -> Result<DBHeader, Error<F::Error>> {
        let count = self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?
                    .read_page(FixedPages::Header.into(), self.buf1.as_mut())?;
        if count == 0 {
            let header = DBHeader::default();
            unsafe {
                self.buf1.write(0, &header);
            }
            self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?
                .write_page(FixedPages::Header.into(), self.buf1.as_ref())?;
        }
        return unsafe {
            Ok(self.buf1.read::<DBHeader>(0))
        };
    }

    fn create_new_db(&mut self, mut header: DBHeader) -> Result<(), Error<F::Error>> {
        header.page_count = 2;
        unsafe {
            self.buf1.write(0, &header);
            let _ = self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?
                    .write_page(FixedPages::Header.into(), self.buf1.as_ref())?;
        }
        let _ = self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?.extend_file_one_page(2, self.buf1.as_mut())?;
        let tbl_name = Column::new("tbl_name", ColumnType::Chars).primary();
        let page = Column::new("page", ColumnType::Int);
        self.new_table_begin("db_cat");
        self.add_column(tbl_name)?;
        self.add_column(page)?;

        unsafe {
            let free_page = get_free_page!(self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?, &mut self.buf1)?;
            self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?.write_page(free_page, self.table_buf.as_ref())?;
        }

        Ok(())
    }

    pub fn insert_to_table(&mut self, table_page: u32, row: Row<'_, A>, allocator: A) -> Result<(), Error<F::Error>> {
        let _ = self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?.read_page(table_page, self.table_buf.as_mut())?;
        let table = unsafe { as_ref_mut!(self.table_buf, Table) }; 

        if table.col_count as usize != row.len() {
            return Err(Error::ColCountDoesNotMatch);
        }

        if table.rows_btree_page == 0 {
            let free_page = unsafe {
                get_free_page!(self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?, &mut self.buf1)?
            };

            table.rows_btree_page = free_page;
            self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?.write_page(table_page, self.table_buf.as_ref())?;
            let btree_leaf = unsafe { as_ref_mut!(self.buf1, BtreeLeaf) }; 
            btree_leaf.init();
            self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?.write_page(free_page, self.buf1.as_ref())?;
        }

        let serialized_row = serde_row::serialize(table, &row, allocator.clone())?;

        PayloadCellView::new_to_buf(
            table, self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?,
            serialized_row, &mut self.buf1, &mut self.buf2
        )?;

        let payload_cell = PayloadCellView::new(table, self.buf1.as_ref(), 0);
        let mut path = Vec::new_in(allocator.clone());
        let leaf_page = btree::traverse_to_leaf_with_path(
            table, &mut self.buf2, payload_cell.key(),
            self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?,
            &mut path
        )?;


        self.file_handler.wal_begin_write(&mut self.buf2)?; {
            self.file_handler.wal_append_pages_vec(&path, &mut self.buf2)?;
            self.file_handler.wal_append_page(leaf_page, &mut self.buf2)?;
        }
        self.file_handler.wal_end_write()?;

        btree::insert_payload_to_leaf(
            &mut self.buf1, &mut self.buf2,
            &mut self.buf3, &mut self.buf4,
            leaf_page, table,
            self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?, path,
            allocator.clone()
        )?;
        self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?.write_page(table_page, self.table_buf.as_ref())?;

        self.file_handler.end_wal()?;

        Ok(())
    }

    pub fn delete_from_table(&mut self, table_page: u32, key: Value<'_>, allocator: A) -> Result<(), Error<F::Error>> {
        let _ = self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?.read_page(table_page, self.table_buf.as_mut())?;
        let table = unsafe { as_ref_mut!(self.table_buf, Table) };

        if table.rows_btree_page == 0 {
            return Err(Error::NotFound);
        }

        key.to_key(&mut self.buf1);
        let key = unsafe { as_ref!(self.buf1, Key) };
        let mut path: Vec<u32, A> = Vec::new_in(allocator.clone());
        let leaf_page = btree::traverse_to_leaf_with_path(
            table, &mut self.buf2, key,
            self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?,
            &mut path
        )?;

        btree::delete_payload_from_leaf(
            &mut self.buf1, &mut self.buf2,
            &mut self.buf3, &mut self.buf4,
            leaf_page, table,
            self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?, path,
            allocator.clone()
        )?;
        self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?.write_page(table_page, self.table_buf.as_ref())?;
        Ok(())
    }

    pub fn get_col_idx(&mut self, table: u32, name: impl ToName) -> Option<u16> {
        let _ = self.file_handler.page_rw.as_ref().unwrap().read_page(table, self.table_buf.as_mut());
        let table = unsafe { as_ref!(self.table_buf, Table) };
        match table.get_col_idx_by_name(name) {
            Some(v) => Some(v as u16),
            None => None
        }
    }

    pub fn get_table<N: ToName>(&mut self, name: N, allocator: A) -> Result<u32, Error<F::Error>> {
        let db_cat_page = FixedPages::DbCat.into();
        let n = name.to_name();
        let query: Query<'_, _, N> = Query::new(db_cat_page, allocator.clone()).key(Value::Chars(&n));

        let mut exec = QueryExecutor::new(
            query, &mut self.table_buf, &mut self.buf1, &mut self.buf2,
            self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?
        )?;

        let row = exec.next()?;

        return match row[1] {
            Value::Int(page) => Ok(page as u32),
            _ => Err(Error::NotFound)
        };
    }

    #[cfg(feature = "std")]
    pub fn print_all_table<N: ToName>(&mut self, allocator: A) {
        let db_cat_page = FixedPages::DbCat.into();
        let query: Query<'_, _, N> = Query::new(db_cat_page, allocator.clone());

        let mut exec = QueryExecutor::new(
            query, &mut self.table_buf, &mut self.buf2, &mut self.buf1,
            self.file_handler.page_rw.as_ref().unwrap()
        ).unwrap();
        while let Ok(row) = exec.next() {
            println!("table = {:?}", row);
        }
    }

    pub fn create_table(&mut self, allocator: A) -> Result<u32, Error<F::Error>> {
        let table = unsafe { as_ref_mut!(self.table_buf, Table) };

        let free_page = unsafe { get_free_page!(self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?, &mut self.buf1)? };
        self.file_handler.page_rw.as_ref().ok_or(Error::InitError)?.write_page(free_page, self.table_buf.as_ref())?;

        let mut row = Row::new_in(allocator.clone());
        let name = table.name.clone();
        row.push(Value::Chars(&name));
        row.push(Value::Int(free_page as i64));
        self.insert_to_table(FixedPages::DbCat.into(), row, allocator)?;

        Ok(free_page)
    }

    pub fn new_table_begin(&mut self, name: impl ToName) {
        self.table_buf.as_mut().fill(0);
        let table = unsafe { as_ref_mut!(self.table_buf, Table) };
        table.name = name.to_name();
    }

    pub fn add_column(&mut self, col: Column) -> Result<(), Error<F::Error>> {
        let table = unsafe { as_ref_mut!(self.table_buf, Table) };
        table.add_column(col)
    }
}

#[cfg(test)]
mod test {
    use crate::embedded_sdmmc_ram_device::{allocators, block_device, esp_alloc, timesource};
    use crate::embedded_sdmmc_ram_device::fs::{DbDirSdmmc};
    use crate::{Column, ColumnType, Value, Row, Query, QueryExecutor};
    use embedded_sdmmc::{VolumeManager, BlockDevice};
    use crate::db;
    use crate::Operations;

    #[cfg(feature = "std")]
    extern crate std;

    #[cfg(not(feature = "hw_failure_test"))]
    #[test]
    pub fn table_basic_operations() {
        allocators::init_simulated_hardware();
        let sdcard = block_device::FsBlockDevice::new("test_file.db").unwrap();
        let vol_man = VolumeManager::new(sdcard, timesource::DummyTimesource);
        let volume = vol_man.open_volume(embedded_sdmmc::VolumeIdx(0)).unwrap();
        let root_dir = volume.open_root_dir().unwrap();
        let _ = root_dir.make_dir_in_dir("STUFF").unwrap();
        let stuff_dir = DbDirSdmmc::new(root_dir.open_dir("STUFF").unwrap());
        let mut db = db::Database::new_init(&stuff_dir, esp_alloc::ExternalMemory).unwrap();

        let allocator = esp_alloc::ExternalMemory;

        {
            let col1 = Column::new("col1", ColumnType::Chars).primary();
            let col2 = Column::new("col2", ColumnType::Int);
            db.new_table_begin("cool_table");
            db.add_column(col1).unwrap();
            db.add_column(col2).unwrap();
            let _ = db.create_table(allocator.clone()).unwrap();
        }
        let cool_table = db.get_table("cool_table", allocator.clone()).unwrap();

        for i in 0..10 {
            let col1 = std::format!("cool_col1_value_{}", i);
            let mut row = Row::new_in(allocator.clone());
            row.push(Value::Chars(col1.as_bytes()));
            row.push(Value::Int(i as i64));
            db.insert_to_table(cool_table, row, allocator.clone()).unwrap();
        }

        for i in 0..5 {
            let col1 = std::format!("cool_col1_value_{}", i);
            db.delete_from_table(cool_table, Value::Chars(col1.as_bytes()), allocator.clone()).unwrap();
        }

        let cool_table = db.get_table("cool_table", allocator.clone()).unwrap();

        {
            let query = Query::<_, &str>::new(cool_table, allocator.clone());
            let mut exec = QueryExecutor::new(
                query, &mut db.table_buf, &mut db.buf1, &mut db.buf2,
                &db.file_handler.page_rw.as_ref().unwrap()
            ).unwrap();

            let mut i = 5;
            while let Ok(row) = exec.next() {
                let col1 = std::format!("cool_col1_value_{}", i);
                assert_eq!(row[0].eq(&Value::Chars(col1.as_bytes())), true);
                assert_eq!(row[1].eq(&Value::Int(i as i64)), true);
                i += 1;
            }
        }

        {
            let query = Query::<_, &str>::new(cool_table, allocator.clone());
            let mut exec = QueryExecutor::new(
                query, &mut db.table_buf, &mut db.buf1, &mut db.buf2,
                &db.file_handler.page_rw.as_ref().unwrap()
            ).unwrap();

            assert_eq!(exec.count().unwrap(), 5);
        }
    }

    fn failure_phase(sdcard: impl BlockDevice + core::panic::UnwindSafe) -> Result<bool, ()> {
        let ret = std::panic::catch_unwind(|| {
            let vol_man = VolumeManager::new(sdcard, timesource::DummyTimesource);
            let volume = vol_man.open_volume(embedded_sdmmc::VolumeIdx(0)).unwrap();
            let root_dir = volume.open_root_dir().unwrap();
            let _ = root_dir.make_dir_in_dir("STUFF").unwrap();
            let stuff_dir = DbDirSdmmc::new(root_dir.open_dir("STUFF").unwrap());
            let mut db = db::Database::new_init(&stuff_dir, esp_alloc::ExternalMemory).unwrap();

            let allocator = esp_alloc::ExternalMemory;

            {
                let col1 = Column::new("col1", ColumnType::Chars).primary();
                let col2 = Column::new("col2", ColumnType::Int);
                db.new_table_begin("cool_table");
                db.add_column(col1).unwrap();
                db.add_column(col2).unwrap();
                let _ = db.create_table(allocator.clone()).unwrap();
            }
            let cool_table = db.get_table("cool_table", allocator.clone()).unwrap();

            for i in 0..10 {
                let col1 = std::format!("cool_col1_value_{}", i);
                let mut row = Row::new_in(allocator.clone());
                row.push(Value::Chars(col1.as_bytes()));
                row.push(Value::Int(i as i64));
                db.insert_to_table(cool_table, row, allocator.clone()).unwrap();
            }
        });

        Ok(match ret {
            Ok(_) => false,
            Err(_) => true
        })
    }

    fn recovery_phase<B: BlockDevice + core::panic::UnwindSafe>(sdcard: B) -> Result<bool, db::Error<B::Error>> {
        let ret = std::panic::catch_unwind(|| {
            let vol_man = embedded_sdmmc::VolumeManager::new(sdcard, timesource::DummyTimesource);
            let volume = vol_man.open_volume(embedded_sdmmc::VolumeIdx(0)).unwrap();
            let root_dir = volume.open_root_dir().unwrap();
            let _ = root_dir.make_dir_in_dir("STUFF");
            let stuff_dir = DbDirSdmmc::new(root_dir.open_dir("STUFF").unwrap());
            let mut db = db::Database::new_init(&stuff_dir, esp_alloc::ExternalMemory).unwrap();

            let allocator = esp_alloc::ExternalMemory;
            let cool_table = db.get_table("cool_table", allocator.clone()).unwrap();

            for i in 2..10 {
                let col1 = std::format!("cool_col1_value_{}", i);
                let mut row = Row::new_in(allocator.clone());
                row.push(Value::Chars(col1.as_bytes()));
                row.push(Value::Int(i as i64));
                db.insert_to_table(cool_table, row, allocator.clone()).unwrap();
            }

            for i in 0..5 {
                let col1 = std::format!("cool_col1_value_{}", i);
                db.delete_from_table(cool_table, Value::Chars(col1.as_bytes()), allocator.clone()).unwrap();
            }

            let cool_table = db.get_table("cool_table", allocator.clone()).unwrap();

            {
                let query = Query::<_, &str>::new(cool_table, allocator.clone());
                let mut exec = QueryExecutor::new(
                    query, &mut db.table_buf, &mut db.buf1, &mut db.buf2,
                    &db.file_handler.page_rw.as_ref().unwrap()
                ).unwrap();

                let mut i = 5;
                while let Ok(row) = exec.next() {
                    let col1 = std::format!("cool_col1_value_{}", i);
                    assert_eq!(row[0].eq(&Value::Chars(col1.as_bytes())), true);
                    assert_eq!(row[1].eq(&Value::Int(i as i64)), true);
                    i += 1;
                }
            }

            {
                let query = Query::<_, &str>::new(cool_table, allocator.clone());
                let mut exec = QueryExecutor::new(
                    query, &mut db.table_buf, &mut db.buf1, &mut db.buf2,
                    &db.file_handler.page_rw.as_ref().unwrap()
                ).unwrap();

                assert_eq!(exec.count().unwrap(), 5);
            }
        });

        Ok(match ret {
            Ok(_) => true,
            Err(_) => false
        })
    }

    // this test needs WRITES_REM = 27 and PANICS_REM = 1
    #[test]
    #[cfg(feature = "hw_failure_test")]
    pub fn random_hardware_failure() {
        allocators::init_simulated_hardware();
        let sdcard = block_device::FsBlockDevice::new("test_file.db").unwrap();
        assert!(failure_phase(sdcard).unwrap());
        let sdcard = block_device::FsBlockDevice::from_existing("test_file.db").unwrap();
        assert!(recovery_phase(sdcard).unwrap());
    }
}
