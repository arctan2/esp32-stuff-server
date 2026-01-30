use embedded_sdmmc::{File, BlockDevice, TimeSource};
use allocator_api2::alloc::Allocator;
use crate::btree;
use crate::btree::{BtreeLeaf, PayloadCellView, Cursor};
use crate::table::{Table, Column, ColumnType, TableErr, ToName, Name};
use crate::PageRW;
use crate::types::PageBuffer;
use crate::overflow::OverflowPage;
use crate::{PageFreeList, as_ref_mut, as_ref, get_free_page, add_page_to_free_list};
use allocator_api2::vec::Vec;
use crate::serde_row;
use crate::serde_row::{Value, Row, Operations};

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
    DuplicateKey,
    // RefKeyNotExist,
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
    EndOfRecords,
    ColumnNotFound,
    InvalidOperands,
    MissingOperands
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

#[derive(Debug, PartialEq)]
pub enum Operator {
    Eq,
    Gt,
    Lt,
    StartsWith,
    EndsWith,
    Contains,
    IsNull
}

impl Operator {
    fn eval<'a>(&self, lhs: &Value<'a>, rhs: &Value<'a>) -> bool {
        match self {
            Operator::Eq => lhs.eq(rhs),
            Operator::Gt => lhs.gt(rhs),
            Operator::Lt => lhs.lt(rhs),
            Operator::StartsWith => lhs.starts_with(rhs),
            Operator::EndsWith => lhs.ends_with(rhs),
            Operator::Contains => lhs.contains(rhs),
            Operator::IsNull => lhs.is_null()
        }
    }
}

#[derive(Debug)]
pub enum Expr<'a> {
    Val(Value<'a>),
    // Col(ColumnName) // later (hopefully) when I decide to do joins and stuff this will be used
    Col(Name)
}

#[derive(Debug)]
pub struct ColumnName {
    table: u32,
    name: Name
}

#[derive(Debug)]
pub struct Op<'a> {
    lhs: Expr<'a>,
    op: Operator,
    rhs: Option<Expr<'a>>,
}

impl <'a> Op<'a> {
    pub fn eq(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self { lhs, op: Operator::Eq, rhs: Some(rhs) }
    }

    pub fn gt(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self { lhs, op: Operator::Gt, rhs: Some(rhs) }
    }

    pub fn lt(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self { lhs, op: Operator::Lt, rhs: Some(rhs) }
    }

    pub fn starts_with(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self { lhs, op: Operator::StartsWith, rhs: Some(rhs) }
    }

    pub fn ends_with(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self { lhs, op: Operator::EndsWith, rhs: Some(rhs) }
    }

    pub fn contains(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self { lhs, op: Operator::Contains, rhs: Some(rhs) }
    }

    pub fn is_null(lhs: Expr<'a>) -> Self {
        Self { lhs, op: Operator::IsNull, rhs: None }
    }
}

#[derive(Debug)]
pub enum Condition<'a> {
    Is(Op<'a>),
    Not(Op<'a>),
}

pub enum TopLevelOperator<'a, A: Allocator + Clone> {
    And(Vec<Condition<'a>, A>),
    Or(Vec<Condition<'a>, A>)
}

pub struct Limit(usize, usize);

pub struct Query<'a, A: Allocator + Clone> {
    allocator: A,
    target_table: u32,
    // tables: Vec<u32, A>,
    filters: TopLevelOperator<'a, A>,
    // project: Vec<ColumnName, A>,
    limit: Option<Limit>,
}

impl <'a, A> Query<'a, A> where A: Allocator + Clone {
    pub fn new(target_table: u32, allocator: A) -> Self {
        Self {
            allocator: allocator.clone(),
            target_table: target_table,
            filters: TopLevelOperator::And(Vec::new_in(allocator.clone())),
            // key: None,
            limit: None,
        }
    }

    pub fn and(mut self) -> Self {
        self.filters = TopLevelOperator::And(Vec::new_in(self.allocator.clone()));
        self
    }

    pub fn or(mut self) -> Self {
        self.filters = TopLevelOperator::Or(Vec::new_in(self.allocator.clone()));
        self
    }

    pub fn not(mut self, op: Op<'a>) -> Self {
        match self.filters {
            TopLevelOperator::And(ref mut v) => v.push(Condition::Not(op)),
            TopLevelOperator::Or(ref mut v) => v.push(Condition::Not(op))
        };
        self
    }

    pub fn is(mut self, op: Op<'a>) -> Self {
        match self.filters {
            TopLevelOperator::And(ref mut v) => v.push(Condition::Is(op)),
            TopLevelOperator::Or(ref mut v) => v.push(Condition::Is(op))
        };
        self
    }

    // pub fn key(mut self, name: Name) -> Self {
    //     self.project.push(ColumnName { table: self.target_table, name: name });
    //     self
    // }

    // pub fn grab_from_table(mut self, table: u32, name: Name) -> Self {
    //     self.project.push(ColumnName{ table: table, name: name });
    //     self
    // }

    // pub fn grab(mut self, name: Name) -> Self {
    //     self.project.push(ColumnName { table: self.target_table, name: name });
    //     self
    // }
}

pub struct QueryExecutor<'a, A: Allocator + Clone> {
    table_buf: &'a mut PageBuffer<A>,
    cursor: Cursor<'a, A>,
    query: Query<'a, A>,
}

impl <'a, A: Allocator + Clone> QueryExecutor<'a, A> {
    pub fn new<
        D: BlockDevice, T: TimeSource,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    >(
        query: Query<'a, A>,
        table_buf: &'a mut PageBuffer<A>,
        cursor_buf: &'a mut PageBuffer<A>,
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> Result<QueryExecutor<'a, A>, Error<D::Error>> {
        let _ = page_rw.read_page(query.target_table, table_buf.as_mut());
        let table = unsafe { as_ref!(table_buf, Table) };
        let cursor = Cursor::new(table, cursor_buf, page_rw)?;

        Ok(Self {
            table_buf: table_buf,
            cursor: cursor,
            query: query,
        })
    }

    fn load_col<'b, E: core::fmt::Debug>(
        table: &Table,
        row: &'b [Value],
        col: &Name,
    ) -> Result<&'b Value<'b>, Error<E>> {
        let idx = table
            .get_col_idx_by_name_ref(col)
            .ok_or(Error::ColumnNotFound)?;
        Ok(&row[idx])
    }

    fn eval_operator<'b, E: core::fmt::Debug>(
        operator: &Op,
        table: &Table,
        row: &'b [Value],
    ) -> Result<bool, Error<E>> {
        match (&operator.lhs, &operator.rhs) {
            (Expr::Col(col), Some(Expr::Val(val))) => {
                let lhs = Self::load_col(table, row, col)?;
                Ok(operator.op.eval(lhs, val))
            },
            (Expr::Col(col), None) if operator.op == Operator::IsNull => {
                let lhs = Self::load_col(table, row, col)?;
                Ok(operator.op.eval(lhs, lhs))
            },
            (_, None) => Err(Error::MissingOperands),
            _ => Err(Error::InvalidOperands),
        }
    }

    pub fn next<
        D: BlockDevice, T: TimeSource,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    >(
        &mut self,
        tmp_buf: &mut PageBuffer<A>,
        payload: &mut Vec<u8, A>,
        row: &mut Row<'a, A>,
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    ) -> Result<(), Error<D::Error>> {
        let _ = page_rw.read_page(self.query.target_table, self.table_buf.as_mut())?;
        let target_table = unsafe { as_ref!(self.table_buf, Table) };

        'outter: loop {
            row.clear();
            let payload: &mut Vec<u8, A> = unsafe { core::mem::transmute(&mut *payload) };
            payload.clear();

            let cell = self.cursor.next(target_table, page_rw)?;

            payload.extend_from_slice(cell.payload(target_table.get_null_flags_width_bytes()));
            if cell.header.payload_overflow > 0 {
                 OverflowPage::read_all(page_rw, cell.header.payload_overflow, payload, tmp_buf)?;
            }

            serde_row::deserialize(target_table, row, &payload.as_slice()[0..]);

            match &self.query.filters {
                TopLevelOperator::And(conditions) => {
                    for condition in conditions.iter() {
                        let (operator, negate) = match condition {
                            Condition::Is(op) => (op, false),
                            Condition::Not(op) => (op, true),
                        };

                        let mut result = Self::eval_operator(operator, target_table, row)?;

                        if negate {
                            result = !result;
                        }

                        if !result {
                            continue 'outter;
                        }
                    }

                    return Ok(());
                },
                TopLevelOperator::Or(conditions) => {
                    for condition in conditions.iter() {
                        let (operator, negate) = match condition {
                            Condition::Is(op) => (op, false),
                            Condition::Not(op) => (op, true),
                        };

                        let mut result = Self::eval_operator(operator, target_table, row)?;

                        if negate {
                            result = !result;
                        }

                        if result {
                            return Ok(());
                        }
                    }
                },
            }
        }
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
        unsafe {
            let _ = self.page_rw.read_page(table_page, self.table_buf.as_mut())?;
            let table = as_ref_mut!(self.table_buf, Table);

            if table.col_count as usize != row.len() {
                return Err(Error::Insert(InsertErr::ColCountDoesNotMatch));
            }

            if table.rows_btree_page == 0 {
                let free_page = get_free_page!(&self.page_rw, &mut self.buf1)?;
                table.rows_btree_page = free_page;
                self.page_rw.write_page(table_page, self.table_buf.as_ref())?;
                let btree_leaf = as_ref_mut!(self.buf1, BtreeLeaf);
                btree_leaf.init();
                self.page_rw.write_page(free_page, self.buf1.as_ref())?;
            }

            let serialized_row = serde_row::serialize(
                table, &row,
                &mut self.buf1,
                &mut self.buf2,
                &mut self.buf3,
                &self.page_rw, allocator.clone()
            )?;

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
        }
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
        let query = Query::new(db_cat_page, allocator.clone())
            .is(Op::eq(Expr::Col("tbl_name".to_name()), Expr::Val(Value::Chars(&name))));

        let mut exec = QueryExecutor::new(query, &mut self.table_buf, &mut self.buf1, &self.page_rw)?;

        let mut payload: Vec<u8, A> = Vec::new_in(allocator.clone());
        let mut row: Row<A> = Row::new_in(allocator.clone());

        match exec.next(&mut self.buf2, &mut payload, &mut row, &self.page_rw) {
            Err(_) => return Err(Error::TableErr(TableErr::NotFound)),
            _ => ()
        }

        return match row[1] {
            Value::Int(page) => Ok(page as u32),
            _ => Err(Error::TableErr(TableErr::NotFound))
        };
    }

    pub fn print_all_table(&mut self, allocator: A) {
        let db_cat_page = FixedPages::DbCat.into();
        let query = Query::new(db_cat_page, allocator.clone());

        let mut exec = QueryExecutor::new(query, &mut self.table_buf, &mut self.buf1, &self.page_rw).unwrap();

        let mut payload: Vec<u8, A> = Vec::new_in(allocator.clone());
        let mut row: Row<A> = Row::new_in(allocator.clone());

        while let Ok(_) = exec.next(&mut self.buf2, &mut payload, &mut row, &self.page_rw) {
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

    pub fn add_column(&mut self, col: Column) -> Result<(), TableErr<D::Error>> {
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
            let files = self.create_table(allocator.clone())?;
        }

        {
            let files = self.get_table("files".to_name(), allocator.clone()).unwrap();
            let path = Column::new("cool_path".to_name(), ColumnType::Chars);
            self.new_table_begin("fav".to_name());
            self.add_column(path)?;
            let fav = self.create_table(allocator.clone())?;
        }

        {
            let files = self.get_table("files".to_name(), allocator.clone()).unwrap();
            let mut row = Row::new_in(allocator.clone());
            row.push(Value::Chars(b"/some/file.txt"));
            row.push(Value::Int(124));
            row.push(Value::Chars(b"file.txt"));
            self.insert_to_table(files, row, allocator.clone())?;
        }

        {
            let fav = self.get_table("fav".to_name(), allocator.clone()).unwrap();
            let mut row = Row::new_in(allocator.clone());
            row.push(Value::Chars(b"/some/file.txt"));
            self.insert_to_table(fav, row, allocator.clone())?;
        }

        {
            let files = self.get_table("files".to_name(), allocator.clone()).unwrap();
            let query = Query::new(files, allocator.clone());
            let mut exec = QueryExecutor::new(query, &mut self.table_buf, &mut self.buf1, &self.page_rw)?;
            let mut payload: Vec<u8, A> = Vec::new_in(allocator.clone());
            let mut row: Row<A> = Row::new_in(allocator.clone());
            match exec.next(&mut self.buf2, &mut payload, &mut row, &self.page_rw) {
                Err(_) => return Err(Error::TableErr(TableErr::NotFound)),
                _ => ()
            }

            println!("row = {:?}", row);
        }

        Ok(())
    }
}
