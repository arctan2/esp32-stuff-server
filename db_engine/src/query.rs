#![allow(unused)]

use embedded_sdmmc::{BlockDevice, TimeSource};
use allocator_api2::alloc::Allocator;
use crate::btree;
use crate::btree::{Cursor, Key};
use crate::table::{Table, Name};
use crate::PageRW;
use crate::types::PageBuffer;
use crate::overflow::OverflowPage;
use crate::{as_ref};
use allocator_api2::vec::Vec;
use crate::serde_row;
use crate::serde_row::{Value, Row, Operations};
use crate::db::{Error};

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
    // Col(ColumnName) // later (hopefully) when I decide to do joins this will be used
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
    key: Option<Vec<u8, A>>,
    // project: Vec<ColumnName, A>,
    limit: Option<Limit>,
}

impl <'a, A> Query<'a, A> where A: Allocator + Clone {
    pub fn new(target_table: u32, allocator: A) -> Self {
        Self {
            allocator: allocator.clone(),
            target_table: target_table,
            filters: TopLevelOperator::And(Vec::new_in(allocator.clone())),
            key: None,
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

    pub fn key(mut self, val: Value<'a>) -> Self {
        let mut v: Vec<u8, A> = Vec::new_in(self.allocator.clone());
        val.to_key_vec(&mut v);
        self.key = Some(v);
        self
    }

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

            let cell = if let Some(key) = &self.query.key {
                btree::find_by_key(target_table, tmp_buf, unsafe { &*((&key.as_slice()).as_ptr() as *const Key) }, page_rw)?
            } else {
                self.cursor.next(target_table, page_rw)?
            };

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

