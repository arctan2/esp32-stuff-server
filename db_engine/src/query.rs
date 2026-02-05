#![allow(unused)]

use allocator_api2::alloc::Allocator;
use crate::btree;
use crate::btree::{Cursor, Key};
use crate::table::{Table, ToName};
use crate::page_rw::{PageRW};
use crate::fs::{PageFile};
use crate::page_buf::PageBuffer;
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
pub enum Expr<'a, N: ToName> {
    Val(Value<'a>),
    // Col(ColumnName) // later (hopefully) when I decide to do joins this will be used
    Col(N)
}

#[derive(Debug)]
pub struct ColumnName<N: ToName> {
    table: u32,
    name: N
}

#[derive(Debug)]
pub struct Op<'a, N: ToName> {
    lhs: Expr<'a, N>,
    op: Operator,
    rhs: Option<Expr<'a, N>>,
}

impl <'a, N> Op<'a, N> where N: ToName {
    pub fn eq(lhs: Expr<'a, N>, rhs: Expr<'a, N>) -> Self {
        Self { lhs, op: Operator::Eq, rhs: Some(rhs) }
    }

    pub fn gt(lhs: Expr<'a, N>, rhs: Expr<'a, N>) -> Self {
        Self { lhs, op: Operator::Gt, rhs: Some(rhs) }
    }

    pub fn lt(lhs: Expr<'a, N>, rhs: Expr<'a, N>) -> Self {
        Self { lhs, op: Operator::Lt, rhs: Some(rhs) }
    }

    pub fn starts_with(lhs: Expr<'a, N>, rhs: Expr<'a, N>) -> Self {
        Self { lhs, op: Operator::StartsWith, rhs: Some(rhs) }
    }

    pub fn ends_with(lhs: Expr<'a, N>, rhs: Expr<'a, N>) -> Self {
        Self { lhs, op: Operator::EndsWith, rhs: Some(rhs) }
    }

    pub fn contains(lhs: Expr<'a, N>, rhs: Expr<'a, N>) -> Self {
        Self { lhs, op: Operator::Contains, rhs: Some(rhs) }
    }

    pub fn is_null(lhs: Expr<'a, N>) -> Self {
        Self { lhs, op: Operator::IsNull, rhs: None }
    }
}

#[derive(Debug)]
pub enum Condition<'a, N: ToName> {
    Is(Op<'a, N>),
    Not(Op<'a, N>),
}

pub enum TopLevelOperator<'a, A: Allocator + Clone, N: ToName = &'a str> {
    And(Vec<Condition<'a, N>, A>),
    Or(Vec<Condition<'a, N>, A>)
}

pub struct Limit(usize, usize);

pub struct Query<'a, A: Allocator + Clone, N: ToName = &'a str> {
    allocator: A,
    target_table: u32,
    // tables: Vec<u32, A>,
    filters: TopLevelOperator<'a, A, N>,
    key: Option<Vec<u8, A>>,
    // project: Vec<ColumnName, A>,
    limit: Option<Limit>,
}

impl <'a, A, N> Query<'a, A, N> where A: Allocator + Clone, N: ToName {
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

    pub fn not(mut self, op: Op<'a, N>) -> Self {
        match self.filters {
            TopLevelOperator::And(ref mut v) => v.push(Condition::Not(op)),
            TopLevelOperator::Or(ref mut v) => v.push(Condition::Not(op))
        };
        self
    }

    pub fn is(mut self, op: Op<'a, N>) -> Self {
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

pub struct QueryExecutor<'a, F: PageFile, A: Allocator + Clone, N: ToName> {
    table_buf: &'a mut PageBuffer<A>,
    tmp_buf: &'a mut PageBuffer<A>,
    cursor: Cursor<'a, A>,
    query: Query<'a, A, N>,
    is_ran: bool,
    payload: Vec<u8, A>,
    row: Row<'a, A>,
    page_rw: &'a PageRW<F>
}

impl <'a, F: PageFile, A: Allocator + Clone, N: ToName> QueryExecutor<'a, F, A, N> {
    pub fn new(
        query: Query<'a, A, N>,
        table_buf: &'a mut PageBuffer<A>,
        tmp_buf: &'a mut PageBuffer<A>,
        cursor_buf: &'a mut PageBuffer<A>,
        page_rw: &'a PageRW<F>
    ) -> Result<Self, Error<F::Error>> {
        let _ = page_rw.read_page(query.target_table, table_buf.as_mut())?;
        let table = unsafe { as_ref!(table_buf, Table) };
        let cursor = Cursor::new(table, cursor_buf, page_rw)?;

        Ok(Self {
            table_buf: table_buf,
            tmp_buf: tmp_buf,
            cursor: cursor,
            payload: Vec::new_in(query.allocator.clone()),
            row: Row::new_in(query.allocator.clone()),
            query: query,
            is_ran: false,
            page_rw: page_rw,
        })
    }

    fn load_col<'b, E: core::fmt::Debug>(
        table: &Table,
        row: &'b [Value],
        col: &N,
    ) -> Result<&'b Value<'b>, Error<E>> {
        let idx = table
            .get_col_idx_by_name_ref(col)
            .ok_or(Error::ColumnNotFound)?;
        Ok(&row[idx])
    }

    fn eval_operator<'b, E: core::fmt::Debug>(
        operator: &Op<'b, N>,
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

    pub fn count(&mut self) -> Result<usize, Error<F::Error>> {
        let mut count = 0;

        loop {
            match self.next() {
                Ok(_) => count += 1,
                Err(e) => match e {
                    Error::EndOfRecords => return Ok(count),
                    _ => return Err(e)
                }
            }
        }
    }

    pub fn next(&mut self) -> Result<&mut Row<'a, A>, Error<F::Error>> {
        let _ = self.page_rw.read_page(self.query.target_table, self.table_buf.as_mut())?;
        let target_table = unsafe { as_ref!(self.table_buf, Table) };
        let payload = &mut self.payload;
        let row = &mut self.row;

        'outter: loop {
            row.clear();
            let payload: &mut Vec<u8, A> = unsafe { core::mem::transmute(&mut *payload) };
            payload.clear();

            let cell = if let Some(key) = &self.query.key {
                if self.is_ran {
                    return Err(Error::EndOfRecords);
                }
                self.is_ran = true;
                btree::find_by_key(target_table, self.tmp_buf, unsafe { &*((&key.as_slice()).as_ptr() as *const Key) }, self.page_rw)?
            } else {
                self.cursor.next(target_table, self.page_rw)?
            };

            payload.extend_from_slice(cell.payload(target_table.get_null_flags_width_bytes()));
            if cell.header.payload_overflow > 0 {
                 OverflowPage::read_all(self.page_rw, cell.header.payload_overflow, payload, self.tmp_buf)?;
            }

            serde_row::deserialize(target_table, row, &payload.as_slice());

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

                    return Ok(row);
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
                            return Ok(row);
                        }
                    }
                },
            }
        }
    }
}

