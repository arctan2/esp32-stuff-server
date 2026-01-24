use allocator_api2::alloc::Allocator;
use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;
use embedded_sdmmc::{BlockDevice, TimeSource};
use crate::PageRW;
use crate::types::PageBuffer;
use crate::btree::{BtreePage, BtreeLeaf, BtreeInternal, Key, KEY_MAX_LEN, NodeType};
use crate::{get_bit, as_ref};
use allocator_api2::vec::Vec;

const NAME_MAX_LEN: usize = 32;
pub type Name = [u8; NAME_MAX_LEN];

pub trait ToName {
    fn to_name(&self) -> Name;
}

impl ToName for str {
    fn to_name(&self) -> Name {
        let mut buffer = [0u8; NAME_MAX_LEN];
        let src = self.as_bytes();
        let len = std::cmp::min(src.len(), NAME_MAX_LEN);
        buffer[..len].copy_from_slice(&src[..len]);
        buffer
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ColumnType {
    Null = 0,
    Int = 1,
    Float = 2,
    Chars = 3,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Flags {
    None = 0,
    Primary = 1 << 0,
    Nullable = 1 << 1,
    Foreign = 1 << 2
}

impl Flags {
    pub fn is_set(self, flag: Flags) -> bool {
        get_bit!(u8, self, flag) == 1
    }
}

#[repr(C, packed)]
#[derive(Debug, Copy, Clone)]
pub struct Column {
    pub name: Name,
    pub flags: Flags,
    pub col_type: ColumnType,
    pub ref_table_page: u32,
    pub ref_col_idx: u16
}

#[derive(Debug)]
#[repr(C, packed)]
pub struct Table {
    pub name: Name,
    pub rows_btree_page: u32,
    pub col_count: u32,
    pub columns: [Column; (PAGE_SIZE - (size_of::<Name>() + (size_of::<u32>() * 2))) / size_of::<Column>()]
}

#[derive(Debug)]
pub enum Value<'a> {
    Null,
    Int(i64),
    Float(f64),
    Chars(&'a [u8])
}

pub type Row<'a, A> = allocator_api2::vec::Vec<Value<'a>, A>;

impl<'a> Value<'a> {
    pub fn to_bytes_vec<A: Allocator + Clone>(&'a self, v: &mut Vec<u8, A>) {
        match self {
            Value::Null => {},
            Value::Int(val) => v.extend_from_slice(&val.to_be_bytes()),
            Value::Float(val) => v.extend_from_slice(&val.to_be_bytes()),
            Value::Chars(val) => {
                let limit = val.len().min(KEY_MAX_LEN);
                v.extend_from_slice(&val[..limit]);
            }
        }
    }
}

#[derive(Debug)]
pub struct SerializedRow<A: Allocator + Clone> {
    pub key: Vec<u8, A>,
    pub null_flags: Vec<u8, A>,
    pub payload: Vec<u8, A>,
    pub primary_key_idx: Option<usize>,
}

#[derive(Debug)]
pub enum TableErr<E: core::fmt::Debug> {
    SdmmcErr(embedded_sdmmc::Error<E>),
    MaxColumnsReached,
}

impl<DErr> From<embedded_sdmmc::Error<DErr>> for TableErr<DErr> where DErr: core::fmt::Debug {
    fn from(err: embedded_sdmmc::Error<DErr>) -> Self {
        TableErr::SdmmcErr(err)
    }
}

impl Table {
    pub fn create(name: Name) -> Self {
        Self {
            name: name,
            rows_btree_page: 0,
            col_count: 0,
            columns: [Column::empty(); 101]
        }
    }

    pub fn add_column<E: core::fmt::Debug>(mut self, column: Column) -> Result<Self, TableErr<E>> {
        if self.col_count as usize >= NAME_MAX_LEN {
            return Err(TableErr::MaxColumnsReached);
        }
        self.columns[self.col_count as usize] = column;
        self.col_count += 1;
        return Ok(self);
    }

    pub fn write_to_buf<A: Allocator + Clone>(&self, buf: &mut PageBuffer<A>) {
        unsafe { buf.write(0, self); }
    }

    pub fn get_null_flags_width(&self) -> usize {
        let s = self.col_count.next_power_of_two() as usize;
        if s < 8 { 8 } else { s }
    }

    pub fn traverse_to_leaf<
        'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    >(
        &self,
        buf: &mut PageBuffer<A>,
        _key: Key,
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> Result<u32, TableErr<D::Error>> {
        unsafe {
            let cur_page = self.rows_btree_page;
            loop {
                let _ = page_rw.read_page(cur_page, buf.as_mut());
                let btree_page = as_ref!(buf, BtreePage);
                if btree_page.node_type == NodeType::Leaf {
                    break;
                }
                let btree_internal = as_ref!(buf, BtreeInternal);
                todo!("traverse internal node");
            }
            return Ok(cur_page);
        }
    }
}

impl Column {
    pub fn new(name: Name, col_type: ColumnType, flags: Flags) -> Self {
        Self {
            name: name,
            flags: flags,
            col_type: col_type,
            ref_table_page: 0,
            ref_col_idx: 0
        }
    }

    pub fn empty() -> Self {
        Self {
            name: [0; NAME_MAX_LEN],
            flags: Flags::None,
            col_type: ColumnType::Int,
            ref_table_page: 0,
            ref_col_idx: 0
        }
    }
}

