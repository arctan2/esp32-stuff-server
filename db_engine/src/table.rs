use allocator_api2::alloc::Allocator;
use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;
use crate::types::PageBuffer;
use crate::{get_bit};

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
pub enum TableErr<E: core::fmt::Debug> {
    SdmmcErr(embedded_sdmmc::Error<E>),
    MaxColumnsReached,
    NotFound,
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

    pub fn get_null_flags_width_bytes(&self) -> usize {
        let s = self.col_count.next_power_of_two() as usize;
        (if s < 8 { 8 } else { s }) / 8
    }

    pub fn get_columns(&self) -> &[Column] {
        &self.columns[0..self.col_count as usize]
    }

    // pub fn get_record_by_key(&self, key: &Key) {
    // }
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

