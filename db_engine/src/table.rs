use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;
use crate::db::{Error};
use crate::{get_bit, set_bit, clear_bit};

const NAME_MAX_LEN: usize = 32;
pub type Name = [u8; NAME_MAX_LEN];

pub trait ToName {
    fn to_name(&self) -> Name;
}

impl ToName for &str {
    fn to_name(&self) -> Name {
        let mut buffer = [0u8; NAME_MAX_LEN];
        let src = self.as_bytes();
        let len = core::cmp::min(src.len(), NAME_MAX_LEN);
        buffer[..len].copy_from_slice(&src[..len]);
        buffer
    }
}

impl ToName for &[u8] {
    fn to_name(&self) -> Name {
        let mut buffer = [0u8; NAME_MAX_LEN];
        let len = core::cmp::min(self.len(), NAME_MAX_LEN);
        buffer[..len].copy_from_slice(&self[..len]);
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
    // Ref = 1 << 2
}

impl Flags {
    pub fn is_set(flags: u8, flag: Flags) -> bool {
        get_bit!(u8, flags, flag) == 1
    }

    pub fn set(flags: u8, flag: Flags) -> u8 {
        set_bit!(u8, flags, flag)
    }

    pub fn clear(flags: u8, flag: Flags) -> u8 {
        clear_bit!(u8, flags, flag)
    }
}

#[repr(C, packed)]
#[derive(Debug, Copy, Clone)]
pub struct Column {
    pub name: Name,
    pub flags: u8,
    pub col_type: ColumnType,
    pub ref_table: u32,
    pub ref_col: u16
}

#[derive(Debug)]
#[repr(C, packed)]
pub struct Table {
    pub name: Name,
    pub rows_btree_page: u32,
    pub col_count: u32,
    pub columns: [Column; (PAGE_SIZE - (size_of::<Name>() + (size_of::<u32>() * 2))) / size_of::<Column>()]
}

impl Table {
    pub fn add_column<E: core::fmt::Debug>(&mut self, column: Column) -> Result<(), Error<E>> {
        if self.col_count as usize >= NAME_MAX_LEN {
            return Err(Error::MaxColumnsReached);
        }
        self.columns[self.col_count as usize] = column;
        self.col_count += 1;
        Ok(())
    }

    pub fn get_null_flags_width_bytes(&self) -> usize {
        let s = self.col_count.next_power_of_two() as usize;
        (if s < 8 { 8 } else { s }) / 8
    }

    pub fn get_columns(&self) -> &[Column] {
        &self.columns[0..self.col_count as usize]
    }

    pub fn get_col_idx_by_name(&self, name: impl ToName) -> Option<usize> {
        let columns = self.get_columns();

        for (idx, col) in columns.iter().enumerate() {
            if col.name == name.to_name() {
                return Some(idx);
            }
        }
        None
    }

    pub fn get_col_idx_by_name_ref(&self, name: &impl ToName) -> Option<usize> {
        let columns = self.get_columns();

        for (idx, col) in columns.iter().enumerate() {
            if col.name == name.to_name() {
                return Some(idx);
            }
        }
        None
    }
}

impl Column {
    pub fn new(name: impl ToName, col_type: ColumnType) -> Self {
        Self {
            name: name.to_name(),
            flags: Flags::None as u8,
            col_type: col_type,
            ref_table: 0,
            ref_col: 0
        }
    }

    pub fn nullable(mut self) -> Self {
        self.flags = Flags::Nullable as u8;
        self
    }

    pub fn primary(mut self) -> Self {
        self.flags = Flags::Primary as u8;
        self
    }

    // pub fn ref_table(mut self, ref_table: u32, ref_col: u16) -> Self {
    //     self.flags = Flags::Ref as u8;
    //     self.ref_table = ref_table;
    //     self.ref_col = ref_col;
    //     self
    // }

    pub fn empty() -> Self {
        Self {
            name: [0; NAME_MAX_LEN],
            flags: Flags::None as u8,
            col_type: ColumnType::Int,
            ref_table: 0,
            ref_col: 0
        }
    }
}

