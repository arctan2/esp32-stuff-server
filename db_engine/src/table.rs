use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;

const NAME_MAX_LEN: usize = 32;

#[repr(u8)]
pub enum ColumnType {
    Int = 1,
    Float = 2,
    Chars = 3,
}

#[repr(C, packed)]
pub struct Column {
    name: [u8; NAME_MAX_LEN],
    flags: u8,
    type: ColumnType
}

#[repr(C, packed)]
pub struct TablePage {
    name: [u8; NAME_MAX_LEN],
    rows_btree_page: u32,
    column_count: u32,
    columns: [Column; (PAGE_SIZE - ((size_of::<u8>() * NAME_MAX_LEN) + (size_of::<u32>() * 2))) / size_of::<Column>()]
}
