use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;

/*
db_cat(primary_key db_name: char[32], page: int)
insert db_cat ("some_table", 5);
insert db_cat ("other_cool_table", 6);

----------------------------------------------
Header {
    node_type: NodeType::Leaf,
    key_count: 2,
    free_start: 0,
    free_end: 0,
    next_leaf: 0,
}
offsets[
    offset_of("other_cool_table")
    offset_of("some_table")
]

Cell {
    key: { len: u8 = 16, bytes: [other_cool_table] }
    payload_len_bytes: 32 + 8,
    payload_overflow: 0,
    null_flags: u8/u16/u32/u64 based on the number of columns in table,
    payload: ["other_cool_table", 6] <- it will be in binary format
}
Cell {
    key: { len: u8 = 10, bytes: [some_table] }
    payload_len_bytes: 32 + 8,
    payload_overflow: 0,
    null_flags: u8/u16/u32/u64 based on the number of columns in table,
    payload: ["some_table", 5] <- it will be in binary format
}
----------------------------------------------

*/


#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum NodeType {
    Internal = 1,
    Leaf = 2
}

#[repr(C, packed)]
pub struct BtreePage {
    pub node_type: NodeType,
    pub data: [u8; PAGE_SIZE - size_of::<NodeType>()]
}

const LEAF_META_SIZE: usize = size_of::<NodeType>() + (size_of::<u32>() * 3);
#[derive(Debug)]
#[repr(C, packed)]
pub struct BtreeLeaf {
    pub node_type: NodeType,
    pub key_count: u32,
    pub free_start: u16,
    pub free_end: u16,
    pub next_leaf: u32,
    pub data: [u8; PAGE_SIZE - LEAF_META_SIZE]
}

impl BtreeLeaf {
    pub fn init_ref(ptr: *mut Self) {
        unsafe {
            (*ptr).node_type = NodeType::Leaf;
            (*ptr).free_start = LEAF_META_SIZE as u16;
            (*ptr).free_end = (PAGE_SIZE - 1) as u16;
        }
    }
}

const INTERNAL_META_SIZE: usize = size_of::<NodeType>() + (size_of::<u32>() * 2);
#[repr(C, packed)]
pub struct BtreeInternal {
    pub node_type: NodeType,
    pub key_count: u32,
    pub free_start: u16,
    pub free_end: u16,
    pub data: [u8; PAGE_SIZE - INTERNAL_META_SIZE]
}

#[repr(C, packed)]
pub struct Key<'a> {
    pub len: u8,
    pub bytes: &'a [u8]
}

#[repr(C, packed)]
pub struct PayloadCell<'a> {
    pub key: Key<'a>,
    pub payload_len_bytes: u32,
    pub payload_overflow: u32,
    pub payload_inline_len_bytes: u8,
    pub null_flags: &'a [u8],
    pub payload: &'a [u8]
}

#[repr(C, packed)]
pub struct InternalCell<'a> {
    pub key: Key<'a>,
    pub child: u32
}

