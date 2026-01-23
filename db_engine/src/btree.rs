#![allow(unused)]
use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;
use core::cmp::Ordering;
use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;
use crate::table::{Value, Row};

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

pub const KEY_MAX_LEN: usize = 64;


#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u8)]
pub enum NodeType {
    Internal = 1,
    Leaf = 2
}

#[derive(Debug)]
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

const INTERNAL_META_SIZE: usize = size_of::<NodeType>() + (size_of::<u32>() * 2);
#[derive(Debug)]
#[repr(C, packed)]
pub struct BtreeInternal {
    pub node_type: NodeType,
    pub key_count: u32,
    pub free_start: u16,
    pub free_end: u16,
    pub data: [u8; PAGE_SIZE - INTERNAL_META_SIZE]
}

#[derive(Debug)]
#[repr(C, packed)]
pub struct Key<'a> {
    pub len: u8,
    pub bytes: &'a [u8]
}

impl<'a> PartialEq for Key<'a> {
    fn eq(&self, other: &Self) -> bool {
        let s_bytes = { self.bytes };
        let o_bytes = { other.bytes };
        let s_len = self.len as usize;
        let o_len = other.len as usize;
        s_len == o_len && s_bytes[..s_len] == o_bytes[..o_len]
    }
}

impl<'a> Eq for Key<'a> {}

impl<'a> Ord for Key<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let s_bytes = { self.bytes };
        let o_bytes = { other.bytes };
        let s_len = self.len as usize;
        let o_len = other.len as usize;
        s_bytes[..s_len].cmp(&o_bytes[..o_len])
    }
}

impl<'a> PartialOrd for Key<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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

impl BtreeLeaf {
    pub fn init(&mut self) {
        self.node_type = NodeType::Leaf;
        self.free_start = LEAF_META_SIZE as u16;
        self.free_end = (PAGE_SIZE - 1) as u16;
    }

    pub fn check_duplicate_by_primary_key(&mut self, primary_key: usize, val: &Value) -> bool {
        return true;
    }
}

// impl <'a> PayloadCell {
//     pub fn serialize<A: Allocator + Clone>(
//         table: &Table,
//         row: Row<Value<'a>, A>,
//         allocator: A
//     ) -> PayloadCell<'a> {
//         let res: Vec<u8, A> = Vec::new_in(allocator);
//         let null_flags: u64 = 0;
//         let mut i = 0;
//         let mut key: Key;
//         let mut payload_len_bytes = 0;
// 
//         while i < row.len() {
//             match row[i] {
//                 Value::Null => {
//                     null_flags |= 1 << i;
//                 },
//                 Value::Int(v) => {
//                     res.extend_from_slice(&v.to_be_bytes()); 
//                     payload_len_bytes += 8;
//                 },
//                 Value::Float(v) => {
//                     res.extend_from_slice(&v.to_be_bytes());
//                     payload_len_bytes += 8;
//                 },
//                 Value::Chars(chars) => {
//                     let length = chars.len() as u8; 
//                     res.push(length);
//                     res.extend_from_slice(chars);
//                     payload_len_bytes += length;
//                 }
//             }
// 
//             if table.columns[i].flags.is_set(Flags::Primary) {
//                 let buf: [u8; 8] = [0; 8];
//                 key = row[i].to_key(buf);
//             }
// 
//             i += 1;
//         }
// 
//         PayloadCell {
//             key: key,
//             payload_len_bytes: payload_inline_len_bytes,
//             payload_overflow: u32,
//             payload_inline_len_bytes: u8,
//             null_flags: &'a [u8],
//             payload: &'a [u8]
//         }
//     }
// }
