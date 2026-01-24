#![allow(unused)]
use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;
use core::cmp::Ordering;
use crate::types::{PageBuffer, PageBufferWriter};
use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;
use crate::{as_ref, PageRW};
use crate::table::{Value, SerializedRow, Table};
use crate::overflow::OverflowPage;
use crate::db::Error;
use crate::buf;
use embedded_sdmmc::{BlockDevice, TimeSource};

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
    payload_total_len: 32 + 8,
    payload_overflow: 0,
    null_flags: u8/u16/u32/u64 based on the number of columns in table,
    payload: ["other_cool_table", 6] <- it will be in binary format
}
Cell {
    key: { len: u8 = 10, bytes: [some_table] }
    payload_total_len: 32 + 8,
    payload_overflow: 0,
    null_flags: u8/u16/u32/u64 based on the number of columns in table,
    payload: ["some_table", 5] <- it will be in binary format
}
----------------------------------------------

*/

pub const KEY_MAX_LEN: usize = 64;
pub const MAX_INLINE_LEN: usize = 255;


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

#[derive(Debug, Copy, Clone)]
#[repr(C, packed)]
pub struct Key {
    pub len: u8,
    pub bytes: *mut u8
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            core::slice::from_raw_parts(self.bytes, self.len.into())
        }
    }
}

impl AsMut<[u8]> for Key {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe {
            core::slice::from_raw_parts_mut(self.bytes, self.len.into())
        }
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        let s_bytes = self.as_ref();
        let o_bytes = other.as_ref();
        self.len == other.len && s_bytes == o_bytes
    }
}

impl Eq for Key {}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        let s_bytes = self.as_ref();
        let o_bytes = other.as_ref();
        s_bytes.cmp(&o_bytes)
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// do not change the layout of the struct
#[derive(Debug)]
#[repr(C, packed)]
pub struct PayloadCell {
    pub payload_total_len: u32,
    pub payload_overflow: u32,
    pub payload_inline_len: u8,
    pub key: Key,
    pub null_flags: *mut u8,
    pub payload: *mut u8
}

#[repr(C, packed)]
pub struct InternalCell {
    pub key: Key,
    pub child: u32
}

impl BtreeLeaf {
    pub fn init(&mut self) {
        self.node_type = NodeType::Leaf;
        self.free_start = LEAF_META_SIZE as u16;
        self.free_end = PAGE_SIZE as u16;
    }

    pub fn check_duplicate_by_primary_key(&mut self, primary_key: usize, val: &Value) -> bool {
        return true;
    }
}

impl <'a> PayloadCell {
    pub fn create_payload_to_buf<
        D: BlockDevice, T: TimeSource, A: Allocator + Clone,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    >(
        table: &Table, 
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
        row: SerializedRow<A>,
        buf: &mut PageBuffer<A>,
        overflow_buf: &mut PageBuffer<A>,
    ) -> Result<(), Error<D::Error>> {
        let mut buf_writer = PageBufferWriter::new(buf);
        let key_len: u8 = row.key.len() as u8;
        let payload_len: u32 = row.payload.len() as u32;
        let mut inline_len: u32 = payload_len;
        let overflow_bytes_len: u32 = if (payload_len as usize) > MAX_INLINE_LEN {
            inline_len = MAX_INLINE_LEN as u32;
            ((payload_len as usize) - MAX_INLINE_LEN) as u32
        } else {
            0 as u32
        };
        buf_writer.write(&payload_len);
        let payload_overflow_page: u32 = if overflow_bytes_len > 0 {
            OverflowPage::new_overflow_list(page_rw, &row.payload[MAX_INLINE_LEN..], overflow_buf)?
        } else {
            0 as u32
        };
        buf_writer.write(&payload_overflow_page);
        buf_writer.write(&(inline_len as u8));
        buf_writer.write(&key_len);
        buf_writer.write_slice(&row.key);
        buf_writer.write_slice(&row.null_flags);
        buf_writer.write_slice(&row.payload[0..(inline_len as usize)]);

        Ok(())
    }

    pub fn get_size(&self, table: &Table) -> usize {
        (size_of::<u32>() * 2) +
        (size_of::<u8>() * 2) +
        (table.get_null_flags_width() / 8) +
        self.key.len as usize +
        self.payload_inline_len as usize
    }
}

pub fn insert_payload_to_leaf<
    'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize
>(
    table: &Table,
    leaf_buf: &mut PageBuffer<A>,
    leaf: &mut BtreeLeaf,
    leaf_page: u32,
    page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    payload_cell: &PayloadCell,
) {
    unsafe {
        let payload_size_bytes = payload_cell.get_size(table);
        let start = leaf.free_end as usize - payload_size_bytes;
        let payload_ptr = payload_cell as *const PayloadCell as *const u8;
        let payload_slice = core::slice::from_raw_parts(payload_ptr, payload_size_bytes);
        buf::write_bytes(leaf_buf.as_mut(), start as usize, &payload_slice);
        leaf.free_end = start as u16;
        buf::write(leaf_buf.as_mut(), leaf.free_start as usize, &(start as u16));
        leaf.key_count += 1;

        println!("{:?}", payload_slice);
        println!("{:?}", leaf);
    }
}
