#![allow(unused)]
use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;
use core::cmp::Ordering;
use crate::types::{PageBuffer, PageBufferWriter};
use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;
use crate::{as_ref, as_ref_mut, PageRW};
use crate::table::{Value, SerializedRow, Table, TableErr};
use crate::overflow::OverflowPage;
use crate::db::{InsertErr, Error};
use crate::buffer;
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
        |
        |
        v
    <free_space>
        ^
        |
        |
Cell {
    key: { len: u8 = 10, bytes: [some_table] }
    payload_total_len: 32 + 8,
    payload_overflow: 0,
    null_flags: u8/u16/u32/u64 based on the number of columns in table,
    payload: ["other_cool_table", 6] <- it will be in binary format
}
Cell {
    key: { len: u8 = 16, bytes: [other_cool_table] }
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

const LEAF_META_SIZE: usize = size_of::<NodeType>() + (size_of::<u32>() * 2) + size_of::<u16>();
#[derive(Debug)]
#[repr(C, packed)]
pub struct BtreeLeaf {
    pub node_type: NodeType,
    pub key_count: u16,
    pub free_start: u16,
    pub free_end: u16,
    pub next_leaf: u32,
    pub data: [u8; PAGE_SIZE - LEAF_META_SIZE]
}

const INTERNAL_META_SIZE: usize = size_of::<NodeType>() + (size_of::<u16>() * 3);
#[derive(Debug)]
#[repr(C, packed)]
pub struct BtreeInternal {
    pub node_type: NodeType,
    pub key_count: u16,
    pub free_start: u16,
    pub free_end: u16,
    pub data: [u8; PAGE_SIZE - INTERNAL_META_SIZE]
}

#[derive(Debug, Copy, Clone)]
#[repr(C, packed)]
pub struct Key {
    pub len: u8,
    /* [u8; len] in memory comes here */
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            let data_ptr = (self as *const Key as *const u8).add(1);
            core::slice::from_raw_parts(data_ptr, self.len as usize)
        }
    }
}

impl AsMut<[u8]> for Key {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe {
            let data_ptr = (self as *mut Key as *mut u8).add(1);
            core::slice::from_raw_parts_mut(data_ptr, self.len as usize)
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

#[derive(Debug)]
#[repr(C, packed)]
pub struct PayloadCellHeader {
    pub payload_total_len: u32,
    pub payload_overflow: u32,
    pub payload_inline_len: u8,
}

#[derive(Clone, Copy, Debug)]
pub struct PayloadCellView<'a> {
    pub header: &'a PayloadCellHeader,
    pub data: &'a [u8]
}

impl <'a> PayloadCellView<'a> {
    pub fn new(raw_buf: &'a [u8], offset: usize) -> Self {
        let (header_bytes, data_bytes) = raw_buf[offset..].split_at(std::mem::size_of::<PayloadCellHeader>());
        let header = unsafe { &*(header_bytes.as_ptr() as *const PayloadCellHeader) };
        Self { header, data: data_bytes }
    }

    pub fn key(&self) -> &'a Key {
        let key_len = self.data[0] as usize;
        unsafe { &*(self.data[0..1 + key_len].as_ptr() as *const Key) as &Key }
    }

    pub fn null_flags(&self, width: usize) -> &'a [u8] {
        let key_len = self.data[0] as usize;
        let start = 1 + key_len;
        &self.data[start..start + width]
    }

    pub fn payload(&self, null_flags_width: usize) -> &'a [u8] {
        let key_len = self.data[0] as usize;
        let start = 1 + key_len + null_flags_width;
        &self.data[start..start + self.header.payload_inline_len as usize]
    }

    pub fn as_bytes(&self, total_size: usize) -> &'a [u8] {
        unsafe {
            let ptr = self.header as *const PayloadCellHeader as *const u8;
            std::slice::from_raw_parts(ptr, total_size)
        }
    }
}


#[repr(C, packed)]
pub struct InternalCell {
    pub child: u32,
    pub key: Key,
}

#[derive(Debug)]
pub struct BtreeCell<'a> {
    key: &'a Key,
    payload_cell: &'a [u8],
}

impl <'a> BtreeCell<'a> {
    pub fn from_leaf_payload(cell: PayloadCellView<'a>, total_size: usize) -> Self {
        Self {
            key: cell.key(),
            payload_cell: cell.as_bytes(total_size)
        }
    }
}

type BtreeCells<'a, A> = Vec<BtreeCell<'a>, A>;

trait BtreeCellsOps {
    fn total_size(&self) -> usize;
    fn sort_last_cell(&mut self);
    fn binary_search_by_key(&self, key: &Key) -> Option<usize>;
}

impl <'a, A: Allocator> BtreeCellsOps for BtreeCells<'a, A> {
    fn total_size(&self) -> usize {
        self.iter().map(|cell| cell.payload_cell.len()).sum()
    }

    fn sort_last_cell(&mut self) {
        if self.len() <= 1 {
            return;
        }
        let mut idx = self.len() - 1;

        while idx > 0 && self[idx].key < &self[idx - 1].key {
            self.swap(idx, idx - 1);
            idx -= 1;
        }
    }

    fn binary_search_by_key(&self, key: &Key) -> Option<usize>{
        let mut h = self.len();
        let mut l = 0;

        while l < h {
            let m = (l + h) / 2;

            if self[m].key == key {
                return Some(m);
            }

            if self[m].key < key {
                l = m + 1;
            } else {
                h = m;
            }
        }

        return None;
    }
}

impl BtreeLeaf {
    pub fn init(&mut self) {
        self.node_type = NodeType::Leaf;
        self.free_start = LEAF_META_SIZE as u16;
        self.free_end = PAGE_SIZE as u16;
    }

    pub fn get_offsets_mut(&mut self) -> &mut [u16] {
        let ptr = self.data.as_ptr();
        return unsafe {
            std::slice::from_raw_parts_mut(ptr as *mut u16, self.key_count as usize)
        };
    }

    pub fn get_offsets(&self) -> &[u16] {
        let ptr = self.data.as_ptr();
        return unsafe {
            std::slice::from_raw_parts(ptr as *const u16, self.key_count as usize)
        };
    }

    pub fn read_btree_cells<'a, A: Allocator>(&'a mut self, table: &Table, allocator: A) -> BtreeCells<'a, A> {
        unsafe {
            let mut cells: BtreeCells<A> = BtreeCells::new_in(allocator);
            let ptr = self.data.as_ptr();
            let offsets: &[u16] = unsafe {
                std::slice::from_raw_parts(ptr as *const u16, self.key_count as usize)
            };

            for i in offsets.iter() {
                let payload_cell = PayloadCellView::new(&self.data, *i as usize);
                let cell = BtreeCell::from_leaf_payload(payload_cell, payload_cell.get_size(table));
                cells.push(cell);
            }

            return cells;
        }
    }

    pub fn write_btree_cells<A: Allocator>(&mut self, cells: &BtreeCells<A>) {
        self.key_count = cells.len() as u16;
        let mut end = self.data.len();

        for i in 0..cells.len() {
            let start = end - cells[i].payload_cell.len();
            unsafe {
                buffer::write_bytes(&mut self.data, start, &cells[i].payload_cell);
            }
            {
                let mut offsets = self.get_offsets_mut();
                offsets[i] = start as u16;
            }

            end = start;
        }
    }
}

impl <'a> PayloadCellView<'a> {
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
        table.get_null_flags_width_bytes() +
        self.key().len as usize +
        self.header.payload_inline_len as usize
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
    tmp_buf: &mut PageBuffer<A>,
    payload_cell: PayloadCellView<'a>,
    leaf_page: u32,
    page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    mut path: Vec<u32, A>,
    allocator: A
) -> Result<(), InsertErr<D::Error>> {
    unsafe {
        let _ = page_rw.read_page(leaf_page, leaf_buf.as_mut())?;
        let leaf = as_ref_mut!(leaf_buf, BtreeLeaf);
        let mut cells = leaf.read_btree_cells(table, allocator);

        if let Some(_) = cells.binary_search_by_key(payload_cell.key()) {
            return Err(InsertErr::DuplicateKey);
        }

        cells.push(BtreeCell::from_leaf_payload(payload_cell, payload_cell.get_size(table)));
        cells.sort_last_cell();

        buffer::copy_buf(tmp_buf.as_mut(), leaf_buf.as_ref());

        let leaf = as_ref_mut!(tmp_buf, BtreeLeaf);
        leaf.write_btree_cells(&cells);
        page_rw.write_page(leaf_page, tmp_buf.as_ref())?;
        Ok(())
    }
}

pub fn traverse_to_leaf<
    'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize
>(
    table: &Table,
    buf: &mut PageBuffer<A>,
    _key: &Key,
    page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    path: &mut Vec<u32, A>
) -> Result<u32, TableErr<D::Error>> {
    unsafe {
        let cur_page = table.rows_btree_page;
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
