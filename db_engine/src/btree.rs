#![allow(unused)]
use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;
use core::cmp::Ordering;
use crate::types::{PageBuffer, PageBufferWriter, PageBufferReader};
use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;
use crate::{as_ref, as_ref_mut, PageRW, get_free_page, PageFreeList};
use crate::table::{Table, TableErr};
use crate::serde_row::{Value, SerializedRow};
use crate::overflow::OverflowPage;
use crate::db::{Error, InsertErr};
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
    None = 0,
    Internal = 1,
    Leaf = 2
}

#[derive(Debug)]
#[repr(C, packed)]
pub struct BtreePage {
    pub node_type: NodeType,
    pub data: [u8; PAGE_SIZE - size_of::<NodeType>()]
}

const LEAF_META_SIZE: usize = size_of::<NodeType>() + size_of::<u32>() + size_of::<u16>();
#[derive(Debug)]
#[repr(C, packed)]
pub struct BtreeLeaf {
    pub node_type: NodeType,
    pub key_count: u16,
    pub next_leaf: u32,
    pub data: [u8; PAGE_SIZE - LEAF_META_SIZE]
}

const INTERNAL_META_SIZE: usize = size_of::<NodeType>() + size_of::<u32>() + size_of::<u16>();
#[derive(Debug)]
#[repr(C, packed)]
pub struct BtreeInternal {
    pub node_type: NodeType,
    pub left_child: u32,
    pub key_count: u16,
    pub data: [u8; PAGE_SIZE - INTERNAL_META_SIZE]
}

#[derive(Debug, Copy, Clone)]
#[repr(C, packed)]
pub struct Key {
    pub len: u8,
    /* [u8; len] in memory comes here */
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

#[derive(Debug)]
#[repr(C, packed)]
pub struct InternalCellHeader {
    pub child: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct InternalCellView<'a> {
    pub header: &'a InternalCellHeader,
    pub data: &'a [u8]
}

impl Key {
    pub fn as_bytes<'a>(&self) -> &'a [u8] {
        unsafe {
            let data_ptr = (self as *const Key as *const u8);
            core::slice::from_raw_parts(data_ptr, (self.len + 1) as usize)
        }
    }
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

impl <'a> PayloadCellView<'a> {
    fn new_unsafe(table: &Table, raw_buf_ptr: *const u8, size: usize, offset: usize) -> Self {
        let raw_buf = unsafe { core::slice::from_raw_parts(raw_buf_ptr, size) };
        let (header_bytes, data_bytes) = raw_buf[offset..].split_at(core::mem::size_of::<PayloadCellHeader>());
        let header = unsafe { &*(header_bytes.as_ptr() as *const PayloadCellHeader) };
        let mut s = Self { header, data: data_bytes };
        s.data = &s.data[0..s.data_size(table)];
        return s;
    }

    pub fn new(table: &Table, raw_buf: &'a [u8], offset: usize) -> Self {
        let (header_bytes, data_bytes) = raw_buf[offset..].split_at(core::mem::size_of::<PayloadCellHeader>());
        let header = unsafe { &*(header_bytes.as_ptr() as *const PayloadCellHeader) };
        let mut s = Self { header, data: data_bytes };
        s.data = &s.data[0..s.data_size(table)];
        return s;
    }

    fn null_flags(&self, width: usize) -> &'a [u8] {
        let key_len = self.data[0] as usize;
        let start = 1 + key_len;
        &self.data[start..start + width]
    }

    pub fn payload(&self, null_flags_width: usize) -> &'a [u8] {
        let key_len = self.data[0] as usize;
        let start = 1 + key_len + null_flags_width;
        &self.data[start..start + self.header.payload_inline_len as usize]
    }

    pub fn key(&self) -> &'a Key {
        let key_len = self.data[0] as usize;
        unsafe { &*(self.data[0..1 + key_len].as_ptr() as *const Key) as &Key }
    }

    fn data_size(&self, table: &Table) -> usize {
        size_of::<u8>() +
        table.get_null_flags_width_bytes() +
        self.key().len as usize +
        self.header.payload_inline_len as usize
    }

    fn size(&self, table: &Table) -> usize {
        (size_of::<u32>() * 2) +
        (size_of::<u8>() * 2) +
        table.get_null_flags_width_bytes() +
        self.key().len as usize +
        self.header.payload_inline_len as usize
    }

    fn as_bytes(&self, table: &Table) -> &'a [u8] {
        unsafe {
            let ptr = self.header as *const PayloadCellHeader as *const u8;
            core::slice::from_raw_parts(ptr, self.size(table))
        }
    }

    pub fn new_to_buf<
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
}

impl <'a> InternalCellView<'a> {
    pub fn new(raw_buf: &'a [u8], offset: usize) -> Self {
        let (header_bytes, data_bytes) = raw_buf[offset..].split_at(core::mem::size_of::<InternalCellHeader>());
        let header = unsafe { &*(header_bytes.as_ptr() as *const InternalCellHeader) };
        let mut s = Self { header, data: data_bytes };
        s.data = &s.data[0..s.data_size()];
        return s;
    }

    pub fn key(&self) -> &'a Key {
        let key_len = self.data[0] as usize;
        unsafe { &*(self.data[0..1 + key_len].as_ptr() as *const Key) as &Key }
    }

    pub fn data_size(&self) -> usize {
        size_of::<u8>() + self.key().len as usize
    }

    pub fn size(&self) -> usize {
        size_of::<u32>() + size_of::<u8>() + self.key().len as usize
    }

    pub fn as_bytes(&self) -> &'a [u8] {
        unsafe {
            let ptr = self.header as *const InternalCellHeader as *const u8;
            core::slice::from_raw_parts(ptr, self.size())
        }
    }

    pub fn new_to_buf<
        D: BlockDevice, T: TimeSource, A: Allocator + Clone,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    >(
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
        buf: &mut PageBuffer<A>,
        child: u32,
        key: &Key,
    ) -> Result<(), Error<D::Error>> {
        let mut buf_writer = PageBufferWriter::new(buf);
        buf_writer.write(&child);
        buf_writer.write_slice(key.as_bytes());
        Ok(())
    }
}

#[derive(Debug)]
pub struct BtreeCell<'a> {
    key: &'a Key,
    payload_cell: &'a [u8],
}

impl <'a> BtreeCell<'a> {
    pub fn from_leaf_view(view: PayloadCellView<'a>, table: &Table) -> Self {
        Self {
            key: view.key(),
            payload_cell: view.as_bytes(table)
        }
    }

    pub fn from_internal_view(view: InternalCellView<'a>) -> Self {
        Self {
            key: view.key(),
            payload_cell: view.as_bytes()
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
    }

    pub fn get_offsets_mut(&mut self) -> &mut [u16] {
        let ptr = self.data.as_ptr();
        return unsafe {
            core::slice::from_raw_parts_mut(ptr as *mut u16, self.key_count as usize)
        };
    }

    pub fn get_offsets(&self) -> &[u16] {
        let ptr = self.data.as_ptr();
        return unsafe {
            core::slice::from_raw_parts(ptr as *const u16, self.key_count as usize)
        };
    }

    pub fn get_payload_cell(&self, table: &Table, idx: usize) -> Option<PayloadCellView<'_>> {
        let offsets = self.get_offsets();
        if idx < offsets.len() {
            Some(PayloadCellView::new(table, &self.data, offsets[idx] as usize))
        } else {
            None
        }
    }

    pub fn find_payload_by_key(&self, table: &Table, key: &Key) -> Option<PayloadCellView<'_>> {
        let offsets = self.get_offsets();
        let mut l = 0;
        let mut h = offsets.len();

        while l < h {
            let m = (l + h) / 2;
            let cell = PayloadCellView::new(table, &self.data, offsets[m] as usize);

            if cell.key() == key {
                return Some(cell);
            }

            if cell.key() < key {
                l = m + 1;
            } else {
                h = m;
            }
        }

        return None;
    }

    pub fn read_btree_cells<'a, A: Allocator>(&'a self, table: &Table, allocator: A) -> BtreeCells<'a, A> {
        unsafe {
            let mut cells: BtreeCells<A> = BtreeCells::new_in(allocator);
            let ptr = self.data.as_ptr();
            let offsets: &[u16] = unsafe {
                core::slice::from_raw_parts(ptr as *const u16, self.key_count as usize)
            };

            for i in offsets.iter() {
                let payload_cell = PayloadCellView::new(table, &self.data, *i as usize);
                let cell = BtreeCell::from_leaf_view(payload_cell, table);
                cells.push(cell);
            }

            return cells;
        }
    }

    pub fn write_btree_cells<A: Allocator>(&mut self, cells: &BtreeCells<A>, start_idx: usize, end_idx: usize) {
        self.key_count = (end_idx - start_idx) as u16;
        let mut end = self.data.len();
        let mut offset_idx = 0;

        for i in start_idx..end_idx {
            let start = end - cells[i].payload_cell.len();
            unsafe {
                buffer::write_bytes(&mut self.data, start, &cells[i].payload_cell);
            }
            {
                let mut offsets = self.get_offsets_mut();
                offsets[offset_idx] = start as u16;
                offset_idx += 1;
            }

            end = start;
        }
    }
}

impl BtreeInternal {
    pub fn init(&mut self) {
        self.node_type = NodeType::Internal;
    }

    pub fn get_offsets_mut(&mut self) -> &mut [u16] {
        let ptr = self.data.as_ptr();
        return unsafe {
            core::slice::from_raw_parts_mut(ptr as *mut u16, self.key_count as usize)
        };
    }

    pub fn get_offsets(&self) -> &[u16] {
        let ptr = self.data.as_ptr();
        return unsafe {
            core::slice::from_raw_parts(ptr as *const u16, self.key_count as usize)
        };
    }

    pub fn next_child_by_key(&self, key: &Key) -> u32 {
        let offsets = self.get_offsets();
        let mut l = 0;
        let mut h = offsets.len();

        while l < h {
            let m = (l + h) / 2;
            let cell = InternalCellView::new(&self.data, offsets[m] as usize);

            if cell.key() < key {
                l = m + 1;
            } else {
                h = m;
            }
        }

        if l == 0 {
            return self.left_child;
        } else {
            let cell = InternalCellView::new(&self.data, offsets[l - 1] as usize);
            return cell.header.child;
        }

    }

    pub fn read_btree_cells<'a, A: Allocator>(&'a self, allocator: A) -> BtreeCells<'a, A> {
        unsafe {
            let mut cells: BtreeCells<A> = BtreeCells::new_in(allocator);
            let ptr = self.data.as_ptr();
            let offsets: &[u16] = unsafe {
                core::slice::from_raw_parts(ptr as *const u16, self.key_count as usize)
            };

            for i in offsets.iter() {
                let payload_cell = InternalCellView::new(&self.data, *i as usize);
                let cell = BtreeCell::from_internal_view(payload_cell);
                cells.push(cell);
            }

            return cells;
        }
    }

    pub fn write_btree_cells<A: Allocator>(&mut self, cells: &BtreeCells<A>, start_idx: usize, end_idx: usize) {
        self.key_count = (end_idx - start_idx) as u16;
        let mut end = self.data.len();
        let mut offset_idx = 0;

        for i in start_idx..end_idx {
            let start = end - cells[i].payload_cell.len();
            unsafe {
                buffer::write_bytes(&mut self.data, start, &cells[i].payload_cell);
            }
            {
                let mut offsets = self.get_offsets_mut();
                offsets[offset_idx] = start as u16;
                offset_idx += 1;
            }

            end = start;
        }
    }
}

pub fn promote_key_iter<
    'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize
>(
    promoted_key_buf: &mut PageBuffer<A>,
    buf1: &'a mut PageBuffer<A>,
    buf2: &mut PageBuffer<A>,
    buf3: &mut PageBuffer<A>,
    table: &mut Table,
    mut path: Vec<u32, A>,
    page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    mut left: u32,
    mut right: u32,
    allocator: A
) -> Result<(), Error<D::Error>> {
    loop { unsafe {
        let promoted_key = as_ref!(promoted_key_buf, Key);
        
        if let Some(internal_page) = path.pop() {
            let _ = page_rw.read_page(internal_page, buf1.as_mut())?;
            let internal = as_ref_mut!(buf1, BtreeInternal);
            let mut cells = internal.read_btree_cells(allocator.clone());

            InternalCellView::new_to_buf(page_rw, buf2, right, promoted_key)?;
            let internal_cell = InternalCellView::new(buf2.as_ref(), 0);
            cells.push(BtreeCell::from_internal_view(internal_cell));
            cells.sort_last_cell();

            let total_size = cells.total_size() + (size_of::<u16>() * cells.len());

            if total_size >= internal.data.len() {
                let mid = cells.len() / 2;
                let new_page = get_free_page!(page_rw, buf3)?;

                {
                    let right_child = as_ref_mut!(buf3, BtreeInternal);
                    right_child.init();
                    right_child.write_btree_cells(&cells, mid, cells.len());
                    page_rw.write_page(new_page, buf3.as_ref())?;
                }

                {
                    buffer::copy_buf(buf3.as_mut(), buf1.as_ref());
                    let tmp_internal = as_ref_mut!(buf3, BtreeInternal);
                    tmp_internal.write_btree_cells(&cells, 0, mid);
                }

                let promoted_key = cells[mid].key;
                buffer::write_bytes(promoted_key_buf.as_mut(), 0, promoted_key.as_bytes());
                page_rw.write_page(internal_page, buf3.as_ref());
                left = internal_page;
                right = new_page;
            } else {
                buffer::copy_buf(buf3.as_mut(), buf1.as_ref());
                let tmp_internal = as_ref_mut!(buf3, BtreeInternal);
                tmp_internal.write_btree_cells(&cells, 0, cells.len());
                page_rw.write_page(internal_page, buf3.as_ref())?;
                break;
            }
        } else {
            let mut cells = Vec::new_in(allocator);
            InternalCellView::new_to_buf(page_rw, buf1, right, promoted_key)?;
            let internal_cell = InternalCellView::new(buf1.as_ref(), 0);
            cells.push(BtreeCell::from_internal_view(internal_cell));

            let new_internal_page_buf = buf2;
            let new_internal_page = get_free_page!(page_rw, new_internal_page_buf)?;
            let internal = as_ref_mut!(new_internal_page_buf, BtreeInternal);
            internal.init();
            internal.left_child = left;
            internal.write_btree_cells(&cells, 0, cells.len());
            page_rw.write_page(new_internal_page, new_internal_page_buf.as_ref())?;
            table.rows_btree_page = new_internal_page;
            break;
        }
    }}

    return Ok(());
}

pub fn split_leaf_iter<
    'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize
>(
    payload_cell_buf: &mut PageBuffer<A>,
    leaf_buf: &mut PageBuffer<A>,
    tmp_buf1: &mut PageBuffer<A>,
    tmp_buf2: &mut PageBuffer<A>,
    leaf_page: u32,
    table: &mut Table,
    page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    mut path: Vec<u32, A>,
    cells: BtreeCells<'a, A>,
    allocator: A
) -> Result<(), Error<D::Error>> {
    // you have be careful here because everything here is literally the spiderman pointing each other meme
    // * cells references leaf_buf
    // * atleast 1 cell reference payload_cell_buf
    // * copy right half to new page which is tmp_buf and write it immediately to storage
    // * copy the leaf_buf into tmp_buf and copy left half to tmp_buf1 (don't write it to storage yet)
    // * copy the promoted_key into tmp_buf2
    // * now I write the tmp_buf1 to storage
    // * current state: tmp_buf1 has the promoted_key and [payload_cell_buf, leaf_buf, tmp_buf1] are free

    unsafe {
        let mid = cells.len() / 2;
        let new_leaf_page = get_free_page!(page_rw, tmp_buf1)?;

        {
            let new_leaf = as_ref_mut!(tmp_buf1, BtreeLeaf);
            new_leaf.init();
            new_leaf.write_btree_cells(&cells, mid, cells.len());
            page_rw.write_page(new_leaf_page, tmp_buf1.as_ref())?;
        }

        {
            buffer::copy_buf(tmp_buf1.as_mut(), leaf_buf.as_ref());
            let tmp_leaf = as_ref_mut!(tmp_buf1, BtreeLeaf);
            tmp_leaf.next_leaf = new_leaf_page;
            tmp_leaf.write_btree_cells(&cells, 0, mid);
        }

        let promoted_key_buf = tmp_buf2;
        let promoted_key = cells[mid].key;
        buffer::write_bytes(promoted_key_buf.as_mut(), 0, promoted_key.as_bytes());
        page_rw.write_page(leaf_page, tmp_buf1.as_ref());
        return promote_key_iter(
            promoted_key_buf, leaf_buf,
            payload_cell_buf, tmp_buf1,
            table, path, page_rw,
            leaf_page, new_leaf_page,
            allocator
        );
    }
}

pub fn insert_payload_to_leaf<
    'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize
>(
    payload_cell_buf: &mut PageBuffer<A>,
    leaf_buf: &mut PageBuffer<A>,
    tmp_buf1: &mut PageBuffer<A>,
    tmp_buf2: &mut PageBuffer<A>,
    leaf_page: u32,
    table: &mut Table,
    page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    mut path: Vec<u32, A>,
    allocator: A
) -> Result<(), Error<D::Error>> {
    unsafe {
        let payload_cell = PayloadCellView::new_unsafe(table, unsafe { payload_cell_buf.as_ptr(0) }, PAGE_SIZE, 0);
        let _ = page_rw.read_page(leaf_page, leaf_buf.as_mut())?;
        let leaf = as_ref!(leaf_buf, BtreeLeaf);
        let mut cells = leaf.read_btree_cells(table, allocator.clone());

        if let Some(_) = cells.binary_search_by_key(payload_cell.key()) {
            return Err(Error::Insert(InsertErr::DuplicateKey));
        }

        cells.push(BtreeCell::from_leaf_view(payload_cell, table));
        cells.sort_last_cell();

        let total_size = cells.total_size() + (size_of::<u16>() * cells.len());

        if total_size >= leaf.data.len() {
            split_leaf_iter(
                payload_cell_buf, leaf_buf,
                tmp_buf1, tmp_buf2,
                leaf_page, table, page_rw,
                path, cells, allocator
            )?;
        } else {
            buffer::copy_buf(tmp_buf1.as_mut(), leaf_buf.as_ref());
            let tmp_leaf = as_ref_mut!(tmp_buf1, BtreeLeaf);
            tmp_leaf.write_btree_cells(&cells, 0, cells.len());
            page_rw.write_page(leaf_page, tmp_buf1.as_ref())?;
        }

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
    tmp_buf: &mut PageBuffer<A>,
    key: &Key,
    page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
    path: &mut Vec<u32, A>
) -> Result<u32, TableErr<D::Error>> {
    unsafe {
        let mut cur_page = table.rows_btree_page;
        loop {
            let _ = page_rw.read_page(cur_page, tmp_buf.as_mut());
            let btree_page = as_ref!(tmp_buf, BtreePage);
            if btree_page.node_type == NodeType::Leaf {
                break;
            }
            let btree_internal = as_ref!(tmp_buf, BtreeInternal);
            let next_child = btree_internal.next_child_by_key(key);
            path.push(cur_page);
            cur_page = next_child;
        }

        return Ok(cur_page);
    }
}

pub fn traverse_to_leaf_no_path<
    'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize
>(
    table: &Table,
    tmp_buf: &mut PageBuffer<A>,
    key: &Key,
    page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
) -> Result<u32, TableErr<D::Error>> {
    unsafe {
        let mut cur_page = table.rows_btree_page;
        loop {
            let _ = page_rw.read_page(cur_page, tmp_buf.as_mut());
            let btree_page = as_ref!(tmp_buf, BtreePage);
            if btree_page.node_type == NodeType::Leaf {
                break;
            }
            let btree_internal = as_ref!(tmp_buf, BtreeInternal);
            cur_page = btree_internal.next_child_by_key(key);
        }

        return Ok(cur_page);
    }
}

pub fn traverse_to_left_most<
    'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize
>(
    table: &Table,
    tmp_buf: &mut PageBuffer<A>,
    page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
) -> Result<u32, TableErr<D::Error>> {
    unsafe {
        let mut cur_page = table.rows_btree_page;
        loop {
            let _ = page_rw.read_page(cur_page, tmp_buf.as_mut());
            let btree_page = as_ref!(tmp_buf, BtreePage);
            if btree_page.node_type == NodeType::Leaf {
                break;
            }
            let btree_internal = as_ref!(tmp_buf, BtreeInternal);
            cur_page = btree_internal.left_child;
        }

        return Ok(cur_page);
    }
}

pub struct Cursor<'a, A: Allocator + Clone> {
    page: u32,
    buf: &'a mut PageBuffer<A>,
    cur_idx: usize,
}

impl <'a, A: Allocator + Clone> Cursor<'a, A> {
    pub fn new<
        D: BlockDevice, T: TimeSource,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    >(
        table: &Table,
        buf: &'a mut PageBuffer<A>,
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> Result<Self, TableErr<D::Error>> {
        let left_most_page = traverse_to_left_most(table, buf, page_rw)?;

        Ok(Self {
            page: left_most_page,
            buf: buf,
            cur_idx: 0
        })
    }

    pub fn next<
        D: BlockDevice, T: TimeSource,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    >(
        &mut self,
        table: &Table,
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> Result<PayloadCellView<'_>, Error<D::Error>> {
        let mut leaf = unsafe { as_ref!(self.buf, BtreeLeaf) };
        if self.cur_idx >= leaf.key_count as usize {
            if leaf.next_leaf == 0 {
                return Err(Error::EndOfRecords);
            }
            let _ = page_rw.read_page(leaf.next_leaf, self.buf.as_mut())?;
            leaf = unsafe { as_ref!(self.buf, BtreeLeaf) };
            self.cur_idx = 0;
        }
        let cur_idx = self.cur_idx;
        self.cur_idx += 1;
        if let Some(view) = leaf.get_payload_cell(table, cur_idx) {
            Ok(view)
        } else {
            Err(Error::EndOfRecords)
        }
    }
}
