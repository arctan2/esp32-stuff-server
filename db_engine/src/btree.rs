use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;

#[repr(u8)]
pub enum NodeType {
    Internal,
    Leaf
}

#[repr(C, packed)]
pub struct BtreePage {
    node_type: NodeType,
    key_count: u32,
    next_leaf: u32,
    data: [u8; PAGE_SIZE - (size_of::<NodeType>() + (size_of::<u32>() * 2))]
}

impl BtreePage {
    pub fn new(node_type: NodeType) -> Self{
        Self {
            node_type: node_type,
            key_count: 0,
            next_leaf: 0,
            data: [0; PAGE_SIZE - (size_of::<NodeType>() + (size_of::<u32>() * 2))]
        }
    }
}

#[repr(C, packed)]
pub struct BtreeLeafPage<T> {
    node_type: NodeType,
    key_count: u32,
    next_leaf: u32,
}
