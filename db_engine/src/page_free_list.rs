use crate::page_rw::{PageRW, PAGE_SIZE};
use embedded_sdmmc::{Error, BlockDevice};

#[repr(packed)]
pub struct PageFreeList {
    page_count: u32,
    next_page: u32,
    pages: [u32; (PAGE_SIZE / core::mem::size_of::<u32>()) - (core::mem::size_of::<u32>() * 2)] 
}

impl <D: BlockDevice> PageFreeList {
    pub fn get_free_page(&mut self, page_rw: &PageRW) -> Result<u32, Error<D::Error>> {
        let mut cur = self;
        let mut prev_page = 0;
        let mut cur_page = 1;
        while cur.next_page != 0 {
            prev_page = cur_page;
            cur_page_num = cur.next_page;
            page_rw.read_page();
        }
    }
}
