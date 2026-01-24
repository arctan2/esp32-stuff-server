use core::mem::size_of;
use crate::page_rw::PAGE_SIZE;
use allocator_api2::alloc::Allocator;
use embedded_sdmmc::{BlockDevice, TimeSource};
use crate::{get_free_page, as_ref_mut, PageRW, PageFreeList};
use crate::types::{PageBuffer};
use crate::db::Error;

pub struct OverflowPage {
    next: u32,
    data: [u8; PAGE_SIZE - size_of::<u32>()]
}

impl OverflowPage {
    pub fn new_overflow_list<
        'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    > (
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>,
        slice: &'a [u8],
        buf: &mut PageBuffer<A>,
    ) -> Result<u32, Error<D::Error>> {
        unsafe {
            let new_page = get_free_page!(page_rw, buf)?;
            let mut cur_page = new_page;
            let _ = page_rw.read_page(cur_page, buf.as_mut())?;
            let mut overflow_page = as_ref_mut!(buf, OverflowPage);
            let mut start = 0;

            loop {
                let end = start + overflow_page.data.len().min(slice.len());
                overflow_page.data.copy_from_slice(&slice[start..end]);
                page_rw.write_page(cur_page, buf.as_ref())?;
                if end == slice.len() {
                    break;
                }
                start = end;

                let next_page = get_free_page!(page_rw, buf)?;

                let _ = page_rw.read_page(cur_page, buf.as_mut())?;
                overflow_page = as_ref_mut!(buf, OverflowPage);
                overflow_page.next = next_page;

                cur_page = next_page;
                let _ = page_rw.read_page(cur_page, buf.as_mut())?;
                overflow_page = as_ref_mut!(buf, OverflowPage);
            }

            Ok(new_page)
        }
    }
}
