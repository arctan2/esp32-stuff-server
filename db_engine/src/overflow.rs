use core::mem::size_of;
use crate::page_rw::{PAGE_SIZE, PageRW};
use crate::page_free_list::PageFreeList;
use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;
use crate::{get_free_page, as_ref_mut, as_ref};
use crate::page_buf::{PageBuffer};
use crate::fs::{PageFile};
use crate::db::Error;

pub struct OverflowPage {
    next: u32,
    data: [u8; PAGE_SIZE - size_of::<u32>()]
}

impl OverflowPage {
    pub fn new_overflow_list<'a, F: PageFile, A: Allocator + Clone> (
        page_rw: &PageRW<F>,
        slice: &'a [u8],
        buf: &mut PageBuffer<A>,
    ) -> Result<u32, Error<F::Error>> {
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

     pub fn read_all<'a, F: PageFile, A: Allocator + Clone>(
        page_rw: &PageRW<F>,
        mut page_num: u32,
        v: &mut Vec<u8, A>,
        buf: &mut PageBuffer<A>,
    ) -> Result<(), Error<F::Error>> {
        unsafe {
            let mut remaining = v.capacity();

            while page_num != 0 && remaining > 0 {
                page_rw.read_page(page_num, buf.as_mut())?;
                let ov = as_ref!(buf, OverflowPage);

                let chunk_size = ov.data.len().min(remaining);
                v.extend_from_slice(&ov.data[..chunk_size]);

                remaining -= chunk_size;
                page_num = ov.next;
            }

            v.truncate(v.capacity());

            Ok(())
        }
    }
}
