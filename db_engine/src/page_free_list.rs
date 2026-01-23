use crate::page_rw::{PageRW, PAGE_SIZE};
use crate::db::{Error, DBHeader, FixedPages};
use crate::types::PageBuffer;
use embedded_sdmmc::{BlockDevice, TimeSource};
use allocator_api2::alloc::Allocator;
use core::mem::size_of;
use crate::{as_ref_mut};

const PAGES_LIST_SIZE: usize = (PAGE_SIZE / size_of::<u32>()) - (size_of::<u32>() * 2);

#[derive(Debug)]
#[repr(packed)]
pub struct PageFreeList {
    page_count: u32,
    next_page: u32,
    pages: [u32; PAGES_LIST_SIZE] 
}

impl PageFreeList {
    pub unsafe fn get_free_page<
        'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    >(
        buf: &mut PageBuffer<A>,
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> Result<u32, Error<D::Error>> {
        unsafe {
            let mut prev_page = 0;
            let mut cur_page = 1;
            let _ = page_rw.read_page(FixedPages::FreeList.into(), buf.as_mut())?;
            let mut cur = as_ref_mut!(buf, PageFreeList);

            while cur.next_page != 0 {
                prev_page = cur_page;
                cur_page = cur.next_page;
                let _ = page_rw.read_page(cur_page, buf.as_mut())?;
                cur = as_ref_mut!(buf, PageFreeList);
            }

            let page: u32;
            if prev_page == 0 {
                if cur.page_count == 0 {
                    page = page_rw.extend_file_by_pages(1, buf.as_mut())?;
                    DBHeader::inc_page_count::<D, T, A, MAX_DIRS, MAX_FILES, MAX_VOLUMES>(buf.as_mut(), page_rw)?;
                    buf.as_mut().fill(0);
                } else {
                    page = cur.pages[0];
                    cur.pages[0] = cur.pages[(cur.page_count - 1) as usize];
                    cur.page_count -= 1;
                }
            } else {
                if cur.page_count == 0 {
                    page = cur_page;
                    cur_page = prev_page;
                    let _ = page_rw.read_page(prev_page, buf.as_mut())?;
                    cur = as_ref_mut!(buf, PageFreeList);
                    cur.next_page = 0;
                } else {
                    page = cur.pages[0];
                    cur.pages[0] = cur.pages[(cur.page_count - 1) as usize];
                    cur.page_count -= 1;
                }
            }

            let _ = page_rw.write_page(cur_page, buf.as_ref())?;
            buf.as_mut().fill(0);
            Ok(page)
        }
    }

    pub unsafe fn add_page_to_list<
        'a, D: BlockDevice, T: TimeSource, A: Allocator + Clone,
        const MAX_DIRS: usize,
        const MAX_FILES: usize,
        const MAX_VOLUMES: usize
    >(
        buf: &mut PageBuffer<A>,
        page_num: u32,
        page_rw: &PageRW<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
    ) -> Result<(), Error<D::Error>> {
        unsafe {
            let mut cur_page = 1;
            let _ = page_rw.read_page(FixedPages::FreeList.into(), buf.as_mut())?;
            let mut cur = as_ref_mut!(buf, PageFreeList);
            loop {
                if cur.page_count < (PAGES_LIST_SIZE as u32) || cur.next_page == 0 {
                    break;
                }
                cur_page = cur.next_page;
                let _ = page_rw.read_page(cur.next_page, buf.as_mut())?;
                cur = as_ref_mut!(buf, PageFreeList);
            }

            if cur.page_count < (PAGES_LIST_SIZE as u32) {
                cur.pages[cur.page_count as usize] = page_num;
                cur.page_count += 1;
            } else {
                cur.next_page = page_num;
            }
            let _ = page_rw.write_page(cur_page, buf.as_ref())?;
            buf.as_mut().fill(0);
            let _ = page_rw.write_page(page_num, buf.as_ref())?;
        }

        Ok(())
    }
}
