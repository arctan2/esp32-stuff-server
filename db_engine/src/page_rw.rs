use crate::fs::PageFile;

#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "std")]
use std::sync::{LazyLock, Mutex};

#[cfg(feature = "std")]
pub static WRITES_REM: LazyLock<Mutex<usize>> = LazyLock::new(|| Mutex::new(27));

#[cfg(feature = "std")]
pub static PANICS_REM: LazyLock<Mutex<usize>> = LazyLock::new(|| Mutex::new(1));

pub const PAGE_SIZE: usize = 4096;

pub struct PageRW<F>
where F: PageFile
{
    pub file: F
}

impl <F> PageRW<F> where F: PageFile {
    pub fn new(file: F) -> Self {
        Self {
            file: file
        }
    }

    pub fn read_page(&self, page_num: u32, buf: &mut [u8; PAGE_SIZE]) -> Result<usize, F::Error> {
        let offset: u32 = page_num * buf.len() as u32;
        self.file.seek_from_start(offset)?;
        return self.file.read(buf);
    }

    #[cfg(not(feature = "hw_failure_test"))]
    pub fn write_page(&self, page_num: u32, buf: &[u8; PAGE_SIZE]) -> Result<(), F::Error> {
        let offset: u32 = page_num * buf.len() as u32;
        self.file.seek_from_start(offset)?;
        return self.file.write(buf);
    }

    #[cfg(feature = "hw_failure_test")]
    pub fn write_page(&self, page_num: u32, buf: &[u8; PAGE_SIZE]) -> Result<(), F::Error> {
        let mut writes_rem = WRITES_REM.lock().unwrap();
        let mut panics_rem = PANICS_REM.lock().unwrap();

        if *writes_rem == 0 && *panics_rem > 0 {
            if *panics_rem > 0 {
                *panics_rem -= 1;
            }
            core::mem::drop(writes_rem);
            core::mem::drop(panics_rem);
            panic!("world ended man");
        }

        if *writes_rem > 0 {
            *writes_rem -= 1;
        }

        let offset: u32 = page_num * buf.len() as u32;
        self.file.seek_from_start(offset)?;
        return self.file.write(buf);
    }

    // this accounts for any incomplete transactions
    // so that's the reason it takes cur_db_page_count and it compares it with actual pages count
    // from file length
    pub fn extend_file_one_page(&self, cur_db_page_count: u32, buf: &mut [u8; PAGE_SIZE]) -> Result<u32, F::Error> {
        let page = (self.file.length() / (PAGE_SIZE as u32)).min(cur_db_page_count);
        buf.fill(0);
        self.write_page(page, buf)?;
        Ok(page)
    }
}

