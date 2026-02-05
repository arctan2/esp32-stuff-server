use crate::fs::PageFile;

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

    pub fn write_page(&self, page_num: u32, buf: &[u8; PAGE_SIZE]) -> Result<(), F::Error> {
        let offset: u32 = page_num * buf.len() as u32;
        self.file.seek_from_start(offset)?;
        return self.file.write(buf);
    }

    pub fn extend_file_by_pages(&self, count: u32, buf: &mut [u8; PAGE_SIZE]) -> Result<u32, F::Error> {
        let cur_page_count = self.file.length() / (PAGE_SIZE as u32);
        buf.fill(0);
        for i in 0..count {
            self.write_page(cur_page_count + i, buf)?;
        }
        Ok(cur_page_count)
    }
}

