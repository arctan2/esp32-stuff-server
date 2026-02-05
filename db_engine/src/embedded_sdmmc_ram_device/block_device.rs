#[cfg(feature = "std")]
extern crate std;

use std::println;
use std::fmt::Debug;
use embedded_sdmmc::{BlockDevice, Block, BlockIdx, BlockCount};
use std::sync::Mutex;

use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;

pub const BLOCK_SIZE: usize = 512;
pub const SD_BUF_SIZE: usize = 64 * 1024 * 1024;
pub const SD_BLOCK_COUNT: u32 = (SD_BUF_SIZE / BLOCK_SIZE) as u32;
static SD_BUF: Mutex<[u8; SD_BUF_SIZE]> = Mutex::new(*include_bytes!("../../fat32.img"));

#[derive(Debug)]
pub enum RamBlockDeviceError {
    OutOfRange,
}

pub struct RamBlockDevice;

impl RamBlockDevice {
    pub fn new() -> Self {
        RamBlockDevice
    }

    fn offset(idx: BlockIdx) -> usize {
        idx.0 as usize * BLOCK_SIZE
    }
}

impl BlockDevice for RamBlockDevice {
    type Error = RamBlockDeviceError;

    fn read(
        &self,
        blocks: &mut [Block],
        start_block_idx: BlockIdx,
    ) -> Result<(), Self::Error> {
        let buf = SD_BUF.lock().unwrap();

        for (i, blk) in blocks.iter_mut().enumerate() {
            let idx = BlockIdx(start_block_idx.0 + i as u32);

            if idx.0 >= SD_BLOCK_COUNT {
                return Err(RamBlockDeviceError::OutOfRange);
            }

            let off = Self::offset(idx);
            blk.contents.copy_from_slice(&buf[off..off + BLOCK_SIZE]);
        }

        Ok(())
    }

    fn write(
        &self,
        blocks: &[Block],
        start_block_idx: BlockIdx,
    ) -> Result<(), Self::Error> {
        let mut buf = SD_BUF.lock().unwrap();

        for (i, blk) in blocks.iter().enumerate() {
            let idx = BlockIdx(start_block_idx.0 + i as u32);

            if idx.0 >= SD_BLOCK_COUNT {
                return Err(RamBlockDeviceError::OutOfRange);
            }

            let off = Self::offset(idx);
            buf[off..off + BLOCK_SIZE].copy_from_slice(&blk.contents);
        }

        Ok(())
    }

    fn num_blocks(&self) -> Result<BlockCount, Self::Error> {
        Ok(BlockCount(SD_BLOCK_COUNT))
    }
}

pub const DEFAULT_FILE_SIZE: u64 = 64 * 1024 * 1024; 

#[derive(Debug)]
pub enum FsBlockDeviceError {
    IoError(std::io::Error),
    OutOfRange,
}

#[derive(Debug)]
pub struct FsBlockDevice {
    file: Mutex<File>,
    block_count: u32,
}

impl FsBlockDevice {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, FsBlockDeviceError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(FsBlockDeviceError::IoError)?;

        let metadata = file.metadata().map_err(FsBlockDeviceError::IoError)?;
        let current_size = metadata.len();
        if current_size == 0 {
            file.set_len(DEFAULT_FILE_SIZE).map_err(FsBlockDeviceError::IoError)?;
        }

        let boot_image = include_bytes!("../../fat32.img");
        file.write_all(boot_image).map_err(FsBlockDeviceError::IoError)?;
        file.sync_all().map_err(FsBlockDeviceError::IoError)?;

        let actual_size = file.metadata().map_err(FsBlockDeviceError::IoError)?.len();
        let block_count = (actual_size / BLOCK_SIZE as u64) as u32;

        Ok(FsBlockDevice {
            file: Mutex::new(file),
            block_count,
        })
    }

    fn offset(idx: BlockIdx) -> u64 {
        idx.0 as u64 * BLOCK_SIZE as u64
    }
}

impl BlockDevice for FsBlockDevice {
    type Error = FsBlockDeviceError;

    fn read(
        &self,
        blocks: &mut [Block],
        start_block_idx: BlockIdx,
    ) -> Result<(), Self::Error> {
        let mut file = self.file.lock().unwrap();

        for (i, blk) in blocks.iter_mut().enumerate() {
            let idx = BlockIdx(start_block_idx.0 + i as u32);
            if idx.0 >= self.block_count {
                return Err(FsBlockDeviceError::OutOfRange);
            }

            file.seek(SeekFrom::Start(Self::offset(idx)))
                .map_err(FsBlockDeviceError::IoError)?;
            file.read_exact(&mut blk.contents)
                .map_err(FsBlockDeviceError::IoError)?;
        }

        Ok(())
    }

    fn write(
        &self,
        blocks: &[Block],
        start_block_idx: BlockIdx,
    ) -> Result<(), Self::Error> {
        let mut file = self.file.lock().unwrap();

        for (i, blk) in blocks.iter().enumerate() {
            let idx = BlockIdx(start_block_idx.0 + i as u32);
            if idx.0 >= self.block_count {
                return Err(FsBlockDeviceError::OutOfRange);
            }

            file.seek(SeekFrom::Start(Self::offset(idx)))
                .map_err(FsBlockDeviceError::IoError)?;
            file.write_all(&blk.contents)
                .map_err(FsBlockDeviceError::IoError)?;
        }

        Ok(())
    }

    fn num_blocks(&self) -> Result<BlockCount, Self::Error> {
        Ok(BlockCount(self.block_count))
    }
}
