use std::fmt::Debug;
use embedded_sdmmc::{BlockDevice, Block, BlockIdx, BlockCount};
use std::sync::Mutex;

pub const BLOCK_SIZE: usize = 512;
pub const SD_BUF_SIZE: usize = 64 * 1024 * 1024;
pub const SD_BLOCK_COUNT: u32 = (SD_BUF_SIZE / BLOCK_SIZE) as u32;
static SD_BUF: Mutex<[u8; SD_BUF_SIZE]> = Mutex::new(*include_bytes!("../fat32.img"));

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

