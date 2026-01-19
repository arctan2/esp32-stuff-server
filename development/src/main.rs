#![allow(nonstandard_style)]
mod block_device;
mod allocators;
use allocators::{SimAllocator, INTERNAL_HEAP, PSRAM_HEAP};
use embedded_sdmmc::{VolumeManager, TimeSource, Timestamp};

pub mod esp_alloc {
    use super::*;
    pub const InternalMemory: SimAllocator<17> = SimAllocator(&INTERNAL_HEAP);
    pub const ExternalMemory: SimAllocator<23> = SimAllocator(&PSRAM_HEAP);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    allocators::init_simulated_hardware();
    let sdcard = block_device::RamBlockDevice::new();
    let vol_man = VolumeManager::new(sdcard, DummyTimesource());
    let volume = vol_man.open_volume(embedded_sdmmc::VolumeIdx(0)).unwrap();
    let root_dir = volume.open_root_dir().unwrap();
    let _ = root_dir.make_dir_in_dir("STUFF").unwrap();
    let stuff_dir = root_dir.open_dir("STUFF").unwrap();
    let db_file = stuff_dir.open_file_in_dir("DB", embedded_sdmmc::Mode::ReadWriteCreateOrAppend).unwrap();
    let mut db = db_engine::db::Database::new(db_file, esp_alloc::ExternalMemory);
    db.init().unwrap();
}

#[derive(Default)]
pub struct DummyTimesource();

impl TimeSource for DummyTimesource {
    fn get_timestamp(&self) -> Timestamp {
        Timestamp {
            year_since_1970: 0,
            zero_indexed_month: 0,
            zero_indexed_day: 0,
            hours: 0,
            minutes: 0,
            seconds: 0,
        }
    }
}
