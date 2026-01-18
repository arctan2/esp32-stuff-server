mod block_device;
mod allocators;
use allocators::{SimAllocator, INTERNAL_HEAP, PSRAM_HEAP};
use embedded_sdmmc::{VolumeManager, TimeSource, Timestamp, Mode};
use allocator_api2::vec::Vec;

pub mod esp_alloc {
    use super::*;
    pub const InternalMemory: SimAllocator<17> = SimAllocator(&INTERNAL_HEAP);
    pub const ExternalMemory: SimAllocator<23> = SimAllocator(&PSRAM_HEAP);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let sdcard = block_device::RamBlockDevice::new();
    let mut volume_mgr = VolumeManager::new(sdcard, DummyTimesource::default());
    allocators::init_simulated_hardware();

    let mut v: Vec<i32, _> = Vec::new_in(esp_alloc::InternalMemory);

    for i in 0..1000 {
        v.push(i);
    }

    match volume_mgr.open_volume(embedded_sdmmc::VolumeIdx(0)) {
        Ok(mut volume) => {
            let mut root_dir = volume.open_root_dir().expect("Failed to open root dir");

            println!("Listing Directory Contents:");

            root_dir.iterate_dir(|entry| {
                println!(
                    "  {} | Name: {} | Size: {} bytes",
                    if entry.attributes.is_directory() { "[DIR] " } else { "[FILE]" },
                    entry.name,
                    entry.size
                );
            }).expect("Failed to iterate directory");
            match root_dir.open_file_in_dir("LAME_F~1.TXT", Mode::ReadOnly) {
                Ok(mut file) => {
                    let mut buffer = [0u8; 64]; 

                    let bytes_read = file.read(&mut buffer).expect("Error reading file");

                    if let Ok(content) = core::str::from_utf8(&buffer[..bytes_read]) {
                        println!("--- FILE CONTENT START ---");
                        println!("{}", content);
                        println!("--- FILE CONTENT END ---");
                        println!("Read {} bytes successfully.", bytes_read);
                    } else {
                        println!("File contains invalid UTF-8 data.");
                    }
                }
                Err(e) => {
                    println!("Could not open file: {:?}", e);
                }
            }
        }
        Err(e) => {
            println!("Could not open volume: {:?}", e);
        }
    }
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
