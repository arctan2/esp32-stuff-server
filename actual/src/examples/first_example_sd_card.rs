#![no_std]
#![no_main]
#![feature(alloc_error_handler)]

extern crate alloc;

use esp_backtrace as _;
use esp_alloc as _;
use esp_hal::{
    clock::CpuClock,
    gpio::{Level, Output, OutputConfig, Input, InputConfig, Pull},
    main,
    time::{Duration, Instant, Rate},

    spi::{master::{Spi, Config}},
    delay::{Delay},
};
use esp_println::println;
use embedded_sdmmc::{SdCard, TimeSource, Timestamp};
use embedded_hal_bus::spi::ExclusiveDevice;
use embedded_hal::delay::DelayNs;
use embedded_hal::spi::SpiBus;
use embedded_sdmmc::{VolumeManager, Mode};

esp_bootloader_esp_idf::esp_app_desc!();

fn disable_sim800L_modem(gpio4: esp_hal::peripherals::GPIO4, gpio23: esp_hal::peripherals::GPIO23) {
    let mut modem_pwr = esp_hal::gpio::Output::new(
        gpio4, 
        esp_hal::gpio::Level::Low, 
        esp_hal::gpio::OutputConfig::default()
    );

    let mut modem_key = esp_hal::gpio::Output::new(
        gpio23, 
        esp_hal::gpio::Level::Low, 
        esp_hal::gpio::OutputConfig::default()
    );

    modem_pwr.set_low(); 
    modem_key.set_low();

    esp_hal::delay::Delay::new().delay_ms(500u32);
}

#[main]
fn main() -> ! {
    // esp_alloc::heap_allocator!(size: 32 * 1024);

    let config = esp_hal::Config::default().with_cpu_clock(esp_hal::clock::CpuClock::max());
    let peripherals = esp_hal::init(config);

    disable_sim800L_modem(peripherals.GPIO4, peripherals.GPIO23);

    let sck  = peripherals.GPIO13; 
    let mosi = peripherals.GPIO14;
    let miso = peripherals.GPIO32;

    let mut spi = esp_hal::spi::master::Spi::new(
        peripherals.SPI2,
        esp_hal::spi::master::Config::default()
            .with_frequency(esp_hal::time::Rate::from_mhz(1))
            .with_mode(esp_hal::spi::Mode::_0),
    )
    .unwrap()
    .with_sck(sck)
    .with_mosi(mosi)
    .with_miso(miso);

    let mut sd_cs = Output::new(peripherals.GPIO33, Level::High, OutputConfig::default());

    sd_cs.set_high();

    for _ in 0..100 {
        let _ = spi.write(&[0xFF]);
    }

    let mut delay = esp_hal::delay::Delay::new();
    let spi_device = ExclusiveDevice::new(spi, sd_cs, delay).unwrap();
    let sdcard = SdCard::new(spi_device, delay);

    match sdcard.num_bytes() {
        Ok(size) => {
            let size_gb = size as f64 / 1024.0 / 1024.0 / 1024.0;
            esp_println::println!("Card size: {} bytes ({:.2} GB)", size, size_gb);
        }
        Err(e) => esp_println::println!("Error getting size: {:?}", e),
    }
    esp_hal::delay::Delay::new().delay_ms(1000u32);

    let mut volume_mgr = VolumeManager::new(sdcard, DummyTimesource::default());

    match volume_mgr.open_volume(embedded_sdmmc::VolumeIdx(0)) {
        Ok(mut volume) => {
            let mut root_dir = volume.open_root_dir().expect("Failed to open root dir");

            esp_println::println!("Listing Directory Contents:");

            root_dir.iterate_dir(|entry| {
                esp_println::println!(
                    "  {} | Name: {} | Size: {} bytes",
                    if entry.attributes.is_directory() { "[DIR] " } else { "[FILE]" },
                    entry.name,
                    entry.size
                );
            }).expect("Failed to iterate directory");
            match root_dir.open_file_in_dir("COOL_F~1.TXT", Mode::ReadOnly) {
                Ok(mut file) => {
                    let mut buffer = [0u8; 64]; 

                    let bytes_read = file.read(&mut buffer).expect("Error reading file");

                    if let Ok(content) = core::str::from_utf8(&buffer[..bytes_read]) {
                        esp_println::println!("--- FILE CONTENT START ---");
                        esp_println::println!("{}", content);
                        esp_println::println!("--- FILE CONTENT END ---");
                        esp_println::println!("Read {} bytes successfully.", bytes_read);
                    } else {
                        esp_println::println!("File contains invalid UTF-8 data.");
                    }
                }
                Err(e) => {
                    esp_println::println!("Could not open file: {:?}", e);
                    esp_println::println!("Check if the name is EXACTLY 'COOL_F~1.TXT'");
                }
            }
        }
        Err(e) => {
            esp_println::println!("Could not open volume: {:?}", e);
            esp_println::println!("HINT: If you see SignatureNotFound, it's because of the 1.6TB sync error.");
        }
    }

    loop{}
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
