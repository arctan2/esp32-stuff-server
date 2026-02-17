#![no_std]
#![no_main]
#![feature(alloc_error_handler)]

extern crate alloc;
mod types;

use esp_backtrace as _;
use esp_alloc as _;
use embedded_sdmmc::{SdCard, TimeSource, Timestamp};
use embedded_hal::delay::DelayNs;
use embedded_hal::spi::SpiBus;
use embedded_sdmmc::{VolumeManager};
use esp_rtos::main;

use types::{String};
use esp_println::{println};
use embassy_executor::Spawner;
use file_manager::{ExtAlloc, init_file_system};
use esp_hal::{
    gpio::{Level, Output, OutputConfig},
    time::{Rate},
    spi::{master::{Spi, Config}, Mode},
    delay::{Delay},
};
use embedded_hal_bus::spi::ExclusiveDevice;

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
async fn main(spawner: Spawner) {
    esp_println::logger::init_logger_from_env();

    let config = esp_hal::Config::default().with_cpu_clock(esp_hal::clock::CpuClock::max());
    let peripherals = esp_hal::init(config);

    esp_alloc::heap_allocator!(size: 64 * 1024);
    esp_alloc::psram_allocator!(peripherals.PSRAM, esp_hal::psram);

    let timg0 = esp_hal::timer::timg::TimerGroup::new(peripherals.TIMG0);
    esp_rtos::start(timg0.timer0);

    disable_sim800L_modem(peripherals.GPIO4, peripherals.GPIO23);

    // let sck  = peripherals.GPIO13; 
    // let mosi = peripherals.GPIO14;
    // let miso = peripherals.GPIO32;

    // let mut spi = esp_hal::spi::master::Spi::new(
    //     peripherals.SPI2,
    //     esp_hal::spi::master::Config::default()
    //         .with_frequency(esp_hal::time::Rate::from_mhz(1))
    //         .with_mode(esp_hal::spi::Mode::_0),
    // )
    // .unwrap()
    // .with_sck(sck)
    // .with_mosi(mosi)
    // .with_miso(miso);

    // let mut sd_cs = Output::new(peripherals.GPIO33, Level::High, OutputConfig::default());

    // sd_cs.set_high();

    // for _ in 0..100 {
    //     let _ = spi.write(&[0xFF]);
    // }

    // let mut delay = esp_hal::delay::Delay::new();
    // let spi_device = ExclusiveDevice::new(spi, sd_cs, delay).unwrap();
    // let sdcard = SdCard::new(spi_device, delay);

    // loop {
    //     match sdcard.num_bytes() {
    //         Ok(size) => {
    //             let size_gb = size as f64 / 1024.0 / 1024.0 / 1024.0;
    //             esp_println::println!("Card size: {} bytes ({:.2} GB)", size, size_gb);
    //             break;
    //         }
    //         Err(e) => esp_println::println!("Error getting size: {:?}", e),
    //     }
    //     esp_hal::delay::Delay::new().delay_ms(1000u32);
    // }

    // let mut volume_mgr = VolumeManager::new(sdcard, DummyTimesource::default());

    {
        let sck  = peripherals.GPIO13;
        let mosi = peripherals.GPIO14;
        let miso = peripherals.GPIO32;

        loop {
            let mut spi = Spi::new(
                unsafe { peripherals.SPI2.clone_unchecked() },
                Config::default()
                    .with_frequency(Rate::from_mhz(1))
                    .with_mode(Mode::_0),
            )
            .unwrap()
            .with_sck(unsafe { sck.clone_unchecked() })
            .with_mosi(unsafe { mosi.clone_unchecked() })
            .with_miso(unsafe { miso.clone_unchecked() });

            let mut sd_cs = Output::new(unsafe { peripherals.GPIO33.clone_unchecked() }, Level::High, OutputConfig::default());

            sd_cs.set_high();

            let delay = Delay::new();
            let spi_device = ExclusiveDevice::new(spi, sd_cs, delay).unwrap();

            match init_file_system(spi_device, delay, ExtAlloc::default()).await {
                Ok(()) => break,
                Err(e) => {
                    embassy_time::Timer::after_secs(1).await;
                    println!("error: {:?}", e);
                }
            }
        }
    }

    let mut tick = 0;
    
    loop{
        println!("tick {tick}");
        embassy_time::Timer::after_secs(5).await;
        tick += 1;
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
