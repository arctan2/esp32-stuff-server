#![no_std]
#![no_main]

use esp_backtrace as _;
use esp_println::println;
use esp_hal::gpio::{Level, Output, OutputConfig};
use esp_rtos::main;
use embassy_time::{Duration, Timer};
use embassy_executor::Spawner;
use static_cell::StaticCell;

use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::mutex::Mutex;

esp_bootloader_esp_idf::esp_app_desc!();

type SharedPin = Mutex<CriticalSectionRawMutex, Output<'static>>;
static SHARED_GPIO: StaticCell<SharedPin> = StaticCell::new();

#[main]
async fn main(spawner: Spawner) {
    let config = esp_hal::Config::default();
    let peripherals = esp_hal::init(config);
    let timg0 = esp_hal::timer::timg::TimerGroup::new(peripherals.TIMG0);
    esp_rtos::start(timg0.timer0);

    println!("RTOS Initialized. Spawning blinky...");

    let led = Output::new(peripherals.GPIO13, Level::Low, OutputConfig::default());

    let mutex = SHARED_GPIO.init(Mutex::new(led));

    spawner.spawn(blinky(mutex)).ok();

    loop {
        println!("Main task is alive...");
        Timer::after(Duration::from_millis(2000)).await;
        {
            let mut pin = mutex.lock().await;
            pin.set_low();
        }
    }
}

#[embassy_executor::task]
async fn blinky(mutex: &'static SharedPin) {
    let mut i = 0_i32;
    loop {
        {
            let mut pin = mutex.lock().await;
            pin.set_high();
        }
        Timer::after(Duration::from_millis(1500)).await;
        println!("blinky {i}");
        i += 1;
    }
}
