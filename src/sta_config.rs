extern crate alloc;
use esp_hal::peripherals::FLASH;
use static_cell::StaticCell;
use alloc::string::String;
use serde::{Serialize, Deserialize};
use esp_storage::FlashStorage;
use embedded_storage::nor_flash::NorFlash;
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embedded_storage::nor_flash::ReadNorFlash;
use embassy_sync::mutex::Mutex;

#[derive(Deserialize, Serialize)]
pub struct WifiSsidPwd {
    pub ssid: String,
    pub pwd: String,
}

pub struct StaConfigManager<'d> {
    flash: FlashStorage<'d>,
}

pub const FLASH_ADDR: u32 = 0x300000;
impl<'d> StaConfigManager<'d> {
    pub fn new(flash: FLASH<'d>) -> Self {
        Self { flash: FlashStorage::new(flash) }
    }

    pub fn save(&mut self, config: &WifiSsidPwd) {
        let mut buf = [0u8; 256];
        if let Ok(_serialized) = postcard::to_slice(config, &mut buf) {
            self.flash.erase(FLASH_ADDR, FLASH_ADDR + 4096).unwrap();
            self.flash.write(FLASH_ADDR, &buf).unwrap(); 
        }
    }

    pub fn load(&mut self) -> Option<WifiSsidPwd> {
        let mut buf = [0u8; 256];
        self.flash.read(FLASH_ADDR, &mut buf).unwrap();
        postcard::from_bytes::<WifiSsidPwd>(&buf).ok()
    }
}

pub static CONFIG_MANAGER: StaticCell<Mutex<NoopRawMutex, StaConfigManager<'static>>> = StaticCell::new();
pub type GlobalStaConfigManager = &'static Mutex<NoopRawMutex, StaConfigManager<'static>>;
