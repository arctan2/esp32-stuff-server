use esp_println::println;
use crate::types::{WifiSsidPwd, WifiStatus, String};
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::signal::Signal;
use embassy_sync::channel::Channel;
use esp_radio::wifi;
use embassy_futures::select::{select, Either};

pub enum Event<'a> {
    Connect,
    Disconnect,
    SoftwareReset,
    GetFlashData(&'a Signal<CriticalSectionRawMutex, WifiSsidPwd>),
    WriteConfigToFlash(WifiSsidPwd),
    SetConfig(WifiSsidPwd),
    GetStatus(&'a Signal<CriticalSectionRawMutex, WifiStatus>),
}

pub static EVENT_CHAN: Channel<CriticalSectionRawMutex, Event, 2> = Channel::new();

pub fn get_wifi_status<'a>(
    controller: &wifi::WifiController<'static>,
    stack: &'a embassy_net::Stack<'static>,
) -> WifiStatus {
    let config = stack.config_v4();
    return WifiStatus {
        is_connected: controller.is_connected().unwrap_or(false),
        is_config_up: stack.is_config_up(),
        ip_address: config.clone().map(|c| c.address.address()),
        gateway: config.and_then(|c| c.gateway),
        mac_address: stack.hardware_address().as_bytes().try_into().unwrap_or([0; 6]),
    };
}

#[embassy_executor::task(pool_size = 1)]
pub async fn event_handler_task(
    mut controller: wifi::WifiController<'static>,
    ap_config: wifi::AccessPointConfig,
    sta_config: crate::sta_config::GlobalStaConfigManager,
    stack: embassy_net::Stack<'static>,
) {
    loop {
        match select(
            controller.wait_for_events(enumset::EnumSet::all(), true),
            EVENT_CHAN.receive(),
        ).await {
            Either::First(events) => {
                for event in events {
                    println!("WiFi Event: {:?}", event);
                }
            }
            
            Either::Second(command) => {
                match command {
                    Event::SetConfig(ssid_pwd) => {
                        println!("Updating config for SSID: {}", ssid_pwd.ssid);
                        let sta_config = wifi::ClientConfig::default()
                            .with_ssid(ssid_pwd.ssid)
                            .with_password(ssid_pwd.pwd);
                        match controller.set_config(&wifi::ModeConfig::ApSta(sta_config, ap_config.clone())) {
                            Err(e) => println!("error while setting config: {:?}", e),
                            Ok(_) => println!("config set successfully"),
                        };
                    }
                    Event::WriteConfigToFlash(ssid_pwd) => {
                        let mut manager = sta_config.lock().await;
                        manager.save(&ssid_pwd);
                    }
                    Event::GetFlashData(sig) => {
                        let mut manager = sta_config.lock().await;
                        if let Some(details) = manager.load() {
                            sig.signal(details);
                        } else {
                            sig.signal(WifiSsidPwd{ ssid: String::from(""), pwd: String::from("") });
                        }
                    }
                    Event::SoftwareReset => {
                        println!("Command: SoftwareReset");
                        esp_hal::system::software_reset();
                    }
                    Event::Connect => {
                        println!("Connecting to router...");
                        match controller.connect_async().await {
                            Err(e) => println!("error while connecting to router: {}", e),
                            Ok(_) => {
                                println!("connected to router. Getting status...");
                                embassy_time::Timer::after_secs(2).await;
                                println!("wifi status: {:?}", get_wifi_status(&controller, &stack));
                            },
                        };
                    }
                    Event::Disconnect => {
                        println!("Disconnecting from router...");
                        match controller.disconnect_async().await {
                            Err(e) => println!("error while disconnecting from router: {}", e),
                            Ok(_) => println!("disconnected from router"),
                        };
                    }
                    Event::GetStatus(sig) => {
                        sig.signal(get_wifi_status(&controller, &stack));
                    }
                }
            }
        }
    }
}
