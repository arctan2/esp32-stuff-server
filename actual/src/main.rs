#![no_std]
#![no_main]

mod types;
mod sta_config;
mod http_task;
mod event_handler;
mod router;
mod dhcp;

use esp_backtrace as _;
use types::{String};
use allocator_api2::vec::Vec;
use esp_println::{println};
use esp_rtos::main;
use embassy_executor::Spawner;
use static_cell::StaticCell;
use esp_radio::wifi;
use embassy_net::{StackResources, Ipv4Address, Ipv4Cidr, StaticConfigV4};
use embassy_sync::mutex::Mutex;
use sta_config::{StaConfigManager, CONFIG_MANAGER};
use event_handler::{Event, EVENT_CHAN};
use embassy_net::icmp::ping::{PingManager, PingParams};
use embassy_net::icmp::PacketMetadata;

esp_bootloader_esp_idf::esp_app_desc!();

#[main]
async fn main(spawner: Spawner) {
    esp_println::logger::init_logger_from_env();

    let config = esp_hal::Config::default().with_psram(esp_hal::psram::PsramConfig::default());
    let peripherals = esp_hal::init(config);

    esp_alloc::heap_allocator!(size: 64 * 1024);
    esp_alloc::psram_allocator!(peripherals.PSRAM, esp_hal::psram);

    let timg0 = esp_hal::timer::timg::TimerGroup::new(peripherals.TIMG0);
    esp_rtos::start(timg0.timer0);

    let config_mgr = StaConfigManager::new(peripherals.FLASH);
    let config_bus = CONFIG_MANAGER.init(Mutex::new(config_mgr));

    static RADIO_CTRL: StaticCell<esp_radio::Controller> = StaticCell::new();
    let esp_radio_controller = RADIO_CTRL.init(esp_radio::init().unwrap());

    let (mut wifi_controller, ifaces) = wifi::new(
        esp_radio_controller,
        peripherals.WIFI,
        wifi::Config::default()
    ).unwrap();

    let ap_config = wifi::AccessPointConfig::default()
        .with_auth_method(wifi::AuthMethod::Wpa2Personal)
        .with_channel(6)
        .with_ssid(String::from("arctan2-ap"))
        .with_password(String::from("123498765"));

    let mut sta_config = wifi::ClientConfig::default();
     
    {
        let mut manager = config_bus.lock().await;
        if let Some(data) = manager.load() {
            println!("wifi_details on flash = {} {}", &data.ssid, &data.pwd);
            sta_config = sta_config.with_ssid(data.ssid).with_password(data.pwd);
            // EVENT_CHAN.send(Event::SetConfig(data)).await;
            EVENT_CHAN.send(Event::Connect).await;
        } else {
            println!("no wifi_details found!");
        }
    }

    static AP_RESOURCES: StaticCell<StackResources<5>> = StaticCell::new();
    static STA_RESOURCES: StaticCell<StackResources<5>> = StaticCell::new();

    let ap_net_config = embassy_net::Config::ipv4_static(StaticConfigV4 {
        address: Ipv4Cidr::new(Ipv4Address::new(10, 0, 1, 1), 24),
        gateway: Some(Ipv4Address::new(10, 0, 1, 1)),
        dns_servers: heapless::Vec::new(),
    });

    let sta_net_config = embassy_net::Config::dhcpv4(Default::default());

    let (ap_stack, ap_runner) = embassy_net::new(
        ifaces.ap,
        ap_net_config,
        AP_RESOURCES.init(StackResources::new()),
        1234,
    );

    let (sta_stack, sta_runner) = embassy_net::new(
        ifaces.sta,
        sta_net_config,
        STA_RESOURCES.init(StackResources::new()),
        5678,
    );

    if let Err(e) = wifi_controller.set_power_saving(wifi::PowerSaveMode::None) {
        println!("error while setting power saving: {}", e);
    }
    wifi_controller.set_config(&wifi::ModeConfig::ApSta(sta_config, ap_config.clone())).unwrap();
    wifi_controller.start_async().await.unwrap();
    println!("wifi started...");

    spawner.spawn(net_runner_task(ap_runner)).unwrap();
    println!("ap_runner spawned...");
    spawner.spawn(net_runner_task(sta_runner)).unwrap();
    println!("sta_runner spawned...");

    spawner.spawn(event_handler::event_handler_task(wifi_controller, ap_config, config_bus, sta_stack)).unwrap();
    println!("wifi logger started...");

    spawner.spawn(dhcp::dhcp_server_task(ap_stack)).unwrap();
    println!("dhcp_server_task spawned...");

    static AP_SOCKET_RESOURCES: StaticCell<([u8; 1024], [u8; 1024], [u8; 2048])> = StaticCell::new();
    static STA_SOCKET_RESOURCES: StaticCell<([u8; 1024], [u8; 1024], [u8; 2048])> = StaticCell::new();

    spawner.spawn(http_task::http_server_task(ap_stack, &AP_SOCKET_RESOURCES)).unwrap();
    println!("ap_http_server_task spawned...");

    spawner.spawn(http_task::http_server_task(sta_stack, &STA_SOCKET_RESOURCES)).unwrap();
    println!("sta_http_server_task spawned...");

    println!("Everything init successfully...");

    let mut rx_buf = [0; 64];
    let mut tx_buf = [0; 64];
    let mut rx_meta = [PacketMetadata::EMPTY];
    let mut tx_meta = [PacketMetadata::EMPTY];
    let mut ping_manager = PingManager::new(sta_stack, &mut rx_meta, &mut rx_buf, &mut tx_meta, &mut tx_buf);
    let mut tick = 0;

    loop {
        println!("tick {tick}");
        if let Some(config) = sta_stack.config_v4() {
            match config.gateway {
                Some(gateway) => {
                    let mut ping_params = PingParams::new(gateway);
                    ping_params.set_payload(b"Hello, router!");
                    match ping_manager.ping(&ping_params).await {
                        Ok(time) => println!("Ping time of {}: {}ms", gateway, time.as_millis()),
                        Err(ping_error) => println!("PingError: {:?}", ping_error),
                    };
                },
                None => println!("Gateway not found.")
            }
        } else {
            println!("STA Link is down or waiting for IP...");
        }
        embassy_time::Timer::after_secs(10).await;
        tick += 1;
    }
}


#[embassy_executor::task(pool_size = 2)]
async fn net_runner_task(mut runner: embassy_net::Runner<'static, wifi::WifiDevice<'static>>) {
    runner.run().await;
}

