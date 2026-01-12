#![no_std]
#![no_main]

mod sta_config;
mod ap_router;
mod sta_router;
mod dhcp;

extern crate alloc;
use esp_backtrace as _;
use esp_println::println;
use esp_rtos::main;
use embassy_time::{Duration};
use embassy_executor::Spawner;
use static_cell::StaticCell;
use esp_radio::wifi;
use alloc::string::String;
use embassy_net::{Stack, StackResources, Ipv4Address, Ipv4Cidr, StaticConfigV4};
use embassy_sync::mutex::Mutex;
use embassy_futures::select::{select, Either};
use embassy_sync::channel::Channel;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::signal::Signal;
use sta_config::WifiSsidPwd;
use crate::sta_config::{StaConfigManager, CONFIG_MANAGER};

#[derive(Debug, Clone, serde::Serialize)]
pub struct WifiStatus {
    pub is_connected: bool,
    pub is_config_up: bool,
    pub ip_address: Option<embassy_net::Ipv4Address>,
    pub gateway: Option<embassy_net::Ipv4Address>,
    pub mac_address: [u8; 6],
}

pub enum WifiCommand<'a> {
    Connect,
    Disconnect,
    SetConfig(WifiSsidPwd),
    GetStatus(&'a Signal<CriticalSectionRawMutex, WifiStatus>),
}

pub static WIFI_CMD_CHAN: Channel<CriticalSectionRawMutex, WifiCommand, 2> = Channel::new();

esp_bootloader_esp_idf::esp_app_desc!();

#[main]
async fn main(spawner: Spawner) {
    esp_println::logger::init_logger_from_env();
    esp_alloc::heap_allocator!(size: 64 * 1024);

    let config = esp_hal::Config::default();
    let peripherals = esp_hal::init(config);
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
        .with_ssid(String::from("arctan2-ap"))
        .with_password(String::from("123498765"));

    let sta_config = wifi::ClientConfig::default();

    static AP_RESOURCES: StaticCell<StackResources<3>> = StaticCell::new();
    static STA_RESOURCES: StaticCell<StackResources<3>> = StaticCell::new();
    static STA_STACK: StaticCell<Stack<'static>> = StaticCell::new();

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

    let (sta_stack_init, sta_runner) = embassy_net::new(
        ifaces.sta,
        sta_net_config,
        STA_RESOURCES.init(StackResources::new()),
        5678,
    );
    let sta_stack = STA_STACK.init(sta_stack_init);

    wifi_controller.set_config(&wifi::ModeConfig::ApSta(sta_config, ap_config.clone())).unwrap();
    wifi_controller.start_async().await.unwrap();
    println!("wifi started...");

    spawner.spawn(net_runner_task(ap_runner)).unwrap();
    println!("ap_runner spawned...");
    spawner.spawn(net_runner_task(sta_runner)).unwrap();
    println!("sta_runner spawned...");

    spawner.spawn(wifi_management_task(wifi_controller, ap_config, sta_stack)).unwrap();
    println!("wifi logger started...");

    spawner.spawn(dhcp::dhcp_server_task(ap_stack)).unwrap();
    println!("dhcp_server_task spawned...");
    spawner.spawn(ap_router::ap_http_server_task(ap_stack, config_bus)).unwrap();
    println!("ap_http_server_task spawned...");

    spawner.spawn(sta_http_server_task(*sta_stack)).unwrap();
    println!("ap_http_server_task spawned...");

    println!("Everything init successfully...");

    {
        let mut manager = config_bus.lock().await;
        if let Some(details) = manager.load() {
            println!("wifi_details on flash = {} {}", &details.ssid, &details.pwd);
            WIFI_CMD_CHAN.send(WifiCommand::SetConfig(details)).await;
            WIFI_CMD_CHAN.send(WifiCommand::Connect).await;
        } else {
            println!("not wifi_details found!");
        }
    }

    loop {
        embassy_time::Timer::after_secs(1).await;
    }
}

#[embassy_executor::task(pool_size = 4)]
async fn sta_http_server_task(stack: Stack<'static>) {
    let mut rx_buffer = [0; 1024];
    let mut tx_buffer = [0; 1024];
    let mut http_buffer = [0; 2048];

    let app = sta_router::sta_router();
    let config = picoserve::Config::new(picoserve::Timeouts {
        start_read_request: Some(Duration::from_secs(5)),
        persistent_start_read_request: Some(Duration::from_secs(1)),
        read_request: Some(Duration::from_secs(1)),
        write: Some(Duration::from_secs(1)),
    });

    println!("HTTP Server listening on port 80...");

    loop {
        let mut socket = embassy_net::tcp::TcpSocket::new(stack, &mut rx_buffer, &mut tx_buffer);
        socket.set_timeout(Some(embassy_time::Duration::from_secs(10)));

        if let Err(e) = socket.accept(80).await {
            println!("Accept error: {:?}", e);
            continue;
        }

        match picoserve::Server::new(&app, &config, &mut http_buffer).serve(socket).await {
            Ok(_) => {}
            Err(e) => println!("Serve error: {:?}", e),
        }
    }
}

#[embassy_executor::task(pool_size = 4)]
async fn net_runner_task(mut runner: embassy_net::Runner<'static, wifi::WifiDevice<'static>>) {
    runner.run().await;
}

#[embassy_executor::task]
async fn wifi_management_task(
    mut controller: wifi::WifiController<'static>,
    ap_config: wifi::AccessPointConfig,
    stack: &'static embassy_net::Stack<'static>,
) {
    loop {
        match select(
            controller.wait_for_events(enumset::EnumSet::all(), true),
            WIFI_CMD_CHAN.receive(),
        ).await {
            Either::First(events) => {
                for event in events {
                    println!("WiFi Event: {:?}", event);
                }
            }
            
            Either::Second(command) => {
                match command {
                    WifiCommand::SetConfig(creds) => {
                        println!("Updating config for SSID: {}", creds.ssid);
                        let sta_config = wifi::ClientConfig::default()
                            .with_ssid(creds.ssid)
                            .with_password(creds.pwd);
                        controller.set_config(&wifi::ModeConfig::ApSta(sta_config, ap_config.clone())).unwrap();
                    }
                    WifiCommand::Connect => {
                        println!("Connecting to router...");
                        match controller.connect_async().await {
                            Err(e) => {
                                println!("error while connecting to station: {}", e);
                            }
                            _ => {}
                        };
                    }
                    WifiCommand::Disconnect => {
                        println!("Disconnecting from router...");
                        match controller.disconnect_async().await {
                            Err(e) => {
                                println!("error while connecting to station: {}", e);
                            }
                            _ => {}
                        };
                    }
                    WifiCommand::GetStatus(sig) => {
                        let config = stack.config_v4();

                        let status = WifiStatus {
                            is_connected: controller.is_connected().unwrap_or(false),
                            is_config_up: stack.is_config_up(),
                            ip_address: config.clone().map(|c| c.address.address()),
                            gateway: config.and_then(|c| c.gateway),
                            mac_address: stack.hardware_address().as_bytes().try_into().unwrap_or([0; 6]),
                        };

                        sig.signal(status);
                    }
                }
            }
        }
    }
}
