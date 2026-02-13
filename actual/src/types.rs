#![allow(unused)]

extern crate alloc;
use serde::{Deserialize, Serialize};
pub use alloc::string::String;
pub use alloc::vec::Vec;

#[derive(Deserialize, Serialize, Clone)]
pub struct WifiSsidPwd {
    pub ssid: String,
    pub pwd: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct WifiStatus {
    pub is_connected: bool,
    pub is_config_up: bool,
    pub ip_address: Option<embassy_net::Ipv4Address>,
    pub gateway: Option<embassy_net::Ipv4Address>,
    pub mac_address: [u8; 6],
}

