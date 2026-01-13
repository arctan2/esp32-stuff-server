use picoserve::routing::{post, get};
use picoserve::response::{IntoResponse, Response};
use picoserve::extract::{Json};
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::signal::Signal;
use crate::event_handler::{Event, EVENT_CHAN};
use crate::types::{WifiSsidPwd, WifiStatus};

static CONFIG_PAGE: &str = include_str!("./html/config.html");
static HOME_PAGE: &str = include_str!("./html/home.html");
static STATUS_SIGNAL: Signal<CriticalSectionRawMutex, WifiStatus> = Signal::new();
static FLASH_DATA_SIGNAL: Signal<CriticalSectionRawMutex, WifiSsidPwd> = Signal::new();

async fn set_config(Json(data): Json<WifiSsidPwd>) -> impl IntoResponse {
    EVENT_CHAN.send(Event::SetConfig(data)).await;
    EVENT_CHAN.send(Event::Connect).await;
    "set_config: config set"
}

async fn write_to_flash(Json(data): Json<WifiSsidPwd>) -> impl IntoResponse {
    EVENT_CHAN.send(Event::WriteConfigToFlash(data)).await;
    "write_to_flash: wrote"
}

async fn connect() -> impl IntoResponse {
    EVENT_CHAN.send(Event::Connect).await;
    "connect: connected"
}

async fn disconnect() -> impl IntoResponse {
    EVENT_CHAN.send(Event::Disconnect).await;
    "disconnect: disconnected"
}

async fn software_reset() -> impl IntoResponse {
    EVENT_CHAN.send(Event::SoftwareReset).await;
    "software_reset: reset done"
}

async fn status() -> impl IntoResponse {
    STATUS_SIGNAL.reset();
    EVENT_CHAN.send(Event::GetStatus(&STATUS_SIGNAL)).await;
    let status = STATUS_SIGNAL.wait().await;
    picoserve::response::json::Json(status)
}

async fn get_flash_data() -> impl IntoResponse {
    FLASH_DATA_SIGNAL.reset();
    EVENT_CHAN.send(Event::GetFlashData(&FLASH_DATA_SIGNAL)).await;
    let data = FLASH_DATA_SIGNAL.wait().await;
    picoserve::response::json::Json(data)
}

pub fn router() -> picoserve::Router<impl picoserve::routing::PathRouter> {
    picoserve::Router::new()
        .route("/", get(|| async {
            Response::ok(HOME_PAGE).with_header("Content-Type", "text/html")
        }))
        .route("/config", get(|| async {
            Response::ok(CONFIG_PAGE).with_header("Content-Type", "text/html")
        }))
        .route("/set-config", post(set_config))
        .route("/write-to-flash", post(write_to_flash))
        .route("/connect", get(connect))
        .route("/disconnect", get(disconnect))
        .route("/status", get(status))
        .route("/software-reset", get(software_reset))
        .route("/get-flash-data", get(get_flash_data))
}

