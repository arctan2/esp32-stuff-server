use esp_println::println;
use embassy_net::{Stack};
use embassy_time::{Duration};
use picoserve::routing::get;
use picoserve::response::{IntoResponse, Response};
use picoserve::extract::{Form};
use crate::sta_config::{WifiSsidPwd, GlobalStaConfigManager};
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::signal::Signal;

static AP_WEBPAGE: &str = include_str!("./ap_client/index.html");
static STATUS_SIGNAL: Signal<CriticalSectionRawMutex, crate::WifiStatus> = Signal::new();

#[embassy_executor::task(pool_size = 4)]
pub async fn ap_http_server_task(stack: Stack<'static>, sta_config: GlobalStaConfigManager) {
    let mut rx_buffer = [0; 1024];
    let mut tx_buffer = [0; 1024];
    let mut http_buffer = [0; 2048];

    let app = ap_router(sta_config);
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

async fn handle_station_config(form: Form<WifiSsidPwd>, sta_config: GlobalStaConfigManager) -> impl IntoResponse {
    let mut manager = sta_config.lock().await;
    manager.save(&form.0);
    crate::WIFI_CMD_CHAN.send(crate::WifiCommand::SetConfig(form.0)).await;
    crate::WIFI_CMD_CHAN.send(crate::WifiCommand::Connect).await;
    Response::ok("success")
}

async fn handle_status() -> impl IntoResponse {
    STATUS_SIGNAL.reset();
    crate::WIFI_CMD_CHAN.send(crate::WifiCommand::GetStatus(&STATUS_SIGNAL)).await;
    let status = STATUS_SIGNAL.wait().await;
    picoserve::response::json::Json(status)
}

fn ap_router(sta_config: GlobalStaConfigManager) -> picoserve::Router<impl picoserve::routing::PathRouter> {
    picoserve::Router::new()
        .route("/config", get(|| async {
            Response::ok(AP_WEBPAGE).with_header("Content-Type", "text/html")
        }))
        .route("/status", get(handle_status))
        .route("/update-config", picoserve::routing::post(move |form: picoserve::extract::Form<WifiSsidPwd>| async move {
            handle_station_config(form, sta_config).await
        }))
}
