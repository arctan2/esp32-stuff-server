use esp_println::println;
use static_cell::StaticCell;
use embassy_net::Stack;
use embassy_time::Duration;
use embassy_net::tcp::TcpSocket;
use crate::router;
use allocator_api2::boxed::Box;

#[embassy_executor::task(pool_size = 2)]
pub async fn http_server_task(
    stack: Stack<'static>,
    static_resources: &'static StaticCell<([u8; 1024], [u8; 1024], [u8; 2048])>
) {
    let (rx_buf, tx_buf, http_buf) = static_resources.init(([0; 1024], [0; 1024], [0; 2048]));
    let app = Box::new_in(router::router(), esp_alloc::ExternalMemory);
    let config = picoserve::Config::new(picoserve::Timeouts {
        start_read_request: Some(Duration::from_secs(5)),
        persistent_start_read_request: None,
        read_request: Some(Duration::from_secs(1)),
        write: Some(Duration::from_secs(1)),
    });

    println!("HTTP Server listening on port 80...");

    loop {
        let mut socket = TcpSocket::new(stack, rx_buf, tx_buf);
        socket.set_timeout(Some(Duration::from_secs(5)));

        if let Ok(_) = embassy_time::with_timeout(Duration::from_secs(20), socket.accept(80)).await {
            let _ = embassy_time::with_timeout(
                Duration::from_secs(10), 
                picoserve::Server::new(&app, &config, http_buf).serve(socket)
            ).await;
        } else {
            // important to free the stuff of wifi. Else the wifi gets clogged up. It's fixing the
            // wifi disappearing problem
            socket.abort();
        }
    }
}

