#![allow(nonstandard_style)]
use alpa::embedded_sdmmc_ram_device::{allocators};
use picoserve::time::Duration;
use picoserve::routing::{post, get, delete, parse_path_segment, Router, PathRouter};
use picoserve::response::{Response, IntoResponse};
use file_manager::{init_file_manager, DummyTimesource};
use server::{CatchAll, HOME_PAGE};
use file_manager::{BlkDev, init_file_system, ExtAlloc};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    allocators::init_simulated_hardware();
    let sdcard = BlkDev::new("test_file.db").unwrap();
    init_file_manager(sdcard, DummyTimesource);

    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 8000)).await.unwrap();

    let app = std::rc::Rc::new(router());

    let config = picoserve::Config::new(picoserve::Timeouts {
        start_read_request: Some(Duration::from_secs(5)),
        persistent_start_read_request: None,
        read_request: Some(Duration::from_secs(1)),
        write: Some(Duration::from_secs(1)),
    });

    tokio::task::LocalSet::new()
        .run_until(async {
            loop {
                match init_file_system(ExtAlloc::default()).await {
                    Ok(()) => break,
                    Err(e) => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                        println!("error: {:?}", e);
                    }
                }
            }

            loop {
                let (stream, remote_address) = listener.accept().await.unwrap();
                
                let config = config.clone();
                let app = app.clone();

                tokio::task::spawn_local(async move {
                    let mut buffer = [0u8; 2048]; 

                    match picoserve::Server::new_tokio(&app, &config, &mut buffer).serve(stream).await {
                        Ok(info) => println!("Handled {} requests from {}", info.handled_requests_count, remote_address),
                        Err(err) => println!("Error handling connection: {:?}", err),
                    }
                });
            }
        })
    .await
}

async fn home() -> impl IntoResponse {
    Response::ok(HOME_PAGE)
        .with_header("Content-Type", "text/html")
}

fn files_routes() -> Router<impl PathRouter> {
    Router::new()
        .route("/list", get(server::handle_files))
        .route(("/delete", parse_path_segment::<String>()), delete(server::handle_delete_file))
}

fn upload_routes() -> Router<impl PathRouter> {
    Router::new()
        .route("/file", post(server::handle_file_upload))
        .route("/music", post(server::handle_music_upload))
}

pub fn router() -> Router<impl PathRouter> {
    Router::new()
        .route("/", get(home))
        .nest("/files", files_routes())
        .nest("/upload", upload_routes())
        .route("/db", delete(server::handle_delete_db))
        .route(("/download", CatchAll), get(server::handle_download))
        .route(("/fs", CatchAll), get(server::handle_fs))
}

