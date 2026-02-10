#![allow(nonstandard_style)]
use alpa::embedded_sdmmc_ram_device::{allocators, block_device, esp_alloc, timesource};
use alpa::embedded_sdmmc_ram_device::fs::{DbDirSdmmc};
use alpa::{Column, ColumnType, Value, Row, Query, QueryExecutor};
use crate::block_device::{FsBlockDevice};
use crate::timesource::{DummyTimesource};
use embedded_sdmmc::{BlockDevice, TimeSource, Error};
use picoserve::time::Duration;
use picoserve::routing::{post, get, parse_path_segment, PathDescription};
use picoserve::response::{IntoResponse, Response};
use picoserve::request::Path;
use file_manager::{FileManager, Signal, Channel, FileType, Event};
use std::sync::OnceLock;

static HOME_PAGE: &str = include_str!("./html/home.html");

type BlkDev = block_device::FsBlockDevice;
type TimeSrc = timesource::DummyTimesource;
type FMan<'a> = FileManager<'a, BlkDev, TimeSrc, 4, 4, 1>;

#[derive(Debug)]
pub struct SyncFMan<'a>(pub FMan<'a>);

unsafe impl <'a> Send for SyncFMan<'a> {}
unsafe impl <'a> Sync for SyncFMan<'a> {}

static FILE_MAN: OnceLock<SyncFMan> = OnceLock::new();

fn init_file_manager(block_device: FsBlockDevice, time_src: DummyTimesource) {
    FILE_MAN.set(
        SyncFMan(FileManager::new(block_device, time_src))
    ).expect("initing twice file_manager");
}

fn get_file_manager() -> &'static FMan<'static> {
    &FILE_MAN.get().expect("file_manager not initialized").0
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    allocators::init_simulated_hardware();
    let sdcard = block_device::FsBlockDevice::new("test_file.db").unwrap();
    init_file_manager(sdcard, timesource::DummyTimesource);

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
            tokio::task::spawn_local(async move {
                let fman = get_file_manager();
                fman.run().await;
            });

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


#[derive(Copy, Clone, Debug)]
struct CatchAll;

impl<T: Copy + core::fmt::Debug> PathDescription<T> for CatchAll {
    type NewPathParameters = String;

    fn parse_and_validate<'r, U, F: FnOnce(Self::NewPathParameters, Path<'r>) -> Result<U, Self::NewPathParameters>>(
        &self,
        current_path_parameters: T,
        path: Path<'r>,
        validate: F,
    ) -> Result<U, T> {
        let remaining = String::from(path.encoded());
        
        let mut empty = path;
        while let Some(p) = empty.split_first_segment() {
            empty = p.1;
        }
        
        validate(remaining, empty).map_err(|_| current_path_parameters)
    }
}

fn router() -> picoserve::Router<impl picoserve::routing::PathRouter> {
    picoserve::Router::new()
        .route("/", get(|| async {
            Response::ok(HOME_PAGE).with_header("Content-Type", "text/html")
        }))
        .route(("/fs", CatchAll), get(|path: String| async move {
            let fman = get_file_manager();
            fman.open_path_sig.reset();
            fman.event_chan.send(Event::OpenPath(path, &fman.open_path_sig)).await;
            let v = fman.open_path_sig.wait().await;
            match v {
                Ok(file) => {
                    println!("found = {:?}", file);
                    fman.close_file(file).await
                },
                Err(e) => println!("e = {:?}", e)
            };
            Response::ok("ok").with_header("Content-Type", "text/html")
        }))
}
