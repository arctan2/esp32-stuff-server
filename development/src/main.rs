#![allow(nonstandard_style)]
use alpa::embedded_sdmmc_ram_device::{allocators, block_device, esp_alloc, timesource};
use alpa::embedded_sdmmc_ram_device::fs::{DbDirSdmmc};
use alpa::{Column, ColumnType, Value, Row, Query, QueryExecutor};
use crate::block_device::{FsBlockDevice};
use crate::timesource::{DummyTimesource};
use embedded_sdmmc::{BlockDevice, TimeSource, Error};
use picoserve::time::Duration;
use picoserve::routing::{post, get, parse_path_segment, PathDescription};
use picoserve::response::{IntoResponse, Response, DebugValue};
use picoserve::request::{RequestBody, Request, RequestParts, Path};
use picoserve::extract::{FromRequest, State};
use picoserve::io::Read;
use file_manager::{FileManager, Signal, Channel, FileType, CardState};
use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;
use std::sync::OnceLock;
use picoserve::response::chunked::{ChunksWritten, ChunkedResponse, ChunkWriter, Chunks};

static HOME_PAGE: &str = include_str!("./html/home.html");

type BlkDev = block_device::FsBlockDevice;
type TimeSrc = timesource::DummyTimesource;
type FMan = FileManager<BlkDev, TimeSrc, 4, 4, 1>;

#[derive(Debug)]
pub struct SyncFMan(pub FMan);

unsafe impl Send for SyncFMan {}
unsafe impl Sync for SyncFMan {}

static FILE_MAN: OnceLock<SyncFMan> = OnceLock::new();

fn init_file_manager(block_device: FsBlockDevice, time_src: DummyTimesource) {
    FILE_MAN.set(
        SyncFMan(FileManager::new(block_device, time_src))
    ).expect("initing twice file_manager");
}

fn get_file_manager() -> &'static FMan {
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
            loop {
                match init_file_system::<block_device::FsBlockDevice>().await {
                    Ok(()) => break,
                    Err(_) => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
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

async fn init_file_system<D: BlockDevice>() -> Result<(), Error<D::Error>>
    where
        Error<<D as BlockDevice>::Error>: From<Error<block_device::FsBlockDeviceError>>
{
    let fman = get_file_manager();
    let root_dir = fman.root_dir().await?;
    fman.mkdir(root_dir.clone(), "DB").await?;
    fman.mkdir(root_dir.clone(), "FILES").await?;
    fman.mkdir(root_dir.clone(), "MUSIC").await?;
    fman.mkdir(root_dir.clone(), "F_IDS").await?;

    let db_dir = fman.resolve_path_iter("DB").await?;
    match db_dir {
        FileType::Dir(_) => {
            fman.mkdir(db_dir.clone(), "FILES").await?;
            fman.mkdir(db_dir.clone(), "MUSIC").await?;
        },
        _ => panic!("your device is cooked")
    };

    fman.close_file(db_dir);
    fman.close_file(root_dir);
    Ok(())
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

struct FileIterChunks<D: BlockDevice, A: Allocator + Clone> {
    file: Result<FileType, Error<D::Error>>,
    fman: &'static FMan,
    allocator: A
}

impl <D: BlockDevice, A: Allocator + Clone> Chunks for FileIterChunks<D, A> {
    fn content_type(&self) -> &'static str {
        "text/html"
    }

    async fn write_chunks<W: picoserve::io::Write>(
        self,
        mut chunk_writer: ChunkWriter<W>,
    ) -> Result<ChunksWritten, W::Error> {
        match self.file {
            Ok(file) => {
                match file {
                    FileType::Dir(dir) => {
                        let state = self.fman.state.lock().await;
                        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
                            let mut files: Vec<Vec<u8, A>, A> = Vec::new_in(self.allocator.clone());
                            vm.iterate_dir(dir, |entry| {
                                if entry.attributes.is_volume() {
                                    return;
                                }
                                let mut buf: Vec<u8, A> = Vec::new_in(self.allocator.clone());
                                let is_dir = entry.attributes.is_directory();
                                buf.extend_from_slice(b"<div>");
                                buf.extend_from_slice(b"<span class=\"size\">");
                                buf.extend_from_slice(format!("{:?} B", entry.size).as_bytes());
                                buf.extend_from_slice(b"</span>");
                                buf.extend_from_slice(b"<a>");
                                buf.extend_from_slice(entry.name.base_name());
                                if is_dir {
                                    buf.push('/' as u8);
                                } else {
                                    buf.push('.' as u8);
                                    buf.extend_from_slice(entry.name.extension());
                                }
                                buf.extend_from_slice(b"</a>");
                                buf.extend_from_slice(b"</div>");
                                files.push(buf);
                            }).unwrap();
                            for f in files.iter() {
                                chunk_writer.write_chunk(f).await?;
                                chunk_writer.write_chunk("<br>".as_bytes()).await?;
                            }

                            chunk_writer.write_chunk(include_str!("./html/dir_page.html").as_bytes()).await?;
                        }
                    },
                    FileType::File(ref entry, f) => {
                        let state = self.fman.state.lock().await;
                        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
                            let ext = entry.name.extension();
                            if ext == b"TXT" || ext == b"HTM" {
                                if ext == b"TXT" {
                                    chunk_writer.write_chunk(b"<pre>").await?;
                                }
                                loop {
                                    let mut buffer = [0u8; 1024];
                                    
                                    match vm.read(f, &mut buffer) {
                                        Ok(count) => {
                                            chunk_writer.write_chunk(&buffer[0..count]).await?;
                                            match vm.file_eof(f) {
                                                Ok(is_eof) => if is_eof { break },
                                                Err(e) => {
                                                    chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await?;
                                                    break;
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await?;
                                            break;
                                        }
                                    }
                                }

                                if ext == b"TXT" {
                                    chunk_writer.write_chunk(b"</pre>").await?;
                                }
                            } else {
                                chunk_writer.write_chunk(b"only files with TXT or HTM extension is supported to view.").await?;
                            }

                            if ext != b"HTM" {
                                chunk_writer.write_chunk(include_str!("./html/file_page.html").as_bytes()).await?;
                            }
                        }
                    }
                }
                self.fman.close_file(file).await;
            },
            Err(e) => {
                chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await?;
            }
        }
        chunk_writer.finalize().await
    }
}

struct DownloadIterChunks<D: BlockDevice, A: Allocator + Clone> {
    file: Result<FileType, Error<D::Error>>,
    fman: &'static FMan,
    allocator: A
}

impl <D: BlockDevice, A: Allocator + Clone> Chunks for DownloadIterChunks<D, A> {
    fn content_type(&self) -> &'static str {
        ""
    }

    async fn write_chunks<W: picoserve::io::Write>(
        self,
        mut chunk_writer: ChunkWriter<W>,
    ) -> Result<ChunksWritten, W::Error> {
        match self.file {
            Ok(file) => {
                match file {
                    FileType::Dir(dir) => {
                    },
                    FileType::File(ref entry, f) => {
                        let state = self.fman.state.lock().await;
                        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
                            loop {
                                let mut buffer = [0u8; 4096];
                                match vm.read(f, &mut buffer) {
                                    Ok(count) => {
                                        chunk_writer.write_chunk(&buffer[0..count]).await?;
                                        match vm.file_eof(f) {
                                            Ok(is_eof) => if is_eof { break },
                                            Err(e) => {
                                                chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await?;
                                                break;
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await?;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                self.fman.close_file(file).await;
            },
            Err(e) => {
                chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await?;
            }
        }
        chunk_writer.finalize().await
    }
}


pub struct FileUploader {
    pub filename: [u8; 128],
    pub filename_len: usize,
}

enum Step {
    FindFilename,
    ReadFilename,
    FindDataStart,
    StreamingBody,
}

fn find_boundary_across_buffers(buf1: &[u8], buf2: &[u8], pat: &[u8]) -> Option<(usize, usize)> {
    if let Some(pos) = buf1.windows(pat.len()).position(|w| w == pat) {
        return Some((1, pos));
    }

    if let Some(pos) = buf2.windows(pat.len()).position(|w| w == pat) {
        return Some((2, pos));
    }

    let len1 = buf1.len();
    let pat_len = pat.len();
    
    for i in 1..pat_len {
        let suffix_len = pat_len - i;
        
        if len1 >= i && buf2.len() >= suffix_len {
            let part1 = &buf1[len1 - i..];
            let part2 = &buf2[..suffix_len];
            
            if part1 == &pat[..i] && part2 == &pat[i..] {
                return Some((1, len1 - i));
            }
        }
    }

    None
}

impl<'r, State> FromRequest<'r, State> for FileUploader {
    type Rejection = &'static str;

    async fn from_request<R: Read>(
        _state: &'r State,
        parts: RequestParts<'r>,
        body: RequestBody<'r, R>,
    ) -> Result<Self, Self::Rejection> {
        let mut reader = body.reader();
        let mut buffer = [0u8; 512];
        let mut filename = [0u8; 128];
        let mut filename_len = 0;

        let mut step = Step::FindFilename;
        let mut pattern_idx = 0;

        let boundary_key = "boundary=";
        let content_type = parts.headers().get("Content-Type").ok_or("Content-Type not found")?;
        let boundary_start = content_type.as_str().unwrap().find(boundary_key).ok_or("boundary not found")?;
        let boundary = &content_type.as_raw()[boundary_start + boundary_key.len()..];
        let mut lookback_buf = [0u8; 128];
        let mut lookback_len = 0;

        println!("path = {:?}", parts.path());

        'stream: while let Ok(n) = reader.read(&mut buffer).await {
            if n == 0 { break; }

            for (i, &byte) in buffer[..n].iter().enumerate() {
                match step {
                    Step::FindFilename => {
                        let file_pat = b"filename=\"";
                        if byte == file_pat[pattern_idx] {
                            pattern_idx += 1;
                            if pattern_idx == file_pat.len() {
                                step = Step::ReadFilename;
                                pattern_idx = 0;
                            }
                        } else {
                            pattern_idx = 0;
                        }
                    }

                    Step::ReadFilename => {
                        println!("reading byte = '{}'", byte as char);
                        if byte == '"' as u8 {
                            step = Step::FindDataStart;
                            pattern_idx = 0;
                        } else {
                            filename[filename_len] = byte;
                            filename_len += 1;
                        }
                    }

                    Step::FindDataStart => {
                        let header_sep = b"\r\n\r\n";
                        if byte == header_sep[pattern_idx] {
                            pattern_idx += 1;
                            if pattern_idx == header_sep.len() {
                                step = Step::StreamingBody;
                                println!("found body start");
                            }
                        } else {
                            pattern_idx = 0;
                        }
                    }

                    Step::StreamingBody => {
                        let data_chunk = &buffer[i..n];

                        if let Some(pos) = find_boundary_across_buffers(&lookback_buf[..lookback_len], data_chunk, boundary) {
                            match pos {
                                (1, idx) => {
                                    let end = idx.saturating_sub(4);
                                    print!("{}", str::from_utf8(&lookback_buf[0..end]).unwrap());
                                },
                                (2, idx) => {
                                    if idx >= 4 {
                                        print!("{}", str::from_utf8(&lookback_buf[..lookback_len]).unwrap());
                                        print!("{}", str::from_utf8(&data_chunk[0..idx-4]).unwrap());
                                    } else {
                                        let lookback_trim = 4 - idx;
                                        let end = lookback_len.saturating_sub(lookback_trim);
                                        print!("{}", str::from_utf8(&lookback_buf[..end]).unwrap());
                                    }
                                },
                                _ => unreachable!()
                            }
                            println!("pos = {:?}", pos);
                            break 'stream;
                        } else {
                            print!("{}", str::from_utf8(&lookback_buf[..lookback_len]).unwrap());

                            if data_chunk.len() >= lookback_buf.len() {
                                let safe_len = data_chunk.len() - lookback_buf.len();
                                print!("{}", str::from_utf8(&data_chunk[..safe_len]).unwrap());

                                let tail = &data_chunk[safe_len..];
                                lookback_buf[..tail.len()].copy_from_slice(tail);
                                lookback_len = tail.len();
                            } else {
                                let move_amt = data_chunk.len();
                                lookback_buf.copy_within(move_amt.., 0);

                                let start = lookback_buf.len() - move_amt;
                                lookback_buf[start..].copy_from_slice(data_chunk);
                                lookback_len = lookback_buf.len();
                            }
                        }
                        continue 'stream;
                    }
                }
            }
        }
        
        Ok(FileUploader { filename, filename_len })
    }
}

async fn handle_file_upload(path: String, file: FileUploader) -> impl IntoResponse {
    let name = core::str::from_utf8(&file.filename[..file.filename_len])
        .unwrap_or("unknown");

    picoserve::response::DebugValue("")
}

async fn handle_fs(path: String) -> impl IntoResponse {
    let fman = get_file_manager();
    let file = fman.resolve_path_iter(&path).await;
    ChunkedResponse::new(FileIterChunks::<FsBlockDevice, allocators::SimAllocator<23>> { 
        file, fman, allocator: esp_alloc::ExternalMemory
    })
}

async fn handle_download(path: String) -> impl IntoResponse {
    let fman = get_file_manager();
    let file = fman.resolve_path_iter(&path).await;
    ChunkedResponse::new(DownloadIterChunks::<FsBlockDevice, allocators::SimAllocator<23>> { 
        file, fman, allocator: esp_alloc::ExternalMemory
    })
}

fn router() -> picoserve::Router<impl picoserve::routing::PathRouter> {
    picoserve::Router::new()
        .route("/", get(|| async {
            Response::ok(HOME_PAGE).with_header("Content-Type", "text/html")
        }))
        .route(("/fs", CatchAll), get(handle_fs))
        .route(("/download", CatchAll), get(handle_download))
        .route(("/upload", CatchAll), post(handle_file_upload))
}
