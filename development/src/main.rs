#![allow(nonstandard_style)]
mod consts;
use alpa::embedded_sdmmc_ram_device::{allocators, block_device, esp_alloc, timesource, fs};
use crate::fs::{DbDirSdmmc};
use alpa::db::Database;
use alpa::{Column, ColumnType, Value, Row, Query, QueryExecutor};
use crate::block_device::{FsBlockDevice};
use crate::timesource::{DummyTimesource};
use embedded_sdmmc::{BlockDevice, TimeSource, Error, RawDirectory, Mode};
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
                match init_file_system::<FsBlockDevice, allocators::SimAllocator<23>>(esp_alloc::ExternalMemory).await {
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

use crate::block_device::FsBlockDeviceError;

#[allow(dead_code)]
#[derive(Debug)]
enum InitError {
    SdCard(embedded_sdmmc::Error<FsBlockDeviceError>),
    Database(alpa::db::Error<embedded_sdmmc::Error<FsBlockDeviceError>>),
}

impl From<alpa::db::Error<embedded_sdmmc::Error<FsBlockDeviceError>>> for InitError {
    fn from(e: alpa::db::Error<embedded_sdmmc::Error<FsBlockDeviceError>>) -> Self {
        InitError::Database(e)
    }
}

impl From<embedded_sdmmc::Error<FsBlockDeviceError>> for InitError {
    fn from(e: embedded_sdmmc::Error<FsBlockDeviceError>) -> Self {
        InitError::SdCard(e)
    }
}

async fn init_file_system<D: BlockDevice, A: Allocator + Clone>(allocator: A) -> Result<(), InitError>
where 
    embedded_sdmmc::Error<D::Error>: Into<embedded_sdmmc::Error<FsBlockDeviceError>>
{
    let fman = get_file_manager();
    let root_dir = fman.root_dir().await?;
    fman.mkdir(root_dir.clone(), consts::DB_DIR).await?;
    fman.mkdir(root_dir.clone(), consts::FILES_DIR).await?;
    fman.mkdir(root_dir.clone(), consts::MUSIC_DIR).await?;

    {
        let db_dir = fman.open_dir(Some(root_dir), consts::DB_DIR).await?;
        let _ = fman.close_dir(root_dir).await;
        let state = fman.state.lock().await;

        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
            let stuff_dir = DbDirSdmmc::new(db_dir.to_directory(vm));
            let mut db = Database::new_init(&stuff_dir, allocator.clone())?;

            {
                let name = Column::new("name", ColumnType::Chars).primary();
                let count = Column::new("count", ColumnType::Int);
                db.new_table_begin(consts::COUNT_TRACKER_TABLE);
                db.add_column(name)?;
                db.add_column(count)?;
                let _ = db.create_table(allocator.clone())?;
            }

            {
                let name = Column::new("path", ColumnType::Chars).primary();
                let count = Column::new("name", ColumnType::Chars);
                let size = Column::new("size", ColumnType::Int);
                db.new_table_begin(consts::FILES_TABLE);
                db.add_column(name)?;
                db.add_column(count)?;
                db.add_column(size)?;
                let _ = db.create_table(allocator.clone())?;
            }

            {
                let name = Column::new("path", ColumnType::Chars).primary();
                let count = Column::new("name", ColumnType::Chars);
                db.new_table_begin(consts::MUSIC_TABLE);
                db.add_column(name)?;
                db.add_column(count)?;
                let _ = db.create_table(allocator.clone())?;
            }

            let count_tracker = db.get_table(consts::COUNT_TRACKER_TABLE, allocator.clone())?;

            {
                let mut row = Row::new_in(allocator.clone());
                row.push(Value::Chars(consts::FILES_TABLE.as_bytes()));
                row.push(Value::Int(1));
                db.insert_to_table(count_tracker, row, allocator.clone())?;
            }

            {
                let mut row = Row::new_in(allocator.clone());
                row.push(Value::Chars(consts::MUSIC_TABLE.as_bytes()));
                row.push(Value::Int(1));
                db.insert_to_table(count_tracker, row, allocator.clone())?;
            }
        }
    }
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

struct FsIterChunks<D: BlockDevice, A: Allocator + Clone> {
    file: Result<FileType, Error<D::Error>>,
    fman: &'static FMan,
    allocator: A
}

impl <D: BlockDevice, A: Allocator + Clone> Chunks for FsIterChunks<D, A> {
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
                self.fman.close_file_type(file).await;
            },
            Err(e) => {
                chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await?;
            }
        }
        chunk_writer.finalize().await
    }
}

struct FilesIterChunks<D: BlockDevice, A: Allocator + Clone> {
    db_dir: Result<RawDirectory, Error<D::Error>>,
    fman: &'static FMan,
    allocator: A
}

impl <D: BlockDevice, A: Allocator + Clone> Chunks for FilesIterChunks<D, A> {
    fn content_type(&self) -> &'static str {
        "text/html"
    }

    async fn write_chunks<W: picoserve::io::Write>(
        self,
        mut chunk_writer: ChunkWriter<W>,
    ) -> Result<ChunksWritten, W::Error> {
        match self.db_dir {
            Ok(dir) => {
                let state = self.fman.state.lock().await;
                if let CardState::Active{ ref vm, vol: _ } = state.card_state {
                    let db_dir = DbDirSdmmc::new(dir.to_directory(vm));
                    let mut db = match Database::new_init(&db_dir, self.allocator.clone()) {
                        Ok(d) => d,
                        Err(e) => {
                            chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await?;
                            return chunk_writer.finalize().await;
                        }
                    };
                

                    let mut files_table = match db.get_table("files", self.allocator.clone()) {
                        Ok(t) => t,
                        Err(e) => {
                            chunk_writer.write_chunk(format!("table not found: {:?}", e).as_bytes()).await?;
                            return chunk_writer.finalize().await;
                        }
                    };

                    {
                        let query = Query::<_, &str>::new(files_table, self.allocator.clone());
                        match QueryExecutor::new(
                            query, &mut db.table_buf, &mut db.buf1, &mut db.buf2,
                            &db.file_handler.page_rw.as_ref().unwrap()
                        ) {
                            Ok(mut exec) => {
                                while let Ok(row) = exec.next() {
                                    chunk_writer.write_chunk(b"<div><span class=\"size\">").await?;
                                    chunk_writer.write_chunk(format!("{} B", row[2].to_int().unwrap()).as_bytes()).await?;
                                    chunk_writer.write_chunk(b"</span><a>").await?;
                                    chunk_writer.write_chunk(row[0].to_chars().unwrap()).await?;
                                    chunk_writer.write_chunk(b";").await?;
                                    chunk_writer.write_chunk(row[1].to_chars().unwrap()).await?;
                                    chunk_writer.write_chunk(b"</a></div><br>").await?;
                                }
                            },
                            Err(e) => {
                                chunk_writer.write_chunk(b"<i>table empty</i><br>").await?;
                            }
                        };
                    }

                    chunk_writer.write_chunk(include_str!("./html/files.html").as_bytes()).await?;
                }
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
                self.fman.close_file_type(file).await;
            },
            Err(e) => {
                chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await?;
            }
        }
        chunk_writer.finalize().await
    }
}


pub struct FileUploader;

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

async fn upload_file_to_dir<'r, R: Read>(
    parts: RequestParts<'r>,
    body: RequestBody<'r, R>,
    file_dir_name: &'static str,
    table_and_count_tracker_name: &'static str
) -> Result<(), &'static str> {
    let table_and_count_tracker_name = table_and_count_tracker_name.as_bytes();
    let fman = get_file_manager();
    let db_dir = fman.open_dir(None, consts::DB_DIR).await.map_err(|_| "unable to open db")?;

    let state = fman.state.lock().await;

    if let CardState::Active{ ref vm, ref vol } = state.card_state {
        let db_dir = DbDirSdmmc::new(db_dir.to_directory(vm));
        let mut db = match Database::new_init(&db_dir, esp_alloc::ExternalMemory) {
            Ok(d) => d,
            Err(e) => return Err("db init error".into())
        };
        let count_tracker_table = db.get_table(consts::COUNT_TRACKER_TABLE, esp_alloc::ExternalMemory)
                                    .map_err(|_| "unable to get count_tracker table")?;
        let files_table = db.get_table(table_and_count_tracker_name, esp_alloc::ExternalMemory)
                            .map_err(|_| "unable to get files table")?;
        let mut cur_file_id: i64 = -1;

        {
            let query = Query::<_, &str>::new(count_tracker_table, esp_alloc::ExternalMemory)
                                         .key(Value::Chars(table_and_count_tracker_name));
            match QueryExecutor::new(
                query, &mut db.table_buf, &mut db.buf1, &mut db.buf2,
                &db.file_handler.page_rw.as_ref().unwrap()
            ) {
                Ok(mut exec) => {
                    if let Ok(row) = exec.next() {
                        cur_file_id = row[1].to_int().unwrap();
                    } else {
                        return Err("bad init".into());
                    }
                },
                Err(e) => {
                    return Err("table empty".into());
                }
            };
        }

        if cur_file_id < 0 || cur_file_id >= 99999999 {
            return Err("id limit reached".into());
        }

        let query_params = parts.query().ok_or("missing extension query")?;
        if query_params.is_empty() {
            return Err("missing extension query".into());
        }

        let mut s = query_params.0.split('=');
        let _ = s.next().unwrap();
        let ext = s.next().unwrap();

        let actual_name = format!("{}.{}", cur_file_id, ext);
        let root_dir = vm.open_root_dir(*vol).map_err(|_| "unable to open root_dir")?;
        let files_dir = vm.open_dir(root_dir, file_dir_name).map_err(|_| "unable to open FILES dir")?;
        let new_file = vm.open_file_in_dir(files_dir, str::from_utf8(actual_name.as_bytes()).unwrap(), Mode::ReadWriteCreate)
                         .map_err(|_| "unable to create file")?;

        vm.close_dir(root_dir).map_err(|_| "unable to close root_dir")?;
        vm.close_dir(files_dir).map_err(|_| "unable to close files_dir")?;

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
        let mut file_size: i64 = 0;

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
                                    let buf = &lookback_buf[0..end];
                                    vm.write(new_file, buf).map_err(|_| "unable to write to new_file")?;
                                    file_size += buf.len() as i64;
                                },
                                (2, idx) => {
                                    if idx >= 4 {
                                        let buf = &lookback_buf[..lookback_len];
                                        vm.write(new_file, buf).map_err(|_| "unable to write to new_file")?;
                                        file_size += buf.len() as i64;

                                        let buf = &data_chunk[0..idx-4];
                                        vm.write(new_file, buf).map_err(|_| "unable to write to new_file")?;
                                        file_size += buf.len() as i64;
                                    } else {
                                        let lookback_trim = 4 - idx;
                                        let end = lookback_len.saturating_sub(lookback_trim);
                                        let buf = &lookback_buf[..end];
                                        vm.write(new_file, buf).map_err(|_| "unable to write to new_file")?;
                                        file_size += buf.len() as i64;
                                    }
                                },
                                _ => unreachable!()
                            }
                            println!("file upload success");
                            break 'stream;
                        } else {
                            let buf = &lookback_buf[..lookback_len];
                            vm.write(new_file, buf).map_err(|_| "unable to write to new_file")?;
                            file_size += buf.len() as i64;

                            if data_chunk.len() >= lookback_buf.len() {
                                let safe_len = data_chunk.len() - lookback_buf.len();
                                let buf = &data_chunk[..safe_len];
                                vm.write(new_file, buf).map_err(|_| "unable to write to new_file")?;
                                file_size += buf.len() as i64;

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

        vm.flush_file(new_file).map_err(|_| "unable to flush new_file")?;
        vm.close_file(new_file).map_err(|_| "unable to close new_file")?;

        {
            let mut row = Row::new_in(esp_alloc::ExternalMemory);
            row.push(Value::Chars(actual_name.as_bytes()));
            row.push(Value::Chars(&filename[..filename_len]));
            row.push(Value::Int(file_size));
            db.insert_to_table(files_table, row, esp_alloc::ExternalMemory).map_err(|_| "unable to insert to table")?;
        }

        {
            let mut row = Row::new_in(esp_alloc::ExternalMemory);
            row.push(Value::Chars(table_and_count_tracker_name));
            row.push(Value::Int(cur_file_id + 1));
            db.update_row(count_tracker_table, Value::Chars(table_and_count_tracker_name), row, esp_alloc::ExternalMemory)
                .map_err(|_| "unable to update count_tracker_table to table")?;
        }

        Ok(())
    } else {
        Err("sdcard not active".into())
    }
}

impl<'r, State> FromRequest<'r, State> for FileUploader {
    type Rejection = &'static str;

    async fn from_request<R: Read>(
        _state: &'r State,
        parts: RequestParts<'r>,
        body: RequestBody<'r, R>,
    ) -> Result<Self, Self::Rejection> {
        upload_file_to_dir(parts, body, consts::FILES_DIR, consts::FILES_TABLE).await.map(|_| Self)
    }
}

struct MusicUploader;

impl<'r, State> FromRequest<'r, State> for MusicUploader {
    type Rejection = &'static str;

    async fn from_request<R: Read>(
        _state: &'r State,
        parts: RequestParts<'r>,
        body: RequestBody<'r, R>,
    ) -> Result<Self, Self::Rejection> {
        upload_file_to_dir(parts, body, consts::MUSIC_DIR, consts::MUSIC_TABLE).await.map(|_| Self)
    }
}

async fn handle_file_upload(file: FileUploader) -> impl IntoResponse {
    "success"
}

async fn handle_music_upload(file: MusicUploader) -> impl IntoResponse {
    "success"
}

async fn handle_fs(path: String) -> impl IntoResponse {
    let fman = get_file_manager();
    let file = fman.resolve_path_iter(&path).await;
    ChunkedResponse::new(FsIterChunks::<FsBlockDevice, allocators::SimAllocator<23>> { 
        file, fman, allocator: esp_alloc::ExternalMemory
    })
}

async fn handle_files(name: String) -> impl IntoResponse {
    let fman = get_file_manager();
    let db_dir = fman.open_dir(None, consts::DB_DIR).await;
    ChunkedResponse::new(FilesIterChunks::<FsBlockDevice, allocators::SimAllocator<23>> { 
        db_dir, fman, allocator: esp_alloc::ExternalMemory
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
        .route("/files", get(handle_files))
        .route(("/download", CatchAll), get(handle_download))
        .route("/upload", post(handle_file_upload))
        .route("/upload-music", post(handle_music_upload))
}
