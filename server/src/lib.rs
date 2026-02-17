#![allow(nonstandard_style)]
#![feature(impl_trait_in_assoc_type)]

#![no_std]
extern crate alloc;

#[cfg(feature = "std-mode")]
extern crate std;

pub(crate) mod internal_prelude {
    #![allow(unused)]
    pub use alloc::string::{String, ToString};
    pub use core::result::Result::{self, Ok, Err};
    pub use core::option::Option::{self, Some, None};
}
use internal_prelude::*;
use alloc::format;

pub mod file_uploader;

use alpa::embedded_sdmmc_fs::{DbDirSdmmc, VM};
use alpa::db::Database;
use alpa::{Query, QueryExecutor, Value};
use embedded_sdmmc::{BlockDevice, RawDirectory, TimeSource, VolumeManager};
use picoserve::routing::{PathDescription};
use picoserve::response::{IntoResponse};
use picoserve::request::{RequestBody, RequestParts, Path};
use picoserve::extract::{FromRequest};
use picoserve::io::Read;
use picoserve::response::Response;
use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;
use picoserve::response::chunked::{ChunksWritten, ChunkedResponse, ChunkWriter, Chunks};
use file_manager::{FMan, BlkDev, ExtAlloc, get_file_manager, FManError, FileType, CardState, consts, AsyncRootFn};

#[cfg(feature = "embassy-mode")]
use esp_println::println;
#[cfg(feature = "std-mode")]
use std::println;

#[cfg(feature = "embassy-mode")]
use file_manager::{ConcreteSpi, ConcreteDelay};

pub static HOME_PAGE: &str = include_str!("./html/home.html");

#[derive(Copy, Clone, Debug)]
pub struct CatchAll;

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

pub struct FsIterChunks<D: BlockDevice, A: Allocator + Clone> {
    pub file: Result<FileType, FManError<D::Error>>,
    #[cfg(feature = "embassy-mode")]
    pub fman: &'static FMan<ConcreteSpi<'static>, ConcreteDelay>,
    #[cfg(feature = "std-mode")]
    pub fman: &'static FMan,
    pub allocator: A
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
                                let mut buffer: Vec<u8, A> = Vec::with_capacity_in(1024, self.allocator.clone());
                                buffer.resize(buffer.capacity(), 0);
                                buffer.fill(0);
                                loop {
                                    match vm.read(f, buffer.as_mut()) {
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

struct HandleFilesAsync<W: picoserve::io::Write> {
    chunk_writer: ChunkWriter<W>,
}

impl<W, D, T> AsyncRootFn<D, T, Result<ChunksWritten, W::Error>> for HandleFilesAsync<W>
where 
    W: picoserve::io::Write,
    D: BlockDevice,
    T: TimeSource,
{
    type Fut<'a> = impl core::future::Future<Output = Result<Result<ChunksWritten, W::Error>, FManError<D::Error>>>
                    + 'a where Self: 'a, D: 'a, T: 'a;

    fn call<'a>(mut self, root_dir: RawDirectory, vm: &'a VolumeManager<D, T, 4, 4, 1>) -> Self::Fut<'a> {
        async move {
            let root_dir = root_dir.to_directory(vm);
            let allocator = ExtAlloc::default();

            match root_dir.open_dir(consts::DB_DIR) {
                Ok(dir) => {
                    let db_dir = DbDirSdmmc::new(dir.to_raw_directory());
                    let vm = VM::new(vm);
                    let mut db = match Database::new_init(vm, db_dir, allocator.clone()) {
                        Ok(d) => d,
                        Err(e) => {
                            if let Err(e) = self.chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await {
                                return Ok(Err(e));
                            }
                            return Ok(self.chunk_writer.finalize().await);
                        }
                    };
                

                    let files_table = match db.get_table("files", allocator.clone()) {
                        Ok(t) => t,
                        Err(e) => {
                            if let Err(e) = self.chunk_writer.write_chunk(format!("table not found: {:?}", e).as_bytes()).await {
                                return Ok(Err(e));
                            }
                            return Ok(self.chunk_writer.finalize().await);
                        }
                    };

                    {
                        let query = Query::<_, &str>::new(files_table, allocator.clone());
                        match QueryExecutor::new(
                            query, &mut db.table_buf, &mut db.buf1, &mut db.buf2,
                            &db.file_handler.page_rw.as_ref().unwrap()
                        ) {
                            Ok(mut exec) => {
                                while let Ok(row) = exec.next() {
                                    let actual_name = unsafe { core::str::from_utf8_unchecked(row[0].to_chars().unwrap()) };
                                    let name = unsafe { core::str::from_utf8_unchecked(row[1].to_chars().unwrap()) };
                                    if let Err(e) = write!(
                                        self.chunk_writer,
                                        "<div><span class=\"size\">{} B</span><a>{};{}</a></div><br>",
                                        row[2].to_int().unwrap(),
                                        actual_name,
                                        name
                                    ).await {
                                        return Ok(Err(e));
                                    }
                                }
                            },
                            Err(_) => {
                                if let Err(e) = self.chunk_writer.write_chunk(b"<i>table empty</i><br>").await {
                                    return Ok(Err(e));
                                }
                            }
                        };
                    }

                    if let Err(e) = self.chunk_writer.write_chunk(include_str!("./html/files.html").as_bytes()).await {
                        return Ok(Err(e));
                    }
                },
                Err(e) => {
                    if let Err(e) = self.chunk_writer.write_chunk(format!("error: {:?}", e).as_bytes()).await {
                        return Ok(Err(e));
                    }
                }
            }
            return Ok(self.chunk_writer.finalize().await);
        }
    }
}

pub struct FilesIterChunks {
    #[cfg(feature = "embassy-mode")]
    pub fman: &'static FMan<ConcreteSpi<'static>, ConcreteDelay>,
    #[cfg(feature = "std-mode")]
    pub fman: &'static FMan,
}

impl Chunks for FilesIterChunks {
    fn content_type(&self) -> &'static str {
        "text/html"
    }

    async fn write_chunks<W: picoserve::io::Write>(
        self,
        mut chunk_writer: ChunkWriter<W>,
    ) -> Result<ChunksWritten, W::Error> {
        if self.fman.is_card_active().await {
            match self.fman.with_root_dir_async(HandleFilesAsync { chunk_writer }).await {
                Ok(res) => res,
                Err(_) => unreachable!()
            }
        } else {
            chunk_writer.write_chunk(b"SD Card not active").await?;
            chunk_writer.finalize().await
        }
    }
}

pub struct DownloadIterChunks<D: BlockDevice, A: Allocator + Clone> {
    pub file: Result<FileType, FManError<D::Error>>,
    #[cfg(feature = "embassy-mode")]
    pub fman: &'static FMan<ConcreteSpi<'static>, ConcreteDelay>,
    #[cfg(feature = "std-mode")]
    pub fman: &'static FMan,
    pub allocator: A
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
                    FileType::Dir(_) => {
                    },
                    FileType::File(_, f) => {
                        let state = self.fman.state.lock().await;
                        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
                            let mut buffer: Vec<u8, A> = Vec::with_capacity_in(1024, self.allocator.clone());
                            buffer.resize(buffer.capacity(), 0);
                            buffer.fill(0);
                            loop {
                                match vm.read(f, buffer.as_mut()) {
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

impl<'r, State> FromRequest<'r, State> for FileUploader {
    type Rejection = &'static str;

    async fn from_request<R: Read>(
        _state: &'r State,
        parts: RequestParts<'r>,
        body: RequestBody<'r, R>,
    ) -> Result<Self, Self::Rejection> {
        file_uploader::upload_file_to_dir(parts, body, consts::FILES_DIR, consts::FILES_TABLE).await.map(|_| Self)
    }
}

pub struct MusicUploader;

impl<'r, State> FromRequest<'r, State> for MusicUploader {
    type Rejection = &'static str;

    async fn from_request<R: Read>(
        _state: &'r State,
        parts: RequestParts<'r>,
        body: RequestBody<'r, R>,
    ) -> Result<Self, Self::Rejection> {
        file_uploader::upload_file_to_dir(parts, body, consts::MUSIC_DIR, consts::MUSIC_TABLE).await.map(|_| Self)
    }
}

pub async fn handle_file_upload(_: FileUploader) -> impl IntoResponse {
    "success"
}

pub async fn handle_music_upload(_: MusicUploader) -> impl IntoResponse {
    "success"
}

pub async fn handle_fs(path: String) -> impl IntoResponse {
    #[cfg(feature = "embassy-mode")]
    let fman = get_file_manager().await;
    #[cfg(feature = "std-mode")]
    let fman = get_file_manager();

    let file = fman.resolve_path_iter(&path).await;

    #[cfg(feature = "std-mode")] {
        ChunkedResponse::new(FsIterChunks::<BlkDev, ExtAlloc> { 
            file, fman, allocator: ExtAlloc::default()
        })
    }

    #[cfg(feature = "embassy-mode")] {
        ChunkedResponse::new(FsIterChunks::<BlkDev<ConcreteSpi<'static>, ConcreteDelay>, ExtAlloc> { 
            file, fman, allocator: ExtAlloc::default()
        })
    }
}

pub async fn handle_files() -> impl IntoResponse {
    #[cfg(feature = "embassy-mode")]
    let fman = get_file_manager().await;
    #[cfg(feature = "std-mode")]
    let fman = get_file_manager();

    #[cfg(feature = "std-mode")] {
        ChunkedResponse::new(FilesIterChunks { 
            fman
        })
    }

    #[cfg(feature = "embassy-mode")] {
        ChunkedResponse::new(FilesIterChunks { 
            fman
        })
    }
}

pub async fn handle_download(path: String) -> impl IntoResponse {
    #[cfg(feature = "embassy-mode")]
    let fman = get_file_manager().await;
    #[cfg(feature = "std-mode")]
    let fman = get_file_manager();

    let file = fman.resolve_path_iter(&path).await;

    #[cfg(feature = "std-mode")] {
        ChunkedResponse::new(DownloadIterChunks::<BlkDev, ExtAlloc> { 
            file, fman, allocator: ExtAlloc::default()
        })
    }

    #[cfg(feature = "embassy-mode")] {
        ChunkedResponse::new(DownloadIterChunks::<BlkDev<ConcreteSpi<'static>, ConcreteDelay>, ExtAlloc> { 
            file, fman, allocator: ExtAlloc::default()
        })
    }
}


struct DeleteFileAsync {
    name: String
}

impl<D, T> AsyncRootFn<D, T, &'static str> for DeleteFileAsync
where 
    D: BlockDevice,
    T: TimeSource,
{
    type Fut<'a> = impl core::future::Future<Output = Result<&'static str, FManError<D::Error>>> + 'a where Self: 'a, D: 'a, T: 'a;

    fn call<'a>(self, root_dir: RawDirectory, vm: &'a VolumeManager<D, T, 4, 4, 1>) -> Self::Fut<'a> {
        async move {
            let root_dir = root_dir.to_directory(vm);
            let allocator = ExtAlloc::default();
            let db_dir = root_dir.open_dir(consts::DB_DIR).map_err(FManError::SdErr)?.to_raw_directory();
            let files_dir = root_dir.open_dir(consts::FILES_DIR).map_err(FManError::SdErr)?;

            let vm = VM::new(vm);
            let mut db = Database::new_init(vm, DbDirSdmmc::new(db_dir), allocator.clone()).map_err(FManError::DbErr)?;
        
            let files_table = db.get_table("files", allocator.clone()).map_err(FManError::DbErr)?;

            match files_dir.delete_file_in_dir(self.name.as_str()) {
                Err(embedded_sdmmc::Error::NotFound) => (),
                Err(e) => return Err(FManError::SdErr(e)),
                Ok(()) => ()
            }

            db.delete_from_table(files_table, Value::Chars(self.name.as_bytes()), allocator.clone()).map_err(FManError::DbErr)?;

            Ok("success")
        }
    }
}

pub async fn handle_delete_file(name: String) -> impl IntoResponse {
    #[cfg(feature = "embassy-mode")]
    let fman = get_file_manager().await;
    #[cfg(feature = "std-mode")]
    let fman = get_file_manager();

    let r = DeleteFileAsync { name };
    fman.with_root_dir_async(r).await.map_err(|e| picoserve::response::DebugValue(e))
}

struct DeleteDbAsync;

impl<D, T> AsyncRootFn<D, T, &'static str> for DeleteDbAsync
where 
    D: BlockDevice,
    T: TimeSource,
{
    type Fut<'a> = impl core::future::Future<Output = Result<&'static str, FManError<D::Error>>> + 'a where Self: 'a, D: 'a, T: 'a;

    fn call<'a>(self, root_dir: RawDirectory, vm: &'a VolumeManager<D, T, 4, 4, 1>) -> Self::Fut<'a> {
        async move {
            let root_dir = root_dir.to_directory(vm);
            let db_dir = root_dir.open_dir(consts::DB_DIR).map_err(FManError::SdErr)?;
            match db_dir.delete_file_in_dir(alpa::WAL_FILE_NAME) {
                Err(embedded_sdmmc::Error::NotFound) => (),
                Err(e) => return Err(FManError::SdErr(e)),
                Ok(()) => ()
            }

            match db_dir.delete_file_in_dir(alpa::DB_FILE_NAME) {
                Err(embedded_sdmmc::Error::NotFound) => (),
                Err(e) => return Err(FManError::SdErr(e)),
                Ok(()) => ()
            }

            Ok("success")
        }
    }
}

pub async fn handle_delete_db() -> impl IntoResponse {
    #[cfg(feature = "embassy-mode")]
    let fman = get_file_manager().await;
    #[cfg(feature = "std-mode")]
    let fman = get_file_manager();

    fman.with_root_dir_async(DeleteDbAsync).await.map_err(|e| picoserve::response::DebugValue(e))
}
