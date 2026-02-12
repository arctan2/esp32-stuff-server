#![allow(nonstandard_style)]
#![allow(unused)]

pub mod file_uploader;

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
use file_manager::{FMan, BlkDev, ExtAlloc, get_file_manager};
use file_manager::consts;

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
    pub file: Result<FileType, Error<D::Error>>,
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

pub struct FilesIterChunks<D: BlockDevice, A: Allocator + Clone> {
    pub db_dir: Result<RawDirectory, Error<D::Error>>,
    pub fman: &'static FMan,
    pub allocator: A
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

pub struct DownloadIterChunks<D: BlockDevice, A: Allocator + Clone> {
    pub file: Result<FileType, Error<D::Error>>,
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

pub async fn handle_file_upload(file: FileUploader) -> impl IntoResponse {
    "success"
}

pub async fn handle_music_upload(file: MusicUploader) -> impl IntoResponse {
    "success"
}

pub async fn handle_fs(path: String) -> impl IntoResponse {
    let fman = get_file_manager();
    let file = fman.resolve_path_iter(&path).await;
    ChunkedResponse::new(FsIterChunks::<BlkDev, ExtAlloc> { 
        file, fman, allocator: esp_alloc::ExternalMemory
    })
}

pub async fn handle_files() -> impl IntoResponse {
    let fman = get_file_manager();
    let db_dir = fman.open_dir(None, consts::DB_DIR).await;
    ChunkedResponse::new(FilesIterChunks::<BlkDev, ExtAlloc> { 
        db_dir, fman, allocator: esp_alloc::ExternalMemory
    })
}

pub async fn handle_download(path: String) -> impl IntoResponse {
    let fman = get_file_manager();
    let file = fman.resolve_path_iter(&path).await;
    ChunkedResponse::new(DownloadIterChunks::<BlkDev, ExtAlloc> { 
        file, fman, allocator: esp_alloc::ExternalMemory
    })
}
