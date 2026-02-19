#![allow(unused)]
use alpa::embedded_sdmmc_fs::{DbDirSdmmc, VM};
use alpa::db::Database;
use alpa::{Value, Row, Query, QueryExecutor};
use embedded_sdmmc::{Mode, RawDirectory, VolumeManager, BlockDevice, TimeSource};
use picoserve::request::{RequestBody, RequestParts};
use picoserve::io::Read;
use file_manager::{get_file_manager, ExtAlloc, AsyncRootFn, FManError, DummyTimesource, BlkDev, FsBlockDevice};
use crate::consts;
use alloc::format;
use crate::chunks;
use allocator_api2::boxed::Box;
use allocator_api2::vec::Vec;
#[cfg(feature = "std-mode")]
use std::println;

#[cfg(feature = "std-mode")]
pub use tokio::select;
#[cfg(feature = "embassy-mode")]
pub use embassy_futures::select;

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

struct FileUploaderAsync<'r, R: Read> {
    parts: RequestParts<'r>,
    body: RequestBody<'r, R>,
    file_dir_name: &'static str,
    table_and_count_tracker_name: &'static str
}

impl<'r, R> AsyncRootFn<()> for FileUploaderAsync<'r, R>
where R: Read {
    type Fut<'a> = impl core::future::Future<Output = Result<(), FManError<<FsBlockDevice as BlockDevice>::Error>>> + 'a where Self: 'a;

    fn call<'a>(self, root_dir: RawDirectory, vm: &'a VolumeManager<BlkDev, DummyTimesource, 4, 4, 1>) -> Self::Fut<'a> {
        async move {
            let root_dir = root_dir.to_directory(vm);
            let db_dir = root_dir.open_dir(consts::DB_DIR).map_err(|_| "unable to open db dir")?.to_raw_directory();
            let db_dir = DbDirSdmmc::new(db_dir);
            let mut db = match Database::new_init(VM::new(vm), db_dir, ExtAlloc::default()) {
                Ok(d) => d,
                Err(_) => return Err("db init error".into())
            };
            let count_tracker_table = db.get_table(consts::COUNT_TRACKER_TABLE, ExtAlloc::default())
                                        .map_err(|_| "unable to get count_tracker table")?;
            let files_table = db.get_table(self.table_and_count_tracker_name, ExtAlloc::default())
                                .map_err(|_| "unable to get files table")?;
            let cur_file_id: i64;

            {
                let query = Query::<_, &str>::new(count_tracker_table, ExtAlloc::default())
                                             .key(Value::Chars(self.table_and_count_tracker_name.as_bytes()));
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
                    Err(_) => {
                        return Err("table empty".into());
                    }
                };
            }

            if cur_file_id < 0 || cur_file_id >= 99999999 {
                return Err("id limit reached".into());
            }

            let query_params = self.parts.query().ok_or("missing extension query")?;
            if query_params.is_empty() {
                return Err("missing extension query".into());
            }

            let mut s = query_params.0.split('=');
            let _ = s.next().unwrap();
            let ext = s.next().unwrap();

            let actual_name = format!("{}.{}", cur_file_id, ext);
            let files_dir = root_dir.open_dir(self.file_dir_name).map_err(|_| "unable to open FILES dir")?.to_raw_directory();

            let mut reader = self.body.reader();

            let boundary_key = "boundary=";
            let content_type = self.parts.headers().get("Content-Type").ok_or("Content-Type not found")?;
            let boundary_start = content_type.as_str().unwrap().find(boundary_key).ok_or("boundary not found")?;
            let boundary = &content_type.as_raw()[boundary_start + boundary_key.len()..];
            let mut boundary_vec = Vec::with_capacity_in(boundary.len(), ExtAlloc::default());
            boundary_vec.extend_from_slice(boundary);
            let boundary = boundary_vec;

            let rsender = chunks::get_ready_sender();
            let mut free_chan = chunks::get_free_chan();

            chunks::send_event_sig(
                chunks::UploadEvent::Begin(
                    files_dir,
                    actual_name,
                    unsafe { chunks::DangerousVMPtr(vm as *const VolumeManager<BlkDev, DummyTimesource, 4, 4, 1>) },
                    boundary,
                )
            ).await;

            loop {
                select!(
                    err = chunks::get_ret_sig().wait() => {
                        return match err {
                            Ok(_) => Ok(()),
                            Err(e) => Err(e.into())
                        };
                    },
                    mut chunk = free_chan.recv() => {
                        if let Ok(n) = reader.read(&mut chunk.buf).await {
                            if chunk.len == 0 {
                                break;
                            }
                            chunk.len = n;
                            rsender.send(chunk).await;
                        } else {
                            chunks::send_event_sig(chunks::UploadEvent::ReadErr).await;
                            return Err("read error".into());
                        }
                    },
                );
            }

            // 'stream: while let Ok(n) = reader.read(buffer.as_mut()).await {
            //     if n == 0 { break; }

            //     for (i, &byte) in buffer[..n].iter().enumerate() {
            //         match step {
            //             Step::FindFilename => {
            //                 let file_pat = b"filename=\"";
            //                 if byte == file_pat[pattern_idx] {
            //                     pattern_idx += 1;
            //                     if pattern_idx == file_pat.len() {
            //                         step = Step::ReadFilename;
            //                         pattern_idx = 0;
            //                     }
            //                 } else {
            //                     pattern_idx = 0;
            //                 }
            //             }

            //             Step::ReadFilename => {
            //                 if byte == '"' as u8 {
            //                     step = Step::FindDataStart;
            //                     pattern_idx = 0;
            //                 } else {
            //                     filename[filename_len] = byte;
            //                     filename_len += 1;
            //                 }
            //             }

            //             Step::FindDataStart => {
            //                 let header_sep = b"\r\n\r\n";
            //                 if byte == header_sep[pattern_idx] {
            //                     pattern_idx += 1;
            //                     if pattern_idx == header_sep.len() {
            //                         step = Step::StreamingBody;
            //                     }
            //                 } else {
            //                     pattern_idx = 0;
            //                 }
            //             }

            //             Step::StreamingBody => {
            //                 let data_chunk = &buffer[i..n];

            //                 if let Some(pos) = find_boundary_across_buffers(&lookback_buf[..lookback_len], data_chunk, &boundary) {
            //                     match pos {
            //                         (1, idx) => {
            //                             let end = idx.saturating_sub(4);
            //                             let buf = &lookback_buf[0..end];
            //                             new_file.write(buf).map_err(|_| "unable to write to new_file")?;
            //                             file_size += buf.len() as i64;
            //                         },
            //                         (2, idx) => {
            //                             if idx >= 4 {
            //                                 let buf = &lookback_buf[..lookback_len];
            //                                 new_file.write(buf).map_err(|_| "unable to write to new_file")?;
            //                                 file_size += buf.len() as i64;

            //                                 let buf = &data_chunk[0..idx-4];
            //                                 new_file.write(buf).map_err(|_| "unable to write to new_file")?;
            //                                 file_size += buf.len() as i64;
            //                             } else {
            //                                 let lookback_trim = 4 - idx;
            //                                 let end = lookback_len.saturating_sub(lookback_trim);
            //                                 let buf = &lookback_buf[..end];
            //                                 new_file.write(buf).map_err(|_| "unable to write to new_file")?;
            //                                 file_size += buf.len() as i64;
            //                             }
            //                         },
            //                         _ => unreachable!()
            //                     }
            //                     break 'stream;
            //                 } else {
            //                     let buf = &lookback_buf[..lookback_len];
            //                     new_file.write(buf).map_err(|_| "unable to write to new_file")?;
            //                     file_size += buf.len() as i64;

            //                     if data_chunk.len() >= lookback_buf.len() {
            //                         let safe_len = data_chunk.len() - lookback_buf.len();
            //                         let buf = &data_chunk[..safe_len];
            //                         new_file.write(buf).map_err(|_| "unable to write to new_file")?;
            //                         file_size += buf.len() as i64;

            //                         let tail = &data_chunk[safe_len..];
            //                         lookback_buf[..tail.len()].copy_from_slice(tail);
            //                         lookback_len = tail.len();
            //                     } else {
            //                         let move_amt = data_chunk.len();
            //                         lookback_buf.copy_within(move_amt.., 0);

            //                         let start = lookback_buf.len() - move_amt;
            //                         lookback_buf[start..].copy_from_slice(data_chunk);
            //                         lookback_len = lookback_buf.len();
            //                     }
            //                 }
            //                 continue 'stream;
            //             }
            //         }
            //     }
            // }

            // new_file.flush().map_err(|_| "unable to flush new_file")?;
            // new_file.close().map_err(|_| "unable to close new_file")?;

            // {
            //     let mut row = Row::new_in(ExtAlloc::default());
            //     row.push(Value::Chars(actual_name.as_bytes()));
            //     row.push(Value::Chars(&filename[..filename_len]));
            //     row.push(Value::Int(file_size));
            //     db.insert_to_table(files_table, row, ExtAlloc::default()).map_err(|_| "unable to insert to table")?;
            // }

            // {
            //     let mut row = Row::new_in(ExtAlloc::default());
            //     row.push(Value::Chars(self.table_and_count_tracker_name.as_bytes()));
            //     row.push(Value::Int(cur_file_id + 1));
            //     db.update_row(count_tracker_table, Value::Chars(self.table_and_count_tracker_name.as_bytes()), row, ExtAlloc::default())
            //         .map_err(|_| "unable to update count_tracker_table to table")?;
            // }

            Ok(())
        }
    }
}

pub async fn upload_file_to_dir<'r, R: Read>(
    parts: RequestParts<'r>,
    body: RequestBody<'r, R>,
    file_dir_name: &'static str,
    table_and_count_tracker_name: &'static str
) -> Result<(), &'static str> {
    #[cfg(feature = "embassy-mode")]
    let fman = get_file_manager().await;
    #[cfg(feature = "std-mode")]
    let fman = get_file_manager();

    let uploader_async = FileUploaderAsync { parts, body, file_dir_name, table_and_count_tracker_name };
    fman.with_root_dir_async(uploader_async).await.map_err(|_| "error while upload_file_to_dir")
}
