use alpa::embedded_sdmmc_ram_device::{esp_alloc};
use alpa::embedded_sdmmc_fs::{DbDirSdmmc};
use alpa::db::Database;
use alpa::{Value, Row, Query, QueryExecutor};
use embedded_sdmmc::{Mode};
use picoserve::request::{RequestBody, RequestParts};
use picoserve::io::Read;
use file_manager::{CardState};
use file_manager::{get_file_manager};
use crate::consts;
use alloc::format;

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

pub async fn upload_file_to_dir<'r, R: Read>(
    parts: RequestParts<'r>,
    body: RequestBody<'r, R>,
    file_dir_name: &'static str,
    table_and_count_tracker_name: &'static str
) -> Result<(), &'static str> {
    let table_and_count_tracker_name = table_and_count_tracker_name.as_bytes();

    #[cfg(feature = "embassy-mode")]
    let fman = get_file_manager().await;
    #[cfg(feature = "std-mode")]
    let fman = get_file_manager();

    let db_dir = fman.open_dir(None, consts::DB_DIR).await.map_err(|_| "unable to open db")?;

    let state = fman.state.lock().await;

    if let CardState::Active{ ref vm, ref vol } = state.card_state {
        let db_dir = DbDirSdmmc::new(db_dir.to_directory(vm));
        let mut db = match Database::new_init(&db_dir, esp_alloc::ExternalMemory) {
            Ok(d) => d,
            Err(_) => return Err("db init error".into())
        };
        let count_tracker_table = db.get_table(consts::COUNT_TRACKER_TABLE, esp_alloc::ExternalMemory)
                                    .map_err(|_| "unable to get count_tracker table")?;
        let files_table = db.get_table(table_and_count_tracker_name, esp_alloc::ExternalMemory)
                            .map_err(|_| "unable to get files table")?;
        let cur_file_id: i64;

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
                Err(_) => {
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
