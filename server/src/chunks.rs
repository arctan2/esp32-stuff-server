#![allow(unused)]
use file_manager::{get_file_manager, ExtAlloc, AsyncRootFn, FManError};
use allocator_api2::boxed::Box;
use allocator_api2::vec::Vec;
use file_manager::runtime::{Sender, Receiver, Channel, Signal, Mutex};
use file_manager::{BlkDev, DummyTimesource, FsBlockDevice};
use embedded_sdmmc::{RawFile, VolumeManager, BlockDevice, TimeSource, RawDirectory, Mode};
use crate::String;

#[cfg(feature = "std-mode")]
pub use tokio::select;
#[cfg(feature = "embassy-mode")]
pub use embassy_futures::select;

#[cfg(feature = "std-mode")]
pub use std::sync::OnceLock;
#[cfg(feature = "embassy-mode")]
pub use embassy_sync::once_lock::OnceLock;

const CHUNK_SIZE: usize = 1024;

#[derive(Debug)]
pub struct Chunk {
    pub len: usize,
    pub buf: [u8; CHUNK_SIZE]
}

pub const CHAN_CAP: usize = 128;

pub static FREE_CHAN: OnceLock<Channel<Box<Chunk, ExtAlloc>, CHAN_CAP>> = OnceLock::new();
pub static READY_SENDER: OnceLock<Sender<Box<Chunk, ExtAlloc>, CHAN_CAP>> = OnceLock::new();

pub fn init_chunks(free_chan: Channel<Box<Chunk, ExtAlloc>, CHAN_CAP>, ready: Sender<Box<Chunk, ExtAlloc>, CHAN_CAP>) {
    FREE_CHAN.set(free_chan).expect("initing free twice sender");
    READY_SENDER.set(ready).expect("initing ready twice recvr");
}

pub fn get_free_chan() -> &'static Channel<Box<Chunk, ExtAlloc>, CHAN_CAP> {
    FREE_CHAN.get().expect("get_free_chan not initialized")
}

pub fn get_ready_sender() -> Sender<Box<Chunk, ExtAlloc>, CHAN_CAP> {
    READY_SENDER.get().expect("get_free_sender not initialized").clone()
}

#[derive(Debug)]
pub struct DangerousVMPtr<D: BlockDevice, T: TimeSource>(pub *const VolumeManager<D, T, 4, 4, 1>);

unsafe impl <D: BlockDevice, T: TimeSource> Send for DangerousVMPtr<D, T>{}

#[derive(Debug)]
pub enum UploadEvent<D: BlockDevice, T: TimeSource> {
    Begin(
        RawDirectory,
        String,
        DangerousVMPtr<D, T>, Vec<u8, ExtAlloc>,
    ),
    EndOfUpload,
    ReadErr
}

static EVENT_SIG: OnceLock<Signal<UploadEvent<BlkDev, DummyTimesource>>> = OnceLock::new();
static RET_SIG: OnceLock<Signal<Result<bool, &'static str>>> = OnceLock::new();

pub fn init_signals() {
    EVENT_SIG.set(Signal::new()).unwrap();
    RET_SIG.set(Signal::new()).unwrap();
}

pub fn get_event_sig() -> &'static Signal<UploadEvent<BlkDev, DummyTimesource>> {
    EVENT_SIG.get().unwrap()
}

pub async fn send_event_sig(msg: UploadEvent<BlkDev, DummyTimesource>) {
    let sig = EVENT_SIG.get().unwrap();
    sig.reset();
    sig.signal(msg).await;
}

pub fn get_ret_sig() -> &'static Signal<Result<bool, &'static str>> {
    RET_SIG.get().unwrap()
}

pub async fn send_ret_sig(msg: Result<bool, &'static str>) {
    let sig = RET_SIG.get().unwrap();
    sig.reset();
    sig.signal(msg).await;
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

#[cfg_attr(feature = "embassy-mode", embassy_executor::task(pool_size = 1))]
pub async fn task_file_uploader() {
    let mut free_chan: Channel<Box<Chunk, ExtAlloc>, CHAN_CAP> = Channel::new();
    let mut ready_chan: Channel<Box<Chunk, ExtAlloc>, CHAN_CAP> = Channel::new();

    let ready_sender = ready_chan.sender();
    let ready_receiver = ready_chan.receiver();

    init_chunks(free_chan, ready_sender);

    let free_chan = get_free_chan();

    for i in 0..CHAN_CAP {
        let chunk = Box::new_in(Chunk{ len: 0, buf: [0; CHUNK_SIZE] }, ExtAlloc::default());
        free_chan.send(chunk).await;
    }

    loop {
        let event = get_event_sig().wait().await;
        match event {
            UploadEvent::Begin(files_dir, actual_name, dangerous_vm_ptr, boundary) => {
                handle_begin(&ready_receiver, files_dir, actual_name, dangerous_vm_ptr, boundary).await;
            }
            _ => ()
        }
    }
}

async fn handle_begin<D: BlockDevice, T: TimeSource>(
    ready_receiver: &Receiver<Box<Chunk, ExtAlloc>, CHAN_CAP>,
    files_dir: RawDirectory,
    actual_name: String,
    vm_ptr: DangerousVMPtr<D, T>,
    boundary: Vec<u8, ExtAlloc>,
) {
    let vm = unsafe { &*(vm_ptr.0 as *const VolumeManager<BlkDev, DummyTimesource, 4, 4, 1>) };
    let files_dir = files_dir.to_directory(vm);
    let new_file = match files_dir.open_file_in_dir(str::from_utf8(actual_name.as_bytes()).unwrap(), Mode::ReadWriteCreate) {
        Ok(f) => f,
        Err(_) => {
            send_ret_sig(Err("unable to create file")).await;
            return;
        }
    };
    let mut filename = [0u8; 128];
    let mut filename_len = 0;

    let mut step = Step::FindFilename;
    let mut pattern_idx = 0;

    let mut lookback_buf = [0u8; 128];
    let mut lookback_len = 0;
    let mut file_size: i64 = 0;

    // 'stream: while let Ok(n) = reader.read(&mut buffer).await {
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

    //                 if let Some(pos) = find_boundary_across_buffers(&lookback_buf[..lookback_len], data_chunk, boundary) {
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
    loop {
        select!(
            chunk = ready_receiver.recv() => {
                std::println!("chunk = {:?}", chunk);
            }
            event = get_event_sig().wait() => {
                match event {
                    UploadEvent::ReadErr => {
                        ()
                    }
                    UploadEvent::EndOfUpload => {
                        ()
                    }
                    _ => ()
                }
            }
        );
    }
}

pub async fn init_all() {
    init_signals();
}
