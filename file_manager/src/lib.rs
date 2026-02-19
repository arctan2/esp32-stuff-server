#![no_std]

pub mod consts;
pub mod runtime;

use alpa::embedded_sdmmc_fs::{DbDirSdmmc};
use alpa::db::Database;
use alpa::{Column, ColumnType, Value, Row};
pub use runtime::{Mutex};
use embedded_sdmmc::{
    BlockDevice,
    TimeSource,
    RawVolume,
    RawFile,
    RawDirectory,
    VolumeIdx,
    VolumeManager,
    DirEntry,
    Mode,
    Error
};

#[cfg(feature = "tokio")]
pub extern crate std;

#[cfg(feature = "tokio")]
mod tokio_impl;
#[cfg(feature = "tokio")]
pub use tokio_impl::*;

#[cfg(feature = "embassy")]
mod embassy_impl;
#[cfg(feature = "embassy")]
pub use embassy_impl::*;

#[derive(Default, Debug)]
pub struct DummyTimesource;

impl embedded_sdmmc::TimeSource for DummyTimesource {
    fn get_timestamp(&self) -> embedded_sdmmc::Timestamp {
        embedded_sdmmc::Timestamp {
            year_since_1970: 0,
            zero_indexed_month: 0,
            zero_indexed_day: 0,
            hours: 0,
            minutes: 0,
            seconds: 0,
        }
    }
}

pub type TimeSrc = DummyTimesource;

#[derive(Debug, Clone)]
pub enum FileType {
    File(DirEntry, RawFile),
    Dir(RawDirectory)
}

#[derive(Debug)]
pub enum CardState {
    NoCard { device: BlkDev, timer: DummyTimesource },
    Active { vm: VolumeManager<BlkDev, DummyTimesource, 4, 4, 1>, vol: RawVolume },
    Processing
}

#[derive(Debug)]
pub struct FileManagerState {
    pub card_state: CardState
}

impl FileManagerState {
    pub fn new(block_device: BlkDev, time_src: DummyTimesource) -> Self {
        Self {
            card_state: CardState::NoCard{ device: block_device, timer: time_src }
        }
    }

    pub fn try_mount(&mut self) {
        if let CardState::NoCard { device, timer } = core::mem::replace(&mut self.card_state, CardState::Processing) {
            let vm = VolumeManager::new(device, timer);
            self.card_state = match vm.open_raw_volume(VolumeIdx(0)) {
                Ok(vol) => CardState::Active{ vm, vol },
                Err(_) => {
                    let (device, timer) = vm.free();
                    CardState::NoCard { device, timer }
                }
            }
        }
    }

    pub fn handle_ejection(&mut self) {
        if let CardState::Active{ vm, vol: _ } = core::mem::replace(&mut self.card_state, CardState::Processing) {
             let (device, timer) = vm.free();
             self.card_state = CardState::NoCard { device, timer };
        }
    }
}

#[derive(Debug)]
pub struct FileManager {
    pub state: Mutex<FileManagerState>,
}

#[derive(Debug)]
pub enum FManError<E: core::fmt::Debug> {
    SdErr(embedded_sdmmc::Error<E>),
    DbErr(alpa::db::Error<embedded_sdmmc::Error<E>>),
    ServerErr(&'static str),
    CardNotActive,
    IsDir
}

impl<E: core::fmt::Debug> From<alpa::db::Error<embedded_sdmmc::Error<E>>> for FManError<E> {
    fn from(e: alpa::db::Error<embedded_sdmmc::Error<E>>) -> Self {
        FManError::DbErr(e)
    }
}

impl<E: core::fmt::Debug> From<embedded_sdmmc::Error<E>> for FManError<E> {
    fn from(e: embedded_sdmmc::Error<E>) -> Self {
        FManError::SdErr(e)
    }
}

impl<E: core::fmt::Debug> From<&'static str> for FManError<E> {
    fn from(e: &'static str) -> Self {
        FManError::ServerErr(e)
    }
}

pub trait AsyncRootFn<R> {
    type Fut<'a>: core::future::Future<Output = Result<R, FManError<<FsBlockDevice as BlockDevice>::Error>>> + 'a 
    where Self: 'a;
    fn call<'a>(self, dir: RawDirectory, vm: &'a VolumeManager<BlkDev, DummyTimesource, 4, 4, 1>) -> Self::Fut<'a>;
}

impl FileManager {
    pub fn new(block_device: BlkDev, time_src: DummyTimesource) -> Self {
        let mut state = FileManagerState::new(block_device, time_src);
        state.try_mount();
        Self {
            state: Mutex::new(state)
        }
    }

    pub async fn is_card_active(&self) -> bool {
        let state = self.state.lock().await;
        matches!(state.card_state, CardState::Active { .. })
    }

    pub async fn try_mount(&self) {
        let mut state = self.state.lock().await;
        state.try_mount();
    }

    pub async fn close_file_type(&self, file_type: FileType) {
        let state = self.state.lock().await;

        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
            match file_type {
                FileType::File(_, f) => {
                    let _ = vm.close_file(f);
                },
                FileType::Dir(dir) => {
                    let _ = vm.close_dir(dir);
                }
            }
        }
    }

    pub async fn open_dir<'a>(&self, dir: Option<RawDirectory>, name: &'a str)
        -> Result<RawDirectory, FManError<<FsBlockDevice as BlockDevice>::Error>>
    {
        let state = self.state.lock().await;

        if let CardState::Active{ ref vm, ref vol } = state.card_state {
            let root_dir = dir.unwrap_or(vm.open_root_dir(*vol)?);
            match vm.open_dir(root_dir, name) {
                Ok(dir) => {
                    let _ = vm.close_dir(root_dir);
                    return Ok(dir);
                },
                Err(e) => {
                    let _ = vm.close_dir(root_dir);
                    return Err(FManError::SdErr(e));
                }
            }
        }
        Err(FManError::CardNotActive)
    }

    pub async fn close_dir<'a>(&self, dir: RawDirectory)
        -> Result<(), FManError<<FsBlockDevice as BlockDevice>::Error>>
    {
        let state = self.state.lock().await;

        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
            return Ok(vm.close_dir(dir)?);
        }
        Err(FManError::CardNotActive)
    }

    pub fn root_dir(vm: &VolumeManager<BlkDev, DummyTimesource, 4, 4, 1>, vol: &RawVolume)
        -> Result<RawDirectory, FManError<<FsBlockDevice as BlockDevice>::Error>>
    {
        Ok(vm.open_root_dir(*vol)?)
    }

    pub async fn root_dir_lock(&self) -> Result<RawDirectory, FManError<<FsBlockDevice as BlockDevice>::Error>> {
        let state = self.state.lock().await;
        if let CardState::Active{ ref vm, ref vol } = state.card_state {
            return Self::root_dir(vm, vol);
        }
        Err(FManError::CardNotActive)
    }

    pub async fn with_vol_man<F, R>(&self, f: F) -> Result<R, FManError<<FsBlockDevice as BlockDevice>::Error>>
    where
        F: FnOnce(&VolumeManager<BlkDev, DummyTimesource, 4, 4, 1>, &RawVolume) -> Result<R, FManError<<FsBlockDevice as BlockDevice>::Error>>,
    {
        let state = self.state.lock().await;
        if let CardState::Active{ ref vm, ref vol } = state.card_state {
            return f(vm, vol);
        }
        Err(FManError::CardNotActive)
    }

    pub async fn with_root_dir<F, R>(&self, f: F) -> Result<R, FManError<<FsBlockDevice as BlockDevice>::Error>>
    where
        F: FnOnce(RawDirectory) -> Result<R, FManError<<FsBlockDevice as BlockDevice>::Error>>,
    {
        let state = self.state.lock().await;
        if let CardState::Active{ ref vm, ref vol } = state.card_state {
            return f(Self::root_dir(vm, vol)?);
        }
        Err(FManError::CardNotActive)
    }

    pub async fn with_root_dir_async<F, R>(&self, f: F) -> Result<R, FManError<<FsBlockDevice as BlockDevice>::Error>>
    where
        F: AsyncRootFn<R>,
    {
        let state = self.state.lock().await;
        if let CardState::Active { ref vm, ref vol } = state.card_state {
            let root = Self::root_dir(vm, vol)?;
            return f.call(root, vm).await;
        }
        Err(FManError::CardNotActive)
    }

    pub async fn resolve_path_iter<'a>(&self, path: &'a str) -> Result<FileType, FManError<<FsBlockDevice as BlockDevice>::Error>> {
        let state = self.state.lock().await;

        if let CardState::Active{ ref vm, ref vol } = state.card_state {
            let mut cur_dir = vm.open_root_dir(*vol)?;

            let path = path.trim_matches('/');
            let mut names = path.split("/").peekable();

            if path == "" {
                let _ = names.next();
            }

            if let None = names.peek() {
                return Ok(FileType::Dir(cur_dir));
            }

            let mut prev_name: Option<&str> = None;
            
            while let Some(name) = names.next() {
                prev_name = Some(name);
                if let None = names.peek() {
                    break;
                }

                match vm.find_directory_entry(cur_dir, name) {
                    Ok(entry) => {
                        if entry.attributes.is_directory() {
                            let prev_dir = cur_dir;
                            match vm.open_dir(cur_dir, name) {
                                Ok(dir) => cur_dir = dir,
                                Err(_) => {
                                    break;
                                }
                            }
                            let _ = vm.close_dir(prev_dir);
                        } else {
                            break;
                        }
                    },
                    Err(_) => {
                        break;
                    }
                }
            }

            let last_name = prev_name.unwrap();

            let mut ret: Result<FileType, FManError<<FsBlockDevice as BlockDevice>::Error>> = Err(FManError::SdErr(Error::NotFound));

            if let None = names.peek() {
                ret = match vm.find_directory_entry(cur_dir, last_name) {
                    Ok(entry) => {
                        if entry.attributes.is_directory() {
                            match vm.open_dir(cur_dir, last_name) {
                                Ok(dir) => Ok(FileType::Dir(dir)),
                                Err(e) => Err(FManError::SdErr(e))
                            }
                        } else {
                            match vm.open_file_in_dir(cur_dir, last_name, Mode::ReadOnly) {
                                Ok(f) => Ok(FileType::File(entry, f)),
                                Err(e) => Err(FManError::SdErr(e))
                            }
                        }
                    },
                    Err(e) => Err(FManError::SdErr(e))
                };
            }

            let _ = vm.close_dir(cur_dir);
            return ret;
        }

        Err(FManError::CardNotActive)
    }
}

