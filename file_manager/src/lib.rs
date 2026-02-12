mod runtime;
pub mod consts;
use alpa::embedded_sdmmc_ram_device::{allocators, block_device, timesource, fs};
use crate::fs::{DbDirSdmmc};
use crate::block_device::{FsBlockDeviceError, FsBlockDevice};
use alpa::db::Database;
use alpa::{Column, ColumnType, Value, Row};
use std::sync::OnceLock;
pub use runtime::{Channel, Signal, Mutex};
use embedded_sdmmc::{
    BlockDevice,
    TimeSource,
    RawVolume,
    RawFile,
    RawDirectory,
    VolumeIdx,
    VolumeManager,
    Error,
    DirEntry,
    Mode
};

#[derive(Debug, Clone)]
pub enum FileType {
    File(DirEntry, RawFile),
    Dir(RawDirectory)
}

#[derive(Debug)]
pub enum CardState<
    D: BlockDevice, T: TimeSource,
    const MD: usize,
    const MF: usize,
    const MV: usize
> {
    NoCard { device: D, timer: T },
    Active { vm: VolumeManager<D, T, 4, 4, 1>, vol: RawVolume },
    Processing
}

#[derive(Debug)]
pub struct FileManagerState<
    D: BlockDevice, T: TimeSource,
    const MD: usize,
    const MF: usize,
    const MV: usize
> {
    pub card_state: CardState<D, T, MD, MF, MV>
}

impl <
    D: BlockDevice, T: TimeSource,
    const MD: usize,
    const MF: usize,
    const MV: usize
> FileManagerState<D, T, MD, MF, MV> {
    pub fn new(block_device: D, time_src: T) -> Self {
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
pub struct FileManager<
    D: BlockDevice, T: TimeSource,
    const MD: usize,
    const MF: usize,
    const MV: usize
> {
    pub state: Mutex<FileManagerState<D, T, MD, MF, MV>>,
}

impl <
    D: BlockDevice, T: TimeSource,
    const MD: usize,
    const MF: usize,
    const MV: usize
> FileManager<D, T, MD, MF, MV> {
    pub fn new(block_device: D, time_src: T) -> Self {
        let mut state = FileManagerState::new(block_device, time_src);
        state.try_mount();
        Self {
            state: Mutex::new(state)
        }
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

    pub async fn open_file<'a>(&self, dir: RawDirectory, name: &'a str, mode: Mode) -> Result<(DirEntry, RawFile), Error<D::Error>> {
        let state = self.state.lock().await;

        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
            match vm.find_directory_entry(dir, name) {
                Ok(entry) => {
                    if entry.attributes.is_directory() {
                        return Err(Error::BadHandle);
                    } else {
                        let file = vm.open_file_in_dir(dir, name, mode)?;
                        return Ok((entry, file));
                    }
                },
                Err(Error::NotFound) => {
                    let file = vm.open_file_in_dir(dir, name, mode)?;
                    vm.flush_file(file)?;
                    let entry = vm.find_directory_entry(dir, name)?;
                    return Ok((entry, file));
                },
                Err(e) => return Err(e)
            }
        }

        Err(Error::NotFound)
    }

    pub async fn close_file<'a>(&self, file: RawFile) -> Result<(), Error<D::Error>> {
        let state = self.state.lock().await;

        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
            return vm.close_file(file);
        }
        Err(Error::NotFound)
    }

    pub async fn mkdir<'a>(&self, dir: RawDirectory, name: &'a str) -> Result<(), Error<D::Error>> {
        let state = self.state.lock().await;

        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
            return vm.make_dir_in_dir(dir, name);
        }

        Err(Error::NotFound)
    }

    pub async fn open_dir<'a>(&self, dir: Option<RawDirectory>, name: &'a str) -> Result<RawDirectory, Error<D::Error>> {
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
                    return Err(e);
                }
            }
        }
        Err(Error::NotFound)
    }

    pub async fn close_dir<'a>(&self, dir: RawDirectory) -> Result<(), Error<D::Error>> {
        let state = self.state.lock().await;

        if let CardState::Active{ ref vm, vol: _ } = state.card_state {
            return vm.close_dir(dir);
        }
        Err(Error::NotFound)
    }

    pub async fn root_dir(&self) -> Result<RawDirectory, Error<D::Error>> {
        let state = self.state.lock().await;
        if let CardState::Active{ ref vm, ref vol } = state.card_state {
            return vm.open_root_dir(*vol);
        }
        Err(Error::NotFound)
    }

    pub async fn resolve_path_iter<'a>(&self, path: &'a str) -> Result<FileType, Error<D::Error>> {
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

            let mut ret: Result<FileType, Error<D::Error>> = Err(Error::NotFound);

            if let None = names.peek() {
                ret = match vm.find_directory_entry(cur_dir, last_name) {
                    Ok(entry) => {
                        if entry.attributes.is_directory() {
                            match vm.open_dir(cur_dir, last_name) {
                                Ok(dir) => Ok(FileType::Dir(dir)),
                                Err(e) => Err(e)
                            }
                        } else {
                            match vm.open_file_in_dir(cur_dir, last_name, Mode::ReadOnly) {
                                Ok(f) => Ok(FileType::File(entry, f)),
                                Err(e) => Err(e)
                            }
                        }
                    },
                    Err(e) => Err(e)
                };
            }

            let _ = vm.close_dir(cur_dir);
            return ret;
        }

        Err(Error::NotFound)
    }
}

pub type BlkDev = block_device::FsBlockDevice;
pub type ExtAlloc = allocators::SimAllocator<23>;
pub type TimeSrc = timesource::DummyTimesource;
pub type FMan = FileManager<BlkDev, TimeSrc, 4, 4, 1>;
pub type FsError = FsBlockDeviceError;

#[derive(Debug)]
pub struct SyncFMan(pub FMan);

unsafe impl Send for SyncFMan {}
unsafe impl Sync for SyncFMan {}

static FILE_MAN: OnceLock<SyncFMan> = OnceLock::new();

pub fn init_file_manager(block_device: BlkDev, time_src: timesource::DummyTimesource) {
    FILE_MAN.set(
        SyncFMan(FileManager::new(block_device, time_src))
    ).expect("initing twice file_manager");
}

pub fn get_file_manager() -> &'static FMan {
    &FILE_MAN.get().expect("file_manager not initialized").0
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum InitError {
    SdCard(embedded_sdmmc::Error<FsError>),
    Database(alpa::db::Error<embedded_sdmmc::Error<FsError>>),
}

impl From<alpa::db::Error<embedded_sdmmc::Error<FsError>>> for InitError {
    fn from(e: alpa::db::Error<embedded_sdmmc::Error<FsError>>) -> Self {
        InitError::Database(e)
    }
}

impl From<embedded_sdmmc::Error<FsError>> for InitError {
    fn from(e: embedded_sdmmc::Error<FsError>) -> Self {
        InitError::SdCard(e)
    }
}

pub async fn init_file_system(allocator: ExtAlloc) -> Result<(), InitError>
where 
    embedded_sdmmc::Error<<FsBlockDevice as BlockDevice>::Error>: Into<embedded_sdmmc::Error<FsError>>
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
