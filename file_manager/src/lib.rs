mod runtime;
pub use runtime::{Channel, Signal, Mutex};
use embedded_sdmmc::{
    BlockDevice,
    TimeSource,
    File,
    Directory,
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
            let mut vm = VolumeManager::new(device, timer);
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
            let root_dir =  vm.open_root_dir(*vol)?;
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
                                Err(e) => {
                                    break;
                                }
                            }
                            let _ = vm.close_dir(prev_dir);
                        } else {
                            break;
                        }
                    },
                    Err(e) => {
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

