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

#[derive(Debug)]
pub enum FileType {
    File(RawFile),
    Dir(RawDirectory)
}

#[derive(Debug)]
pub enum Event<'a, D: BlockDevice> {
    OpenPath(String, &'a Signal<Result<FileType, Error<D::Error>>>),
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
    card_state: CardState<D, T, MD, MF, MV>
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
    'a, D: BlockDevice, T: TimeSource,
    const MD: usize,
    const MF: usize,
    const MV: usize
> {
    pub event_chan: Channel<Event<'a, D>, 2>,
    pub open_path_sig: Signal<Result<FileType, Error<D::Error>>>,
    state: Mutex<FileManagerState<D, T, MD, MF, MV>>
}

impl <
    'a, D: BlockDevice, T: TimeSource,
    const MD: usize,
    const MF: usize,
    const MV: usize
> FileManager<'a, D, T, MD, MF, MV> {
    pub fn new(block_device: D, time_src: T) -> Self {
        let mut state = FileManagerState::new(block_device, time_src);
        state.try_mount();
        Self {
            event_chan: Channel::new(),
            open_path_sig: Signal::new(),
            state: Mutex::new(state)
        }
    }

    pub async fn close_file(&self, file_type: FileType) {
        let state = self.state.lock().await;

        if let CardState::Active{ vm: ref vm, vol: ref vol } = state.card_state {
            match file_type {
                FileType::File(f) => {
                    let _ = vm.close_file(f).unwrap();
                },
                FileType::Dir(dir) => {
                    println!("file closed = {:?}", vm.close_dir(dir));
                }
            }
        }
    }

    async fn resolve_path_iter(&self, path: String) -> Result<FileType, Error<D::Error>> {
        let state = self.state.lock().await;

        if let CardState::Active{ vm: ref vm, vol: ref vol } = state.card_state {
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
                                Ok(f) => Ok(FileType::File(f)),
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

    pub async fn run(&self) {
        loop {
            match self.event_chan.recv().await {
                Event::OpenPath(path, sig) => sig.signal(self.resolve_path_iter(path).await).await
            }
        }
    }
}

