use crate::fs::{Mode, DbDir, PageFile};
use embedded_sdmmc::{BlockDevice, TimeSource, File, Directory, Mode as SdMode};

#[derive(Debug)]
pub struct DbDirSdmmc<
    'a, D, T,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize,
>
where
    D: BlockDevice,
    T: TimeSource,
{
    pub dir: Directory<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
}

#[derive(Debug)]
pub struct FileSdmmc<
    'a, D, T,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize,
>
where
    D: BlockDevice,
    T: TimeSource,
{
    pub file: File<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
}

impl <
    'a, D, T,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize,
> DbDirSdmmc<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
where
    D: BlockDevice,
    T: TimeSource,
{
    pub fn new(d: Directory<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>) -> Self {
        Self {
            dir: d
        }
    }
}

impl <
    'a, D, T,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize,
> FileSdmmc<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
where
    D: BlockDevice,
    T: TimeSource,
{
    pub fn new(f: File<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>) -> Self {
        Self {
            file: f
        }
    }
}

impl <
    'a, D, T,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize,
> PageFile for FileSdmmc<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
where
    D: BlockDevice,
    T: TimeSource,
{
    type Error = embedded_sdmmc::Error<D::Error>;

    fn seek_from_start(&self, offset: u32) -> Result<(), Self::Error> {
        self.file.seek_from_start(offset)
    }

    fn read(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.file.read(buf)
    }

    fn write(&self, buf: &[u8]) -> Result<(), Self::Error> {
        self.file.write(buf)
    }

    fn length(&self) -> u32 {
        self.file.length()
    }

    fn close(self) -> Result<(), Self::Error> {
        self.file.close()
    }

    fn flush(&self) -> Result<(), Self::Error> {
        self.file.flush()
    }
}

fn map_mode(m: Mode) -> SdMode {
    match m {
        Mode::ReadOnly => SdMode::ReadOnly,
        Mode::ReadWriteAppend => SdMode::ReadWriteAppend,
        Mode::ReadWriteTruncate => SdMode::ReadWriteTruncate,
        Mode::ReadWriteCreate => SdMode::ReadWriteCreate,
        Mode::ReadWriteCreateOrTruncate => SdMode::ReadWriteCreateOrTruncate,
        Mode::ReadWriteCreateOrAppend => SdMode::ReadWriteCreateOrAppend,
    }
}

impl <
    'a, D, T,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize,
> DbDir<'a> for DbDirSdmmc<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
where
    D: BlockDevice,
    T: TimeSource,
{
    type Error = embedded_sdmmc::Error<D::Error>;
    type File<'b> = FileSdmmc<'b, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES> where Self: 'b, Self: 'a;

    fn open_file_in_dir(&'a self, name: &'static str, mode: Mode) -> Result<Self::File<'a>, Self::Error> {
        Ok(FileSdmmc::new(
            self.dir.open_file_in_dir(name, map_mode(mode))?
        ))
    }

    fn delete_file_in_dir(&self, name: &'static str) -> Result<(), Self::Error> {
        self.dir.delete_file_in_dir(name)
    }
}
