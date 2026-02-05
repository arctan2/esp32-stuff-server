pub trait PageFile {
    type Error: core::fmt::Debug;

    fn seek_from_start(&self, offset: u32) -> Result<(), Self::Error>;
    fn read(&self, buf: &mut [u8]) -> Result<usize, Self::Error>;
    fn write(&self, buf: &[u8]) -> Result<(), Self::Error>;
    fn length(&self) -> u32;
    fn close(self) -> Result<(), Self::Error>;
    fn flush(&self) -> Result<(), Self::Error>;
}

pub enum Mode {
    ReadOnly,
    ReadWriteAppend,
    ReadWriteTruncate,
    ReadWriteCreate,
    ReadWriteCreateOrTruncate,
    ReadWriteCreateOrAppend,
}

pub trait DbDir<'a> {
    type Error: core::fmt::Debug;
    type File<'s>: PageFile<Error = Self::Error> where Self: 's, Self: 'a;

    fn open_file_in_dir(&'a self, name: &'static str, mode: Mode) -> Result<Self::File<'a>, Self::Error>;
    fn delete_file_in_dir(&self, name: &'static str) -> Result<(), Self::Error>;
}
