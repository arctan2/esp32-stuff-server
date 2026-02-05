#![allow(nonstandard_style)]
mod block_device;
mod allocators;
use db_engine::fs::{Mode, DbDir, PageFile};
use db_engine::{Column, ColumnType, Value, Row, ToName, Query, QueryExecutor};
use allocators::{SimAllocator, INTERNAL_HEAP, PSRAM_HEAP};
use embedded_sdmmc::{VolumeManager, BlockDevice, TimeSource, Timestamp, File, Directory, Mode as SdMode};

pub mod esp_alloc {
    use super::*;
    pub const InternalMemory: SimAllocator<17> = SimAllocator(&INTERNAL_HEAP);
    pub const ExternalMemory: SimAllocator<23> = SimAllocator(&PSRAM_HEAP);
}

struct DbDirSdmmc<
    'a, D, T,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize,
>
where
    D: BlockDevice,
    T: TimeSource,
{
    dir: Directory<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
}

struct FileSdmmc<
    'a, D, T,
    const MAX_DIRS: usize,
    const MAX_FILES: usize,
    const MAX_VOLUMES: usize,
>
where
    D: BlockDevice,
    T: TimeSource,
{
    file: File<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>
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
    fn new(d: Directory<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>) -> Self {
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
    fn new(f: File<'a, D, T, MAX_DIRS, MAX_FILES, MAX_VOLUMES>) -> Self {
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

#[tokio::main(flavor = "current_thread")]
async fn main() {
    allocators::init_simulated_hardware();
    let sdcard = block_device::RamBlockDevice::new();
    let vol_man = VolumeManager::new(sdcard, DummyTimesource());
    let volume = vol_man.open_volume(embedded_sdmmc::VolumeIdx(0)).unwrap();
    let root_dir = volume.open_root_dir().unwrap();
    let _ = root_dir.make_dir_in_dir("STUFF").unwrap();
    let stuff_dir = DbDirSdmmc::new(root_dir.open_dir("STUFF").unwrap());
    let mut db = db_engine::db::Database::new_init(&stuff_dir, esp_alloc::ExternalMemory).unwrap();

    let allocator = esp_alloc::ExternalMemory;

    {
        let path = Column::new("path".to_name(), ColumnType::Chars).primary();
        let size = Column::new("size".to_name(), ColumnType::Int);
        let name = Column::new("name".to_name(), ColumnType::Chars);
        db.new_table_begin("files".to_name());
        db.add_column(path).unwrap();
        db.add_column(size).unwrap();
        db.add_column(name).unwrap();
        let _ = db.create_table(allocator.clone()).unwrap();
    }

    {
        let path = Column::new("cool_path".to_name(), ColumnType::Chars).primary();
        db.new_table_begin("fav".to_name());
        db.add_column(path).unwrap();
        let fav = db.create_table(allocator.clone()).unwrap();
        let mut row = Row::new_in(allocator.clone());
        row.push(Value::Chars(b"/some/file.txt"));
        db.insert_to_table(fav, row, allocator.clone()).unwrap();
    }

    let files = db.get_table("files".to_name(), allocator.clone()).unwrap();

    {
        use rand::{SeedableRng, seq::SliceRandom};
        use rand::rngs::StdRng;

        let to = 1000;
        let mut rng = StdRng::seed_from_u64(42);
        let mut ids: Vec<usize> = (0..to).collect();
        ids.shuffle(&mut rng);

        for i in ids.iter() {
            let path = format!("/some/file_{}.txt", i);
            let mut row = Row::new_in(allocator.clone());
            row.push(Value::Chars(path.as_bytes()));
            row.push(Value::Int(*i as i64));
            row.push(Value::Chars(b"file.txt"));
            db.insert_to_table(files, row, allocator.clone()).unwrap();
        }

        for i in 0..900 {
            let path = format!("/some/file_{}.txt", i);
            db.delete_from_table(files, Value::Chars(path.as_bytes()), allocator.clone()).unwrap();
        }

        {
            let files = db.get_table("files".to_name(), allocator.clone()).unwrap();
            let query = Query::new(files, allocator.clone());
            let mut exec = QueryExecutor::new(query, &mut db.table_buf, &mut db.buf1, &mut db.buf2, &db.file_handler.page_rw.as_ref().unwrap()).unwrap();

            while let Ok(row) = exec.next() {
                println!("row = {:?}", row);
            }
        }

        db.close(&stuff_dir).unwrap();
        println!("db closed successfully");
    }
}

#[derive(Default)]
pub struct DummyTimesource();

impl TimeSource for DummyTimesource {
    fn get_timestamp(&self) -> Timestamp {
        Timestamp {
            year_since_1970: 0,
            zero_indexed_month: 0,
            zero_indexed_day: 0,
            hours: 0,
            minutes: 0,
            seconds: 0,
        }
    }
}
