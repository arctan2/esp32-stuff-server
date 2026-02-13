use super::*;
use esp_hal::{
    gpio::{Output},
    spi::{master::{Spi}},
    delay::{Delay},
};
use esp_hal::Blocking;
use embedded_hal_bus::spi::ExclusiveDevice;
use allocator_api2::alloc::{Allocator, AllocError, Layout};
use core::ptr::NonNull;
use esp_println::{println};
pub use embedded_sdmmc::{SdCard as FsBlockDevice, SdCardError};
pub use embassy_sync::once_lock::OnceLock;

pub struct EspAlloc(pub esp_alloc::ExternalMemory);

impl EspAlloc {
    pub fn default() -> Self {
        Self(esp_alloc::ExternalMemory)
    }
}

impl Clone for EspAlloc {
    fn clone(&self) -> Self {
        EspAlloc(esp_alloc::ExternalMemory)
    }
}

unsafe impl Allocator for EspAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.0.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            self.0.deallocate(ptr, layout)
        }
    }
}

pub type BlkDev<S, D> = FsBlockDevice<S, D>;
pub type ExtAlloc = EspAlloc;
pub type FMan<S, D> = FileManager<BlkDev<S, D>, TimeSrc, 4, 4, 1>;
pub type FsError = embedded_sdmmc::SdCardError;

pub type ConcreteSpi<'a> = ExclusiveDevice<Spi<'a, Blocking>, Output<'a>, Delay>;
pub type ConcreteDelay = Delay;

pub struct SyncFMan<'a>(pub FMan<ConcreteSpi<'a>, ConcreteDelay>);
unsafe impl <'a> Send for SyncFMan<'a> {}
unsafe impl <'a> Sync for SyncFMan<'a> {}

pub static FILE_MAN: OnceLock<SyncFMan> = OnceLock::new();

pub fn init_file_manager(block_device: BlkDev<ConcreteSpi<'static>, ConcreteDelay>, time_src: DummyTimesource)
{
    let _ = FILE_MAN.init(SyncFMan(FileManager::new(block_device, time_src)));
}

pub async fn get_file_manager() -> &'static FMan<ConcreteSpi<'static>, ConcreteDelay> {
    &FILE_MAN.get().await.0
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum InitError {
    SdCard(embedded_sdmmc::Error<FsError>),
    Database(alpa::db::Error<embedded_sdmmc::Error<FsError>>),
    FileMan(FManError<SdCardError>),
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

impl From<FManError<SdCardError>> for InitError {
    fn from(e: FManError<SdCardError>) -> Self {
        InitError::FileMan(e)
    }
}

pub async fn init_file_system(spi_device: ConcreteSpi<'static>, delay: ConcreteDelay, allocator: ExtAlloc) -> Result<(), InitError>
where 
    embedded_sdmmc::Error<<FsBlockDevice<ConcreteSpi<'static>, ConcreteDelay> as BlockDevice>::Error>: Into<embedded_sdmmc::Error<FsError>>
{
    let sdcard = BlkDev::new(spi_device, delay);
    init_file_manager(sdcard, DummyTimesource);

    let fman = get_file_manager().await;

    fman.with_vol_man(|vm, vol| {
        let root_dir = FileManager::<FsBlockDevice<ConcreteSpi<'static>, ConcreteDelay>, DummyTimesource, 4, 4, 1>
                                  ::root_dir(vm, vol)?
                                  .to_directory(vm);
        let _ = root_dir.make_dir_in_dir(consts::DB_DIR);
        let _ = root_dir.make_dir_in_dir(consts::FILES_DIR);
        let _ = root_dir.make_dir_in_dir(consts::MUSIC_DIR);

        {
            let db_dir = root_dir.open_dir(consts::DB_DIR)?;
            let stuff_dir = DbDirSdmmc::new(db_dir);
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

            Ok(())
        }
    }).await?;
    Ok(())
}
