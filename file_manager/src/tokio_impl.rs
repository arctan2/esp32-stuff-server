use super::*;
use allocator_api2::alloc::{Allocator, AllocError, Layout};
use core::ptr::NonNull;
use alpa::embedded_sdmmc_ram_device::esp_alloc::ExternalMemory;
pub use alpa::embedded_sdmmc_ram_device::{
    allocators,
};
use alpa::embedded_sdmmc_fs::VM;
pub use alpa::embedded_sdmmc_ram_device::block_device::{FsBlockDeviceError, FsBlockDevice};
pub use std::sync::OnceLock;

pub struct EspAlloc(pub allocators::SimAllocator<23>);

impl EspAlloc {
    pub fn default() -> Self {
        Self(ExternalMemory)
    }
}

impl Clone for EspAlloc {
    fn clone(&self) -> Self {
        EspAlloc(ExternalMemory)
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

pub type BlkDev = FsBlockDevice;
pub type ExtAlloc = EspAlloc;
pub type FMan = FileManager<BlkDev, TimeSrc, 4, 4, 1>;
pub type FsError = FsBlockDeviceError;

#[derive(Debug)]
pub struct SyncFMan(pub FMan);

unsafe impl Send for SyncFMan {}
unsafe impl Sync for SyncFMan {}

pub static FILE_MAN: OnceLock<SyncFMan> = OnceLock::new();

pub fn init_file_manager(block_device: BlkDev, time_src: DummyTimesource) {
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
    FManErr(FManError<FsBlockDeviceError>),
    DbErr(alpa::db::Error<embedded_sdmmc::Error<FsError>>),
}

impl From<alpa::db::Error<embedded_sdmmc::Error<FsError>>> for InitError {
    fn from(e: alpa::db::Error<embedded_sdmmc::Error<FsError>>) -> Self {
        InitError::DbErr(e)
    }
}

impl From<embedded_sdmmc::Error<FsError>> for InitError {
    fn from(e: embedded_sdmmc::Error<FsError>) -> Self {
        InitError::SdCard(e)
    }
}

impl From<FManError<FsBlockDeviceError>> for InitError {
    fn from(e: FManError<FsBlockDeviceError>) -> Self {
        InitError::FManErr(e)
    }
}

pub async fn init_file_system(allocator: ExtAlloc) -> Result<(), InitError>
where 
    embedded_sdmmc::Error<<FsBlockDevice as BlockDevice>::Error>: Into<embedded_sdmmc::Error<FsError>>
{
    let fman = get_file_manager();
    fman.with_vol_man(|vm, vol| -> Result<(), FManError<FsBlockDeviceError>> {
        let root_dir = FileManager::<FsBlockDevice, DummyTimesource, 4, 4, 1>::root_dir(vm, vol)?.to_directory(vm);
        let _ = root_dir.make_dir_in_dir(consts::DB_DIR);
        let _ = root_dir.make_dir_in_dir(consts::FILES_DIR);
        let _ = root_dir.make_dir_in_dir(consts::MUSIC_DIR);

        {
            let db_dir = root_dir.open_dir(consts::DB_DIR)?;
            let db_dir = db_dir.to_raw_directory();
            let stuff_dir = DbDirSdmmc::new(db_dir);
            let mut db = Database::new_init(VM::new(vm), stuff_dir, allocator.clone())?;

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
