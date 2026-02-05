#![allow(nonstandard_style)]
use db_engine::embedded_sdmmc_ram_device::{allocators, block_device, esp_alloc};
use db_engine::embedded_sdmmc_ram_device::fs::{DbDirSdmmc};
use db_engine::{Column, ColumnType, Value, Row, ToName, Query, QueryExecutor};
use embedded_sdmmc::{VolumeManager, TimeSource, Timestamp};

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
