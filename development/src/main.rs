#![allow(nonstandard_style)]
use alpa::embedded_sdmmc_ram_device::{allocators, block_device, esp_alloc, timesource};
use alpa::embedded_sdmmc_ram_device::fs::{DbDirSdmmc};
use alpa::{Column, ColumnType, Value, Row, Query, QueryExecutor};
use embedded_sdmmc::{VolumeManager};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    allocators::init_simulated_hardware();
    let sdcard = block_device::FsBlockDevice::new("test_file.db").unwrap();
    let vol_man = VolumeManager::new(sdcard, timesource::DummyTimesource);
    let volume = vol_man.open_volume(embedded_sdmmc::VolumeIdx(0)).unwrap();
    let root_dir = volume.open_root_dir().unwrap();
    let _ = root_dir.make_dir_in_dir("STUFF").unwrap();
    let stuff_dir = DbDirSdmmc::new(root_dir.open_dir("STUFF").unwrap());
    let mut db = alpa::db::Database::new_init(&stuff_dir, esp_alloc::ExternalMemory).unwrap();

    let allocator = esp_alloc::ExternalMemory;

    println!("stuff dir");
    {
        let path = Column::new("path", ColumnType::Chars).primary();
        let size = Column::new("size", ColumnType::Int);
        let name = Column::new("name", ColumnType::Chars);
        db.new_table_begin("files");
        db.add_column(path).unwrap();
        db.add_column(size).unwrap();
        db.add_column(name).unwrap();
        let _ = db.create_table(allocator.clone()).unwrap();
    }
    println!("created table");

    {
        let path = Column::new("cool_path", ColumnType::Chars).primary();
        db.new_table_begin("fav");
        db.add_column(path).unwrap();
        let fav = db.create_table(allocator.clone()).unwrap();
        let mut row = Row::new_in(allocator.clone());
        row.push(Value::Chars(b"/some/file.txt"));
        db.insert_to_table(fav, row, allocator.clone()).unwrap();
    }

    let files = db.get_table("files", allocator.clone()).unwrap();

    {
        use rand::{SeedableRng, seq::SliceRandom};
        use rand::rngs::StdRng;

        let to = 10;
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

        {
            let files = db.get_table("files", allocator.clone()).unwrap();
            let query = Query::<_, &str>::new(files, allocator.clone()).limit(2, 9);
            let mut exec = QueryExecutor::new(
                query, &mut db.table_buf, &mut db.buf1, &mut db.buf2, &db.file_handler.page_rw.as_ref().unwrap()
            ).unwrap();

            while let Ok(row) = exec.next() {
                println!("row = {:?}", row);
            }
        }

        let fav = db.get_table("fav", allocator.clone()).unwrap();

        db.delete_table(files, allocator.clone()).unwrap();
        db.delete_table(fav, allocator.clone()).unwrap();

        db.close(&stuff_dir).unwrap();
        println!("db closed successfully");
    }
}

