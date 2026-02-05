#![no_std]

#[cfg(feature = "std")]
pub mod embedded_sdmmc_ram_device;

pub mod db;
pub mod page_buf;
pub mod fs;
pub mod page_rw;
pub mod table;
pub mod serde_row;
pub mod query;

mod page_free_list;
mod macros;
mod btree;
mod overflow;
mod buffer;
mod file_handler;

pub use table::{Column, ColumnType, Name, ToName};
pub use query::{Query, QueryExecutor};
pub use serde_row::{Row, Value, Operations};
