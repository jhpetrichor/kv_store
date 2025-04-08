use std::{path::PathBuf, sync::Arc};

use parking_lot::RwLock;

use crate::{errors::Result, fio};

use super::log_record::ReadLogRecord;

pub const DATA_FILE_NAME_SUFFIX: &str = "./data";

pub struct DataFile {
    // 数据文件id
    file_id: Arc<RwLock<u32>>,
    // 当前写便宜，记录文件写到什么位置
    write_off: Arc<RwLock<u64>>,
    // IO 管理
    io_manger: Box<dyn fio::IOManger>,
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<Self> {
        todo!()
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read()
    }

    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        todo!()
    }

    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        // self.io_manger.sync();
        // Ok(())
        todo!()
    }
}
