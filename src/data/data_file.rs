use std::{path::PathBuf, sync::Arc};

use parking_lot::RwLock;

use crate::{errors::Result, fio};

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

    pub fn sync(&self) -> Result<()> {
        // self.io_manger.sync();
        // Ok(())
        todo!()
    }
}
