use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use parking_lot::{MappedRwLockWriteGuard, RwLock};

use crate::{
    data::{
        data_file::DataFile,
        log_record::{self, LogRecord, LogRecordType},
    },
    errors::{Errors, Result},
    options::Options,
};

pub struct Engine {
    options: Arc<Options>,
    // 当前活跃文件
    active_file: Arc<RwLock<DataFile>>,
    // 旧的数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
}

impl Engine {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // 判断key的有效性
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 构造logRecord结构体
        let log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };

        // 追加写入到活跃文件中

        Ok(())
    }

    // 追加数据到当前活跃文件中
    fn append_log_record(&self, record: &mut LogRecord) -> Result<()> {
        let dir_path = self.options.dir_path.clone();

        let enc_record = record.encode();
        let record_len = enc_record.len();

        // 当前活跃文件
        let mut active_file = self.active_file.write();
        // 判断当前写入文件是否达到阈值
        //* */ 可否将持久化后的当前活跃文件加入到旧的文件中？
        if active_file.get_write_off() + record_len as u64 > self.options.data_file_size {
            // 将当前文件持久化
            active_file.sync()?;

            let current_fid = active_file.get_file_id();
            // 将旧的数据文件存储到map中
            let mut older_files = self.older_files.write();
            let older_file = DataFile::new(dir_path.clone(), current_fid)?;
            older_files.insert(current_fid, older_file);

            // 打开新的数据文件
            let new_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
            *active_file = new_file;
        }

        Ok(())
    }
}
