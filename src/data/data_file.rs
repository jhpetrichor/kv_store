use std::{path::PathBuf, sync::Arc};

use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::errors::Errors;
use crate::{
    data::log_record::{max_log_record_header_size, LogRecord, LogRecordType},
    errors::Result,
    fio::{self, new_io_manager},
};

use super::log_record::ReadLogRecord;

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

pub struct DataFile {
    // 数据文件id
    pub(crate) file_id: Arc<RwLock<u32>>,
    // 当前写便宜，记录文件写到什么位置
    pub(crate) write_off: Arc<RwLock<u64>>,
    // IO 管理
    pub(crate) io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<Self> {
        // 根据path和id构造出完整的文件名称
        let file_name = get_data_file_name(&dir_path, file_id);
        // 初始化 io manager
        let io_manager = new_io_manager(&file_name)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read()
    }

    /// 根据 offet 从数据文件中读取Logrecord
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());

        self.io_manager.read(&mut header_buf, offset)?;

        // 取出 type，在第一字节
        let rec_type = header_buf.get_u8();
        // 取出key和value的长度
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(header_buf).unwrap();

        // 如果key和value的长度都为0，则表示读取到文件末尾
        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadDataFileEOF);
        }

        // key 和value 有值，则读取header实际的长度,1为校验位的值
        let actual_header_size =
            length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;

        let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4);
        self.io_manager
            .read(&mut kv_buf, offset + actual_header_size as u64)?;

        // 构造LogRecord
        let mut log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
            rec_type: LogRecordType::from_u8(rec_type),
        };

        // 向前移动到最后四个字节，就是crc值 拿到校验值
        kv_buf.advance(key_size + value_size);
        if kv_buf.get_u32() != log_record.get_crc() {
            return Err(Errors::InvalidLogRecordCrc);
        }
        // 构造结果并返回
        Ok(ReadLogRecord {
            record: log_record,
            size: actual_header_size + key_size + value_size + 4,
        })
    }

    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;
        let mut write_guard = self.write_off.write();
        *write_guard += n_bytes as u64;

        Ok(n_bytes)
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
}

fn get_data_file_name(path: &PathBuf, file_id: u32) -> PathBuf {
    // 文件名
    PathBuf::from(format!(
        "{}{:09}{}",
        path.to_str().unwrap(),
        file_id,
        DATA_FILE_NAME_SUFFIX
    ))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::DataFile;


    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();
        let data_file = DataFile::new(dir_path, 9090);
        println!("{:?}", data_file.err());
        // assert!(data_file.is_ok());

        // let data_file = data_file.unwrap();
        // assert_eq!(data_file.get_file_id(), 0);

    }   
}