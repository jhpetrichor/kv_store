use core::panic;

use prost::length_delimiter_len;

#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
    // pub(crate) size: u64,
}

/// LogRecord写入数据文件的记录
/// 之所以叫日志，是因为数据文件中的数据是以追加形式写入，类似与日志格式
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

#[derive(PartialEq, Eq)]
pub enum LogRecordType {
    NORMAL = 1,

    DELETED = 2,
}

impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            _ => panic!("Unknown log record type!"),
        }
    }
}

impl LogRecord {
    pub fn encode(&self) -> Vec<u8> {
        todo!()
    }

    pub fn get_crc(&mut self) -> u32 {
        todo!()
    }
}

/// 从数据文件中读取的log_record 信息
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

/// 获取Logrecord header部分的最大长度
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}
