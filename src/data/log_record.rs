#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
    pub(crate) size: u64,
}

/// LogRecord写入数据文件的记录
/// 之所以叫日志，是因为数据文件中的数据是以追加形式写入，类似与日志格式
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

pub enum LogRecordType {
    NORMAL = 1,

    DELETED = 2,
}

impl LogRecord {
    pub fn encode(&self) -> Vec<u8> {
        todo!()
    }
}
