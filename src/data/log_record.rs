use core::panic;

use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
    // pub(crate) size: u64,
}

/// LogRecord写入数据文件的记录
/// 之所以叫日志，是因为数据文件中的数据是以追加形式写入，类似与日志格式
#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum LogRecordType {
    NORMAL = 1,

    DELETED = 2,

    //事务完成标识
    TXNFINISH = 3,
}

// LogRecordType::from_v8
impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            3 => LogRecordType::TXNFINISH,
            _ => panic!("Unknown log record type!"),
        }
    }
}

/// 暂存事务信息
pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecordPos,
}

//
// + -------- + --------- + --------- + --- + ----- + ------- +
// | type 类型 | key size  | value size| key | value | CrC校验值 |
// + -------- + --------- + --------- + --- + ----- + ------- +
// |    1字节  | 变长（最大5）| 变长（最大5） | 变长 |  变长  |   4字节   |
// + -------- + --------- + --------- + --- + ----- + ------- +
impl LogRecord {
    // encode 对logRecord 进行编码，，返回字节数组及其长度
    pub fn encode(&self) -> Vec<u8> {
        let (_, enc_buf) = self.encode_and_get_crc();
        enc_buf
    }

    pub fn get_crc(&mut self) -> u32 {
        let (crc, _) = self.encode_and_get_crc();
        crc
    }

    fn encode_and_get_crc(&self) -> (u32, Vec<u8>) {
        let mut buf = BytesMut::with_capacity(self.encoded_length());

        // 第一个字节存type类型
        buf.put_u8(self.rec_type as u8);
        // 在存储key和value的长度
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // 计算并存储CRC校验值
        let mut header = crc32fast::Hasher::new();
        header.update(&buf);
        let crc = header.finalize();
        buf.put_u32(crc);

        // println!("crc: {}", crc);

        (crc, buf.to_vec())
    }

    // 计算编码后长度
    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }
}

/// 从数据文件中读取的log_record 信息
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

/// 获取Logrecord header部分的最大长度
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode_crc() {
        // 对正常的 Logrecord 编码
        let rec1 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let (crc1, enc1) = rec1.encode_and_get_crc();
        assert!(crc1 == 1020360578);
        assert!(enc1.len() == 21);
        // println!("{}, {:?}", crc1, enc1);

        // Logrecord value为空
        let rec2 = LogRecord {
            key: "name1".as_bytes().to_vec(),
            value: Vec::default(),
            rec_type: LogRecordType::NORMAL,
        };
        let (crc2, enc2) = rec2.encode_and_get_crc();
        // println!("{}, {:?}", crc2, enc2);
        assert!(crc2 == 1467182769);
        assert!(enc2.len() == 12);

        // 类型为Deleted
        let rec3 = LogRecord {
            key: "name1".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::DELETED,
        };
        let (crc3, enc3) = rec3.encode_and_get_crc();
        // println!("{}, {:?}", crc3, enc3);
        assert!(crc3 == 243009088);
        assert!(enc3.len() == 22);
    }
}
