use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};

use crate::{
    data::log_record::{LogRecord, LogRecordType},
    db::Engine,
    errors::{Errors, Result},
    options::WriteBatchOptions,
};

pub(crate) const NON_TRANSACTION_SEQ_NO: usize = 0;
const TXN_FINISH: &[u8] = "txn-fin".as_bytes();

pub struct WriteBatch<'a> {
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>, // 暂存用户写入的数据
    engine: &'a Engine,

    options: WriteBatchOptions,
}

impl Engine {
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        Ok(WriteBatch {
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options,
        })
    }
}

impl WriteBatch<'_> {
    // 批量操作写数据
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 暂存数据
        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };

        let mut pending_writes = self.pending_writes.lock();
        pending_writes.insert(key.to_vec(), record);

        Ok(())
    }

    // 批量删除数据
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(crate::errors::Errors::KeyIsEmpty);
        }

        let mut pending_writes = self.pending_writes.lock();
        // 如果数据不存在则直接返回
        let index_pos = self.engine.index.get(key.to_vec());
        if index_pos.is_none() {
            if pending_writes.contains_key(&key.to_vec()) {
                pending_writes.remove(&key.to_vec());
            }
        }
        // 暂存数据
        let record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETED,
        };
        pending_writes.insert(key.to_vec(), record);
        Ok(())
    }

    // 提交数据，将数据写入到文件中，并更新内存索引
    pub fn commit(&self) -> Result<()> {
        let mut pending_writes = self.pending_writes.lock();
        if pending_writes.len() == 0 {
            return Ok(());
        }

        if pending_writes.len() > self.options.max_batch_num {
            return Err(Errors::ExceddMaxBatchNum);
        }

        // 加锁保证事务提交串行化
        let _lock = self.engine.batch_commit_lock.lock();
        // 获取全局事务序列号
        let seq_no = self.engine.seq_no.fetch_add(1, Ordering::SeqCst);

        let mut positions = HashMap::new();
        // 开始写数据到数据文件中
        for (_, item) in pending_writes.iter() {
            let mut record = LogRecord {
                key: log_record_key_with_seq(item.key.to_vec(), seq_no),
                value: item.value.clone(),
                rec_type: item.rec_type,
            };
            let pos = self.engine.append_log_record(&mut record)?;
            positions.insert(item.key.clone(), pos);
        }

        // 写入最后一条事务完成的数据
        let mut finish_record = LogRecord {
            key: log_record_key_with_seq(TXN_FINISH.to_vec(), seq_no),
            value: Default::default(),
            rec_type: LogRecordType::TXNFINISH,
        };
        self.engine.append_log_record(&mut finish_record)?;

        // 持久化
        if self.options.sync_writes {
            self.engine.sync()?;
        }

        // 数据全部写完之后更新内存索引
        for (_, item) in pending_writes.iter() {
            let reord_pos = positions.get(&item.key).unwrap();
            if item.rec_type == LogRecordType::NORMAL {
                self.engine.index.put(item.key.clone(), *reord_pos);
            }
            if item.rec_type == LogRecordType::DELETED {
                self.engine.index.delete(item.key.clone());
            }
        }
        //清空暂存数据
        pending_writes.clear();
        Ok(())
    }
}

// 编码seq no 和 key
pub(crate) fn log_record_key_with_seq(key: Vec<u8>, seq_no: usize) -> Vec<u8> {
    let mut enc_key = BytesMut::new();
    encode_length_delimiter(seq_no, &mut enc_key).unwrap();
    enc_key.extend_from_slice(&key.to_vec());
    enc_key.to_vec()
}

// 解析LogRecord的key，拿到实际的key和seq no
pub(crate) fn parse_log_record_key(key: Vec<u8>) -> (Vec<u8>, usize) {
    let mut buf = BytesMut::new();
    buf.put_slice(&key);

    let seq_no = decode_length_delimiter(&mut buf).unwrap();

    (buf.to_vec(), seq_no)
}

#[cfg(test)]
mod tests {
    use std::{collections::btree_map::Keys, fs};

    use crate::{
        options::Options,
        utils::{
            self,
            rand_kv::{get_test_key, get_test_value},
        },
    };

    use super::*;

    #[test]
    fn test_write_batch_1() {
        let mut opts = Options::default();
        opts.dir_path = "/tmp/bitcask-rs-batch-1".parse().unwrap();
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("Failed to open engine");

        let wb = engine.new_write_batch(Default::default()).unwrap();
        // 写数据之前未提交
        let put_res1 = wb.put(utils::rand_kv::get_test_key(1), get_test_value(1));
        assert!(put_res1.is_ok());
        let put_res1 = wb.put(utils::rand_kv::get_test_key(2), get_test_value(11));
        assert!(put_res1.is_ok());
        let put_res1 = wb.put(utils::rand_kv::get_test_key(2), get_test_value(111));
        assert!(put_res1.is_ok());

        let res1 = engine.get(get_test_key(1));
        // println!("{:?}", res1);
        assert!(res1.is_err());
        assert!(res1 == Err(Errors::KeyNotFound));

        // 提交事务之后查询
        let commit_res = wb.commit();
        assert!(commit_res.is_ok());
        let res1 = engine.get(get_test_key(1));
        println!("{:?}", res1.is_ok());

        // 验证事务序列号
        let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
        // println!("{}", seq_no);
        assert_eq!(seq_no, 2);

        fs::remove_dir_all(opts.dir_path.clone()).unwrap();
    }

    #[test]
    fn test_write_batch_2() {
        let mut opts = Options::default();
        opts.dir_path = "/tmp/bitcask-rs-batch-2".parse().unwrap();
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("Failed to open engine");

        let wb = engine.new_write_batch(Default::default()).unwrap();
        // 写数据之前未提交
        let put_res1 = wb.put(utils::rand_kv::get_test_key(1), get_test_value(1));
        assert!(put_res1.is_ok());
        let put_res1 = wb.put(utils::rand_kv::get_test_key(2), get_test_value(11));
        assert!(put_res1.is_ok());
        let put_res1 = wb.put(utils::rand_kv::get_test_key(3), get_test_value(111));
        assert!(put_res1.is_ok());

        let commit_res = wb.commit();
        assert!(commit_res.is_ok());
        let res1 = engine.get(get_test_key(1));
        println!("{:?}", res1.is_ok());

        let put_res1 = wb.put(utils::rand_kv::get_test_key(4), get_test_value(111));
        assert!(put_res1.is_ok());

        let commit_res = wb.commit();
        assert!(commit_res.is_ok());
        let res1 = engine.get(get_test_key(1));
        println!("{:?}", res1.is_ok());

        // 重启之后进行验证
        engine.close().expect("failed to close");
        let engine2 = Engine::open(opts.clone()).expect("Failed to open engine");
        let keys = engine2.list_keys();
        assert!(keys.is_ok());
        let keys = keys.unwrap();
        // println!("{:?}", keys);
        assert_eq!(3, keys.len());

        let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
        // println!("{}", seq_no);
        assert_eq!(seq_no, 3);

        fs::remove_dir_all(opts.dir_path.clone()).unwrap();
    }

    
    #[test]
    fn test_write_batch_3() {
        let mut opts = Options::default();
        opts.dir_path = "/tmp/bitcask-rs-batch-3".parse().unwrap();
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("Failed to open engine");
        
        let keys = engine.list_keys();
        println!("{:?}", keys);


        // let mut wb_opts = WriteBatchOptions::default();
        // wb_opts.max_batch_num = 10000000;
        // let wb  = engine.new_write_batch(wb_opts).unwrap();

        // for i in 0..=1000000 {
        //     let put_res = 
        //     wb.put(get_test_key(i), get_test_value(i));
        //     assert!(put_res.is_ok());
        // }

        // wb.commit();
    }
}
