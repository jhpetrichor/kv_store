pub mod btree;

use crate::{data::log_record::LogRecordPos, options::IndexType};

pub trait Indexer: Sync + Send {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    fn delete(&self, key: Vec<u8>) -> bool;
}

// 根据类型创建内存索引
pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
        IndexType::SkipList => todo!(),
    }
}
