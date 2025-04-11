pub mod btree;

use bytes::Bytes;

use crate::{
    data::log_record::LogRecordPos,
    errors::Result,
    options::{IndexType, IteratorOptions},
};

pub trait Indexer: Sync + Send {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    fn delete(&self, key: Vec<u8>) -> bool;

    fn iterator(&self, option: IteratorOptions) -> Box<dyn IndexerIterator>;

    fn list_keys(&self) -> Result<Vec<Bytes>>;
}

// 根据类型创建内存索引
pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
        IndexType::SkipList => todo!(),
    }
}

pub trait IndexerIterator: Sync + Send {
    // Rewind 从新回到迭代器的起点，即第一个数据
    fn rewind(&mut self);

    // Seek 根据传入的key 查找第一恶大于或小于等于的目标key，从这个key开始遍历
    fn seek(&mut self, key: Vec<u8>);

    // Next 跳转到下一个key，返回None则说明迭代完毕
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
