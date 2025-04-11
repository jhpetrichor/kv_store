use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    pub dir_path: PathBuf,

    pub data_file_size: u64,

    pub sync_write: bool,

    pub index_type: IndexType,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum IndexType {
    // BTree 索引
    BTree,

    // 跳表索引
    SkipList,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir(),
            data_file_size: 256 * 1024 * 1024,
            sync_write: false,
            index_type: IndexType::BTree,
        }
    }
}

/// 索引迭代器配置项
#[derive(Clone)]
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}

/// 批量写入数据配置项
pub struct WriteBatchOptions {
    // 最大暂存数据量
    pub max_batch_num: usize,
    // 持久化选项
    pub sync_writes: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 10000,
            sync_writes: true,
        }
    }
}
