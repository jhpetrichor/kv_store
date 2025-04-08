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
