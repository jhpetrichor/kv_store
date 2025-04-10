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
