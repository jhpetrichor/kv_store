mod file_io;

use crate::errors::Result;

pub trait IOManger: Sync + Send {
    // 从文件给定位置读取数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// 写入字节数组到文件中
    fn write(&self, buf: &[u8]) -> Result<usize>;

    /// 持久化数据
    fn sync(&self) -> Result<()>;
}
