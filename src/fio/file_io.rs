use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::Arc,
};

use log::error;
use parking_lot::RwLock;

use crate::errors::{Errors, Result};

use super::IOManager;

/// FileIO 标准系统文件IO
pub struct FileIO {
    /// 系统文件描述符
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    // pub fn new(file_name: &PathBuf) -> Result<Self> {
    //     match OpenOptions::new()
    //         .create(true)
    //         .read(true)
    //         .append(true) // 或者 append(true)
    //         .open(file_name)
    //     {
    //         Ok(file) => Ok(Self {
    //             fd: Arc::new(RwLock::new(file)),
    //         }),
    //         Err(e) => {
    //             error!(
    //                 "Failed to open file: {:?}, error kind: {:?}, error msg: {}",
    //                 file_name,
    //                 e.kind(),
    //                 e.to_string()
    //             );
    //         }
    //     }
    // }
    pub fn new(file_name: &PathBuf) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            // .write(true)
            .append(true)
            .open(file_name)
        {
            Ok(file) => {
                return Ok(Self {
                    fd: Arc::new(RwLock::new(file)),
                })
            }
            Err(e) => {
                error!("Failed to open file: {e}");
                return Err(Errors::FailedToOpenDataFile);
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> crate::errors::Result<usize> {
        let read_guard = self.fd.read();
        match read_guard.read_at(buf, offset) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("read from data file err: {}", e);
                return Err(Errors::FailedToOpenDataFile);
            }
        };
    }

    fn sync(&self) -> crate::errors::Result<()> {
        // self.fd.
        let read_guard = self.fd.read();
        if let Err(e) = read_guard.sync_all() {
            error!("Failed to sync data file: {}", e);
            return Err(Errors::FailedToSyncFile);
        }

        Ok(())
    }

    fn write(&self, buf: &[u8]) -> crate::errors::Result<usize> {
        let mut write_guard = self.fd.write();
        match write_guard.write(buf) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("Write to file err: {e}");
                return Err(Errors::FailedToReadFromDataFile);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, read_to_string},
        path::PathBuf,
    };

    use crate::fio::IOManager;

    use super::FileIO;

    #[test]
    fn test_file_io_write() {
        let path = PathBuf::from("/tmp/a.data");
        let fio_res = FileIO::new(&path);
        assert!(fio_res.is_ok());
        let fio = fio_res.unwrap();

        let res1 = fio.write("Hello World1".as_bytes());
        assert!(res1.is_ok());

        let res2 = fio.write("Hello KV-store".as_bytes());
        assert!(res2.is_ok());

        let a = read_to_string(&path).unwrap();
        println!("{}", a);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_file_io_read() {
        let path = PathBuf::from("/tmp/b.data");
        let fio_res = FileIO::new(&path);
        assert!(fio_res.is_ok());
        let fio = fio_res.unwrap();

        let res1 = fio.write("Hello World".as_bytes());
        assert!(res1.is_ok());

        let res2 = fio.write("Hello KV-store".as_bytes());
        assert!(res2.is_ok());

        let mut buf = [0u8; 11];
        let res1 = fio.read(&mut buf, 0);
        assert!(res1.is_ok());
        assert!(res1.ok().unwrap() == 11);
        println!("{:?}", String::from_utf8(buf.to_vec()));

        let mut buf = [0u8; 14];
        let res2 = fio.read(&mut buf, 11);
        assert!(res2.is_ok());
        assert!(res2.ok().unwrap() == 14);
        println!("{:?}", String::from_utf8(buf.to_vec()));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_file_io_sync() {
        let path = PathBuf::from("/tmp/c.data");
        let fio_res = FileIO::new(&path);
        assert!(fio_res.is_ok());
        let fio = fio_res.unwrap();

        let res1 = fio.write("Hello World".as_bytes());
        assert!(res1.is_ok());

        let sync_res = fio.sync();
        assert!(sync_res.is_ok());
    }
}
