use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};

use bytes::Bytes;
use log::{error, warn};
use parking_lot::RwLock;

use crate::{
    data::{
        data_file::{DataFile, DATA_FILE_NAME_SUFFIX},
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    errors::{Errors, Result},
    index::{self, new_indexer},
    options::Options,
};

const INITAL_DILE_ID: u32 = 0;

pub struct Engine {
    options: Arc<Options>,
    // 当前活跃文件
    active_file: Arc<RwLock<DataFile>>,
    // 旧的数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    // 数据内存索引
    index: Box<dyn index::Indexer>,
    //数据库启动时的文件id，只用于加载索引使用，
    file_ids: Vec<u32>,
}

impl Engine {
    // 打开 bitcask 存储引擎实例
    pub fn open(opts: Options) -> Result<Self> {
        if let Some(e) = check_options(&opts) {
            return Err(e);
        }
        let options = opts.clone();
        // 判断数据目录是否存在，如果不存在则需要创建这个目录
        let dir_path = options.dir_path.clone();
        if !dir_path.is_dir() {
            if let Err(e) = fs::create_dir(&dir_path) {
                warn!("Failed to create database Directory: {e}");
                return Err(Errors::FailedToCreateDatabaseDir);
            }
        }
        // 加载数据文件
        let mut data_files = load_data_file(&dir_path)?;
        // 设置 file id信息
        let mut file_ids = Vec::new();
        for v in data_files.iter() {
            file_ids.push(v.get_file_id());
        }
        // 将旧的数据文件保存在older_files中
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..data_files.len() - 1 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }
        let active_file = match data_files.pop() {
            Some(v) => v,
            None => DataFile::new(dir_path.clone(), INITAL_DILE_ID)?,
        };

        // 构造存储引擎实例
        let engine = Self {
            options: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(new_indexer(options.index_type)),
            file_ids,
        };

        engine.load_index_from_data_file()?;

        Ok(engine)
    }

    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // 判断key的有效性
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 构造logRecord结构体
        let mut record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };

        // 追加写入到活跃文件中
        let log_record_pos = self.append_log_record(&mut record)?;
        // 更新内存索引
        let ok = self.index.put(key.to_vec(), log_record_pos);
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }

        Ok(())
    }

    /// 根据key读取对应数据
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 从内存索引中拿到对应的数据信息
        let pos = self.index.get(key.to_vec());
        // 不存在
        if pos.is_none() {
            return Err(Errors::KeyNotFound);
        }

        let log_record_pos = pos.unwrap();
        let active_file = self.active_file.read();
        let older_file = self.older_files.read();
        // 从对应的数据文件中获取对应的 Logrecord
        let read_log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?,
            false => {
                let data_file = older_file.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    // 找不到对应的数据文件，返回错误
                    return Err(Errors::FailedToOpenDataFile);
                }
                data_file.unwrap().read_log_record(log_record_pos.offset)?
            }
        };
        let log_record = read_log_record.record;

        // 判断 log_record 的类型
        if log_record.rec_type == LogRecordType::DELETED {
            return Err(Errors::KeyNotFound);
        }
        // 否则返回有效数据
        Ok(log_record.value.into())
    }

    /// 根据key删除对应数据
    pub fn delete(&self, key: Bytes) -> Result<()> {
        // 判断key的有效性
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        // key 是够存在
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }
        // 构造 LogRecord，标识其被删除
        let mut record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETED,
        };

        // 将数据追写入大数据文件中
        self.append_log_record(&mut record)?;
        // 更新（删除）内存索引
        let ok = self.index.delete(key.to_vec());
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }

        Ok(())
    }

    // 追加数据到当前活跃文件中
    fn append_log_record(&self, record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();

        let enc_record = record.encode();
        let record_len = enc_record.len();

        // 当前活跃文件
        let mut active_file = self.active_file.write();
        // 判断当前写入文件是否达到阈值
        //* */ 可否将持久化后的当前活跃文件加入到旧的文件中？
        if active_file.get_write_off() + record_len as u64 > self.options.data_file_size {
            // 将当前文件持久化
            active_file.sync()?;

            let current_fid = active_file.get_file_id();
            // 将旧的数据文件存储到map中
            let mut older_files = self.older_files.write();
            let older_file = DataFile::new(dir_path.clone(), current_fid)?;
            older_files.insert(current_fid, older_file);

            // 打开新的数据文件
            let new_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
            *active_file = new_file;
        }

        // 追加写数据到当前活跃文件中
        let write_off = active_file.get_write_off();
        active_file.write(&enc_record)?;

        // 根据配置项决定是否持久化
        if self.options.sync_write {
            active_file.sync()?;
        }

        // 构造内存索引信息
        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        })
    }

    // 从数据文件中加载内存索引
    // 遍历数据文件中的内容，并依次处理其中的记录
    fn load_index_from_data_file(&self) -> Result<()> {
        if self.file_ids.is_empty() {
            return Ok(());
        }
        let active_file = self.active_file.read();
        let older_file = self.older_files.read();

        // 遍历每个文件id，去除对应的数据文件，并加载其中的数据
        for (i, file_id) in self.file_ids.iter().enumerate() {
            let mut offset = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let data_file = older_file.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    }
                };
                let (log_record, size) = match log_record_res {
                    Ok(res) => (res.record, res.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        }
                        return Err(e);
                    }
                };

                // 构建内存索引
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };
                let ok = match log_record.rec_type {
                    LogRecordType::NORMAL => {
                        self.index.put(log_record.key.to_vec(), log_record_pos)
                    }
                    LogRecordType::DELETED => self.index.delete(log_record.key.to_vec()),
                };
                if !ok {
                    return Err(Errors::IndexUpdateFailed);
                }

                // 更新offset，下一次读取时候的开始位置
                offset += size as u64;
            }
            // 如果当前文件时活跃文件，则需要设置活跃文件offset，供新数据写入
            if i == self.file_ids.len() {
                active_file.set_write_off(offset);
            }
        }

        Ok(())
    }
}

// 从数据目录中加载数据文件
fn load_data_file(dir_path: &PathBuf) -> Result<Vec<DataFile>> {
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(Errors::FailedToReadDatabaseDir);
    }

    let mut file_ids = Vec::<u32>::new();
    let mut data_files = Vec::<DataFile>::new();
    for file in dir.unwrap() {
        if let Ok(entry) = file {
            // 拿到文件名
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();

            // 判断文件名是否以 .data结尾
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                // 000001.data
                let split_name = file_name.split('.').collect::<Vec<_>>();
                let file_id = match split_name[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => {
                        error!("");
                        return Err(Errors::DataDirectoryCorrupted);
                    }
                };
                file_ids.push(file_id);
            }
        }
    }
    // 如果没有数据文件，则直接返回
    if file_ids.is_empty() {
        return Ok(data_files);
    }

    // 对文件排序，从小到大加载
    file_ids.sort();
    // 遍历所有的文件id，依次打开对应的数据文件
    for file_id in file_ids.iter() {
        let data_file = DataFile::new(dir_path.clone(), *file_id)?;
        data_files.push(data_file);
    }

    Ok(data_files)
}

fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().is_empty() {
        return Some(Errors::DirPathIsEmpty);
    }

    if opts.data_file_size < 100 {
        return Some(Errors::DataFileSizeTooSmall);
    }

    None
}
