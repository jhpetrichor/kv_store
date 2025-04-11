use std::result;

use thiserror::Error;

pub type Result<T> = result::Result<T, Errors>;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Errors {
    #[error("Failed to read from data file!")]
    FailedToReadFromDataFile,

    #[error("Failed to write to data file!")]
    FailedToWriteToDataFile,

    #[error("Failed to sync file!")]
    FailedToSyncFile,

    #[error("Failed to open data file!")]
    FailedToOpenDataFile,

    #[error("Empty key!")]
    KeyIsEmpty,

    #[error("Memory index faile to update!")]
    IndexUpdateFailed,

    #[error("Key is not found in database!")]
    KeyNotFound,

    #[error("Data file not found in database!")]
    DataFileNotFound,

    #[error("Database data path can not be empty!")]
    DirPathIsEmpty,

    #[error("Database data file size must be greater than 100")]
    DataFileSizeTooSmall,

    #[error("Failed to create the databse directory")]
    FailedToCreateDatabaseDir,

    #[error("Failed to read databse directory")]
    FailedToReadDatabaseDir,

    #[error("The databse directory maybe corrupted!")]
    DataDirectoryCorrupted,

    #[error("Read data file EOF!")]
    ReadDataFileEOF,

    #[error("Invalid crc value, log record maybe corrupted!")]
    InvalidLogRecordCrc,

    #[error("Exceed the max batch num")]
    ExceddMaxBatchNum,
}
