use std::{fmt::Error, result};

use thiserror::Error;

pub type Result<T> = result::Result<T, Errors>;

#[derive(Error, Debug)]
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
}
