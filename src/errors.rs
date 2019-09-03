use failure;
use std::result;

/// Convenience Result type.
pub type Result<T> = result::Result<T, failure::Error>;

/// Errors
#[derive(Debug, failure::Fail, Clone, Copy)]
pub enum KvsError {
    /// An attempt was made to open the KV store in a non-directory file path
    #[fail(display = "Not a directory")]
    NotADirectory {},

    /// A key was not found in the database
    #[fail(display = "Key not found")]
    KeyNotFound {},

    /// An unexpected command was found in the database - probably a program error
    #[fail(display = "Unexpected command found in log")]
    UnexpectedCommand {},

    /// An unexpected key was found in the database - probably a program error
    #[fail(display = "Unexpected key found in log")]
    UnexpectedKey {},

    /// An unexpected file name was found
    #[fail(display = "Unexpected file name, should be an integer")]
    UnexpectedFileName {},
}
