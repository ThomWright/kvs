use failure;
use std::result;

/// Convenience Result type.
pub type Result<T> = result::Result<T, failure::Error>;

/// Errors
#[derive(Debug, failure::Fail, Clone, Copy)]
pub enum KvsError {
    /// A key was not found in the database
    #[fail(display = "Key not found")]
    KeyNotFound {},

    /// An unexpected command was found in the database - probably a program error.
    #[fail(display = "Unexpected command found in log")]
    UnexpectedCommand {},

    /// An unexpected key was found in the database - probably a program error.
    #[fail(display = "Unexpected key found in log")]
    UnexpectedKey {},
}
