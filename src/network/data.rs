use failure;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// The network representation of commands which can be performed on the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkCommand {
    Get {
        #[serde(rename = "k")]
        key: String,
    },
    Set {
        #[serde(rename = "k")]
        key: String,
        #[serde(rename = "v")]
        value: String,
    },
    Rm {
        #[serde(rename = "k")]
        key: String,
    },
}

impl Display for NetworkCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkCommand::Get { key } => write!(f, "Get '{}'", key),
            NetworkCommand::Set { key, value } => write!(f, "Set '{}' to '{}'", key, value),
            NetworkCommand::Rm { key } => write!(f, "Remove '{}'", key),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkResponse {
    Error { code: ErrorType },
    Empty,
    Value(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, failure::Fail)]
pub enum ErrorType {
    #[fail(display = "Command failed to deserialise")]
    CommandDeserialisation,

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "Unknown error")]
    Unknown,
}
