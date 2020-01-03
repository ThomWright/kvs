use super::data::{ErrorType, NetworkCommand, NetworkResponse};
use crate::Result;
use std::net::{TcpStream, ToSocketAddrs};

/// Client for accessing KVS over a network connection.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct KvsClient {
    connection: TcpStream,
}

impl KvsClient {
    /// Create a connection to the KVS server.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<KvsClient> {
        Ok(KvsClient {
            connection: TcpStream::connect(addr)?,
        })
    }
    #[allow(missing_docs)]
    pub fn get(self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&self.connection, &NetworkCommand::Get { key })?;
        let mut responses =
            serde_json::Deserializer::from_reader(&self.connection).into_iter::<NetworkResponse>();

        match responses.next() {
            Some(response) => match response {
                Ok(response) => match response {
                    NetworkResponse::Error { code } => Err(code.into()),
                    NetworkResponse::Empty => Ok(None),
                    NetworkResponse::Value(value) => Ok(Some(value)),
                },
                Err(_e) => Err((Error::ResponseDeserialisation).into()),
            },
            None => Err((Error::NoResponse).into()),
        }
    }
    #[allow(missing_docs)]
    pub fn set(self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&self.connection, &NetworkCommand::Set { key, value })?;
        let mut responses =
            serde_json::Deserializer::from_reader(&self.connection).into_iter::<NetworkResponse>();

        match responses.next() {
            Some(response) => match response {
                Ok(response) => match response {
                    NetworkResponse::Error { code } => Err(code.into()),
                    NetworkResponse::Empty => Ok(()),
                    NetworkResponse::Value { .. } => Err(Error::UnexpectedResponse.into()),
                },
                Err(_e) => Err((Error::ResponseDeserialisation).into()),
            },
            None => Err((Error::NoResponse).into()),
        }
    }
    #[allow(missing_docs)]
    pub fn remove(self, key: String) -> Result<()> {
        serde_json::to_writer(&self.connection, &NetworkCommand::Rm { key })?;
        let mut responses =
            serde_json::Deserializer::from_reader(&self.connection).into_iter::<NetworkResponse>();

        match responses.next() {
            Some(response) => match response {
                Ok(response) => match response {
                    NetworkResponse::Error { code } => match code {
                        ErrorType::KeyNotFound => Err(Error::KeyNotFound.into()),
                        _ => Err(code.into()),
                    },
                    NetworkResponse::Empty => Ok(()),
                    NetworkResponse::Value { .. } => Err(Error::UnexpectedResponse.into()),
                },
                Err(_e) => Err((Error::ResponseDeserialisation).into()),
            },
            None => Err((Error::NoResponse).into()),
        }
    }
}

/// Errors which can be thrown in the client.
#[derive(Debug, Clone, Copy, failure::Fail)]
#[allow(missing_docs)]
pub enum Error {
    #[fail(display = "Failed to deserialise response")]
    ResponseDeserialisation,

    #[fail(display = "Unexpected response")]
    UnexpectedResponse,

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "No response from server")]
    NoResponse,
}
