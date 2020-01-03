use crate::engine::KvsEngine;
use crate::errors::KvsError;
use crate::network_data::{ErrorType, NetworkCommand, NetworkResponse};
use crate::sled::SLED_DIR;
use crate::store::KVS_DIR;
use crate::Result;
use serde_json;
use slog;
use slog::Logger;
use std::fmt;
use std::fmt::Display;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::net::{TcpListener, ToSocketAddrs};
use std::path;

#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineType {
    Kvs,
    Sled,
}
impl Display for EngineType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineType::Kvs => write!(f, "kvs"),
            EngineType::Sled => write!(f, "sled"),
        }
    }
}
impl slog::Value for EngineType {
    fn serialize(
        &self,
        _rec: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match self {
            EngineType::Kvs => serializer.emit_str(key, "kvs"),
            EngineType::Sled => serializer.emit_str(key, "sled"),
        }
    }
}

/// Listens for KVS commands over a TCP connection.
#[allow(clippy::module_name_repetitions, missing_debug_implementations)]
pub struct KvsServer<T: KvsEngine> {
    socket: TcpListener,
    log: Logger,
    engine: T,
}

impl<T> KvsServer<T>
where
    T: KvsEngine,
{
    /// Create a new KVS server bound to addr
    pub fn bind<A: ToSocketAddrs>(addr: A, log: Logger, engine: T) -> Result<KvsServer<T>> {
        Ok(KvsServer {
            socket: TcpListener::bind(addr)?,
            log,
            engine,
        })
    }

    /// Start listening
    pub fn start(&mut self) {
        while let Some(stream) = self.socket.incoming().next() {
            match stream {
                Ok(stream) => {
                    let reader = BufReader::new(&stream);
                    let mut writer = BufWriter::new(&stream);
                    let commands =
                        serde_json::Deserializer::from_reader(reader).into_iter::<NetworkCommand>();

                    for command in commands {
                        match command {
                            Err(_e) => {
                                serde_json::to_writer(
                                    &mut writer,
                                    &NetworkResponse::Error {
                                        code: ErrorType::CommandDeserialisation,
                                    },
                                )
                                .expect("Failed to write to TCP stream");
                            }
                            Ok(c) => {
                                let response = &self.handle_command(&c);

                                serde_json::to_writer(&mut writer, response)
                                    .expect("Failed to write to TCP stream");

                                writer.flush().expect("Failed to flush TCP stream");
                            }
                        }
                    }
                }
                Err(_e) => {
                    warn!(self.log, "Error on connection stream");
                }
            }
        }
    }

    fn handle_command(&mut self, cmd: &NetworkCommand) -> NetworkResponse {
        match cmd {
            NetworkCommand::Get { key } => match self.engine.get(key.to_string()) {
                Ok(v) => match v {
                    Some(value) => NetworkResponse::Value(value),
                    None => NetworkResponse::Empty,
                },
                _ => NetworkResponse::Error {
                    code: ErrorType::Unknown,
                },
            },
            NetworkCommand::Set { key, value } => {
                match self.engine.set(key.to_string(), value.to_string()) {
                    Ok(()) => NetworkResponse::Empty,
                    _ => NetworkResponse::Error {
                        code: ErrorType::Unknown,
                    },
                }
            }
            NetworkCommand::Rm { key } => match self.engine.remove(key.to_string()) {
                Ok(()) => NetworkResponse::Empty,
                Err(e) => match e.downcast::<KvsError>() {
                    Ok(KvsError::KeyNotFound) => NetworkResponse::Error {
                        code: ErrorType::KeyNotFound,
                    },
                    _ => NetworkResponse::Error {
                        code: ErrorType::Unknown,
                    },
                },
            },
        }
    }
}

/// Is there existing data from one of the engines?
pub fn existing_engine(dir: &path::PathBuf) -> Option<EngineType> {
    if path::Path::new(&dir.join(KVS_DIR)).exists() {
        return Some(EngineType::Kvs);
    }
    if path::Path::new(&dir.join(SLED_DIR)).exists() {
        return Some(EngineType::Sled);
    }
    None
}
