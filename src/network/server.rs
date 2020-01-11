use super::data::{ErrorType, NetworkCommand, NetworkResponse};
use crate::engines::KvsEngine;
use crate::engines::KVS_DIR;
use crate::engines::SLED_DIR;
use crate::errors::KvsError;
use crate::thread_pool::ThreadPool;
use crate::Result;
use serde_json;
use slog;
use slog::Logger;
use std::fmt;
use std::fmt::Display;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path;

/// Listens for KVS commands over a TCP connection.
#[allow(clippy::module_name_repetitions, missing_debug_implementations)]
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    log: Logger,
    engine: E,
    pool: P,
}

impl<E, P> KvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    /// Create a new KVS server
    pub fn new(log: Logger, engine: E, pool: P) -> Result<KvsServer<E, P>> {
        Ok(KvsServer { log, engine, pool })
    }

    /// Bind to a socket and start listening
    pub fn run<A: ToSocketAddrs>(&self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let eng = self.engine.clone();
                    let log = self.log.clone();
                    self.pool.spawn(move || {
                        KvsServer::<E, P>::handle_req(&stream, &eng).unwrap_or_else(|_e| {
                            error!(log, "Error handling request");
                        })
                    })
                }
                Err(_e) => error!(self.log, "Error on connection stream"),
            }
        }

        Ok(())
    }

    fn handle_req(stream: &TcpStream, engine: &E) -> Result<()> {
        let reader = BufReader::new(stream);
        let mut writer = BufWriter::new(stream);
        let commands = serde_json::Deserializer::from_reader(reader).into_iter::<NetworkCommand>();

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
                Ok(cmd) => {
                    let response = KvsServer::<E, P>::handle_command(&cmd, &engine);

                    serde_json::to_writer(&mut writer, &response)
                        .expect("Failed to write to TCP stream");

                    writer.flush().expect("Failed to flush TCP stream");
                }
            }
        }

        Ok(())
    }

    fn handle_command(cmd: &NetworkCommand, engine: &E) -> NetworkResponse {
        match cmd {
            NetworkCommand::Get { key } => match engine.get(key.to_string()) {
                Ok(v) => match v {
                    Some(value) => NetworkResponse::Value(value),
                    None => NetworkResponse::Empty,
                },
                _ => NetworkResponse::Error {
                    code: ErrorType::Unknown,
                },
            },
            NetworkCommand::Set { key, value } => {
                match engine.set(key.to_string(), value.to_string()) {
                    Ok(()) => NetworkResponse::Empty,
                    _ => NetworkResponse::Error {
                        code: ErrorType::Unknown,
                    },
                }
            }
            NetworkCommand::Rm { key } => match engine.remove(key.to_string()) {
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
