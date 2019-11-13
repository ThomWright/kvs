use crate::KvsEngine;
use crate::KvsError;
use crate::Result;
use std::fs;
use std::path::PathBuf;

pub(crate) const SLED_DIR: &str = ".sled";

/// Implementation of a simple, persistent key-value store using `sled`.
#[derive(Debug, Copy, Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct SledKvsEngine {}

impl SledKvsEngine {
    /// Create a new sled store inside the given `path` directory.
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let path_dir = path.into();
        if !path_dir.is_dir() {
            return Err(KvsError::NotADirectory.into());
        }
        let kvs_dir = path_dir.join(SLED_DIR);

        fs::create_dir_all(&kvs_dir)?;

        Ok(SledKvsEngine {})
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        unimplemented!();
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        unimplemented!()
    }

    fn remove(&mut self, key: String) -> Result<()> {
        unimplemented!()
    }
}
