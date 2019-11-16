use crate::errors::KvsError;
use crate::KvsEngine;
use crate::Result;
use sled::Db;
use std::fs;
use std::path::PathBuf;
use std::str;

pub const SLED_DIR: &str = ".sled";

/// Implementation of a simple, persistent key-value store using `sled`.
#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    /// Create a new sled store inside the given `path` directory.
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let path_dir = path.into();
        if !path_dir.is_dir() {
            return Err(KvsError::NotADirectory.into());
        }
        let sled_dir = path_dir.join(SLED_DIR);

        fs::create_dir_all(&sled_dir)?;

        let db = Db::open(sled_dir)?;

        Ok(SledKvsEngine { db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.db.get(key)? {
            None => Ok(None),
            Some(buf) => Ok(Some(String::from_utf8(buf.to_vec())?)),
        }
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.db.remove(key)? {
            None => Err(KvsError::KeyNotFound.into()),
            Some(_) => {
                self.db.flush()?;
                Ok(())
            }
        }
    }
}
