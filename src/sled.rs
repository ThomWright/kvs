use crate::KvsEngine;
use crate::Result;

/// Implementation of a simple, persistent key-value store using `sled`.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
struct SledKvsEngine {}

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
