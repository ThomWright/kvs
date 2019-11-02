use crate::Result;

/// Interface for a simple key-value store.
#[allow(clippy::module_name_repetitions)]
pub trait KvsEngine {
    #[allow(missing_docs)]
    fn get(&mut self, key: String) -> Result<Option<String>>;
    #[allow(missing_docs)]
    fn set(&mut self, key: String, value: String) -> Result<()>;
    #[allow(missing_docs)]
    fn remove(&mut self, key: String) -> Result<()>;
}
