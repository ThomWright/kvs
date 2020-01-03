use super::ThreadPool;
use crate::Result;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone, Copy)]
pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
    fn new(_: u32) -> Result<Self> {
        unimplemented!()
    }

    fn spawn<F>(&self, _job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        unimplemented!()
    }
}
