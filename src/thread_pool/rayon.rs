use super::ThreadPool;
use crate::Result;
use rayon;
use rayon::ThreadPoolBuilder;

/// A rayon thread pool for performance comparison.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(num_threads: u32) -> Result<Self> {
        Ok(RayonThreadPool(
            ThreadPoolBuilder::new()
                .num_threads(num_threads as usize)
                .build()?,
        ))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.install(job);
    }
}
