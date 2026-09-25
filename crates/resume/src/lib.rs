mod lock;
mod producer;
mod run;
mod worker;

pub use lock::lock_resource;
pub use producer::{Producer, Submitted};
pub use run::Run;
pub use worker::Worker;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// The run was marked failed, so no later attempt will retry it.
#[derive(Debug)]
pub struct RunFailed(pub String);

impl std::fmt::Display for RunFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RunFailed {}
