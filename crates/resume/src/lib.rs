mod producer;
mod run;
mod worker;

pub use producer::{Producer, Submitted};
pub use run::Run;
pub use worker::Worker;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
