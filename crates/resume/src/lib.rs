mod lock;
mod producer;
mod run;
mod worker;

use std::fmt;

pub use lock::lock_resource;
pub use producer::{Producer, RetryPolicy, Submitted};
pub use run::Run;
pub use worker::{Worker, shutdown_signal};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// What `Run::step_if`'s check decided.
pub enum Check {
    /// Run the action.
    Proceed,
    /// Skip the action for this reason; the step returns None, now and on every replay.
    Skip(String),
    /// Fail the run for this reason, without retrying.
    Fail(String),
}

/// Return this from a step for an error another attempt cannot fix, such as invalid input.
/// The run fails at once instead of using its remaining attempts.
#[derive(Debug)]
pub struct Permanent(pub String);

/// The run was marked failed, so no later attempt will retry it.
#[derive(Debug)]
pub struct RunFailed(pub String);

/// The worker is stopping, so the run stops before its next step and is released.
#[derive(Debug)]
pub(crate) struct Stopping;

impl fmt::Display for Permanent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for RunFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for Stopping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("worker is stopping")
    }
}

impl std::error::Error for Permanent {}
impl std::error::Error for RunFailed {}
impl std::error::Error for Stopping {}
