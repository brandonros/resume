mod producer;
mod worker;

use std::fmt;
use std::time::Duration;

pub use producer::{Producer, RetryPolicy, Submitted};
pub use worker::{Run, Worker, lock_resource, shutdown_signal};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// Fails the run without retrying, for errors such as invalid input.
#[derive(Debug)]
pub struct Permanent(pub String);

/// Releases the run for this duration without consuming a retry or extending its deadline.
/// Propagate with `?` before a readiness check completes: that step's database changes roll
/// back, completed steps stay saved, and the unfinished step runs on the next claim.
/// A `step_once` action cannot snooze because it must not run again.
#[derive(Debug)]
pub struct Snooze(pub Duration);

impl fmt::Display for Permanent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for Snooze {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "snooze for {:?}", self.0)
    }
}

impl std::error::Error for Permanent {}
impl std::error::Error for Snooze {}
