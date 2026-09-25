mod producer;
mod worker;

use std::fmt;
use std::time::Duration;

pub use producer::{Producer, RetryPolicy, Submitted};
pub use worker::{Run, Worker, lock_resource, shutdown_signal};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// Return this from a step for an error another attempt cannot fix, such as invalid input.
/// The run fails at once instead of using its remaining attempts.
#[derive(Debug)]
pub struct Permanent(pub String);

/// Give the run back until this delay has passed, without using up a retry. Propagate it to
/// the worker with `?`: completed steps stay saved, and the unfinished step runs again when
/// the run is next claimed. The wait never extends the run's deadline.
///
/// Return it before saving a readiness check as completed. Like other step errors, it rolls
/// back that step's database changes. A `step_once` action cannot snooze: it must not run again.
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
