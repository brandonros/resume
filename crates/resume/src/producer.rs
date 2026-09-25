use std::time::Duration;

use serde_json::Value;
use tokio_postgres::Client;

use crate::Result;

/// How a run retries after a failed attempt. The first retry waits `delay`, and each later one
/// waits twice as long as the last, up to `max_delay`, less a random part of up to half.
/// `max_attempts` counts claims, including recovery after a crash, but not claims a stopping
/// worker gave back.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub max_attempts: i32,
    pub delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
        }
    }
}

pub struct Producer<'a> {
    client: &'a Client,
    workflow: String,
    retry: RetryPolicy,
}

pub struct Submitted {
    pub id: i64,
    /// False when a run with this idempotency key already existed, in any state.
    pub created: bool,
}

impl<'a> Producer<'a> {
    pub fn new(client: &'a Client, workflow: impl Into<String>) -> Self {
        Self {
            client,
            workflow: workflow.into(),
            retry: RetryPolicy::default(),
        }
    }

    /// Sets the retry policy for runs this producer creates.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Returns the run for `idempotency_key`, creating it if needed. Reusing a key with
    /// different input is an error. The retry policy only applies when the run is created.
    pub async fn submit(&self, idempotency_key: &str, input: &Value) -> Result<Submitted> {
        let row = self
            .client
            .query_one(
                "select run_id, created from resume.submit_run($1, $2, $3, $4, $5, $6)",
                &[
                    &self.workflow,
                    &idempotency_key,
                    input,
                    &self.retry.max_attempts,
                    &self.retry.delay.as_secs_f64(),
                    &self.retry.max_delay.as_secs_f64(),
                ],
            )
            .await?;
        Ok(Submitted {
            id: row.try_get("run_id")?,
            created: row.try_get("created")?,
        })
    }
}
