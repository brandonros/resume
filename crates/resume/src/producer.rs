use std::time::Duration;

use serde_json::Value;
use tokio_postgres::GenericClient;

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

pub struct Producer<'a, C: GenericClient> {
    client: &'a C,
    workflow: String,
    version: String,
    retry: RetryPolicy,
    deadline: Option<Duration>,
}

pub struct Submitted {
    pub id: i64,
    /// False when a run with this idempotency key already existed, in any state.
    pub created: bool,
}

impl<'a, C: GenericClient> Producer<'a, C> {
    /// `client` can be a transaction, so a request's own changes and the runs it submits
    /// commit together, or not at all. Runs are submitted for `version` of the workflow, and
    /// only workers of that version claim them.
    pub fn new(client: &'a C, workflow: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            client,
            workflow: workflow.into(),
            version: version.into(),
            retry: RetryPolicy::default(),
            deadline: None,
        }
    }

    /// Sets the retry policy for runs this producer creates.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Fails runs this producer creates if they have not completed within `deadline` of being
    /// submitted, whether they are waiting or in progress.
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Returns the run for `idempotency_key`, creating it if needed. Reusing a key with
    /// different input is an error. The retry policy only applies when the run is created.
    pub async fn submit(&self, idempotency_key: &str, input: &Value) -> Result<Submitted> {
        self.submit_run(idempotency_key, input, None).await
    }

    /// Like `submit`, for a run that changes `subject`, such as "customer:42". Its
    /// `step_latest` steps run only if no newer run of this workflow has the same subject.
    pub async fn submit_for(
        &self,
        subject: &str,
        idempotency_key: &str,
        input: &Value,
    ) -> Result<Submitted> {
        self.submit_run(idempotency_key, input, Some(subject)).await
    }

    async fn submit_run(
        &self,
        idempotency_key: &str,
        input: &Value,
        subject: Option<&str>,
    ) -> Result<Submitted> {
        let row = self
            .client
            .query_one(
                "select run_id, created from resume.submit_run($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                &[
                    &self.workflow,
                    &self.version,
                    &idempotency_key,
                    input,
                    &self.retry.max_attempts,
                    &self.retry.delay.as_secs_f64(),
                    &self.retry.max_delay.as_secs_f64(),
                    &subject,
                    &self.deadline.map(|d| d.as_secs_f64()),
                ],
            )
            .await?;
        Ok(Submitted {
            id: row.try_get("run_id")?,
            created: row.try_get("created")?,
        })
    }
}
