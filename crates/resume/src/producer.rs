use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio_postgres::GenericClient;

use crate::Result;

/// How a run retries after a failed attempt. The first retry waits `delay`, and each later one
/// waits twice as long as the last, up to `max_delay`, less a random part of up to half.
/// `max_attempts` counts claims, including recovery after a crash, but excludes claims
/// released for shutdown or snoozing.
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
    delay: Duration,
    at: Option<SystemTime>,
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
            delay: Duration::ZERO,
            at: None,
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

    /// Insert runs now, but wait at least this long from submission before workers may claim
    /// them. Defaults to zero. Applies to both `submit` and `submit_for`; submitting an existing
    /// key again keeps its original schedule. A deadline still counts from submission, so a
    /// run whose deadline comes first fails without executing. Replaces any earlier `at` call.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self.at = None;
        self
    }

    /// Insert runs now, eligible at this timestamp. Accepts a UTC datetime; parsing an ISO
    /// 8601 / RFC 3339 string with `Z` or a UTC offset converts it to UTC. Past times are eligible
    /// immediately. Actual execution depends on worker availability.
    ///
    /// Applies to `submit` and `submit_for`. Replaces any earlier `delay` call; submitting an
    /// existing key keeps its original schedule. Deadlines still count from submission.
    ///
    /// ```no_run
    /// # async fn example(client: &tokio_postgres::Client) -> resume::Result<()> {
    /// resume::Producer::new(client, "reminders", "1")
    ///     .at("2026-09-26T00:00:00-04:00".parse()?)
    ///     .submit("reminder:42", &serde_json::json!({"message": "Time to stretch"}))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn at(mut self, at: DateTime<Utc>) -> Self {
        self.at = Some(at.into());
        self.delay = Duration::ZERO;
        self
    }

    /// Returns the run for `idempotency_key`, creating it if needed. Reusing a key with
    /// different input is an error. The retry policy, schedule and deadline only apply when the
    /// run is created.
    pub async fn submit(&self, idempotency_key: &str, input: &Value) -> Result<Submitted> {
        self.submit_run(idempotency_key, input, None).await
    }

    /// Like `submit`, for a run that changes `subject`, such as "customer:42". `Run::is_latest`
    /// then says whether a newer run of this workflow has the same subject.
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
                "select run_id, created from resume.submit_run($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
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
                    &self.delay.as_secs_f64(),
                    &self.at,
                ],
            )
            .await?;
        Ok(Submitted {
            id: row.try_get("run_id")?,
            created: row.try_get("created")?,
        })
    }
}
