use std::time::{Duration, SystemTime};

use serde_json::Value;
use tokio_postgres::GenericClient;

use crate::Result;

use super::JobHandle;

/// Retry delays double from `delay` up to `max_delay`, then shrink randomly by up to half.
/// `max_attempts` includes crash recovery but excludes shutdown and snooze releases.
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
    start: Start,
    on_failure: Option<(String, String)>,
}

/// When a new run becomes eligible. A delay counts from the database's clock at submission.
enum Start {
    After(Duration),
    At(SystemTime),
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
            start: Start::After(Duration::ZERO),
            on_failure: None,
        }
    }

    /// Sets the retry policy for runs this producer creates.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// After terminal failure or operator cancellation, enqueue this workflow atomically.
    /// Its input contains `failed_run`, `error`, and the original `input`.
    /// The handler has its own three attempts; a failed handler can be reopened by an operator.
    /// Like the retry policy, this only applies when the original run is first created.
    pub fn on_failure(mut self, workflow: impl Into<String>, version: impl Into<String>) -> Self {
        self.on_failure = Some((workflow.into(), version.into()));
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
        self.start = Start::After(delay);
        self
    }

    /// Insert runs now, eligible at this instant, such as a `SystemTime` or a chrono
    /// `DateTime<Utc>`. Past times are eligible immediately. Actual execution depends on worker
    /// availability.
    ///
    /// Applies to `submit` and `submit_for`. Replaces any earlier `delay` call; submitting an
    /// existing key keeps its original schedule. Deadlines still count from submission.
    ///
    /// ```no_run
    /// # async fn example(client: &tokio_postgres::Client) -> resume::Result<()> {
    /// let tomorrow = std::time::SystemTime::now() + std::time::Duration::from_secs(86400);
    /// resume::Producer::new(client, "reminders", "1")
    ///     .at(tomorrow)
    ///     .submit("reminder:42", &serde_json::json!({"message": "Time to stretch"}))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn at(mut self, at: impl Into<SystemTime>) -> Self {
        self.start = Start::At(at.into());
        self
    }

    /// Returns the run for `idempotency_key`, creating it if needed. Reusing a key with
    /// different input is an error. The retry policy, schedule and deadline only apply when the
    /// run is created.
    pub async fn submit(&self, idempotency_key: &str, input: &Value) -> Result<JobHandle> {
        self.submit_run(idempotency_key, input, None).await
    }

    /// Like `submit`, for a run that changes `subject`, such as "customer:42". `Job::is_latest`
    /// then says whether a newer run of this workflow has the same subject.
    pub async fn submit_for(
        &self,
        subject: &str,
        idempotency_key: &str,
        input: &Value,
    ) -> Result<JobHandle> {
        self.submit_run(idempotency_key, input, Some(subject)).await
    }

    async fn submit_run(
        &self,
        idempotency_key: &str,
        input: &Value,
        subject: Option<&str>,
    ) -> Result<JobHandle> {
        let (delay, at) = match self.start {
            Start::After(delay) => (delay, None),
            Start::At(at) => (Duration::ZERO, Some(at)),
        };
        let (on_failure_workflow, on_failure_version) =
            self.on_failure.as_ref().map(|(w, v)| (w, v)).unzip();
        let row = self
            .client
            .query_one(
                "select run_id, created from resume.submit_run($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
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
                    &delay.as_secs_f64(),
                    &at,
                    &on_failure_workflow,
                    &on_failure_version,
                ],
            )
            .await?;
        Ok(JobHandle {
            id: row.try_get("run_id")?,
            created: row.try_get("created")?,
        })
    }
}
