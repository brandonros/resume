use std::time::Duration;

use tokio_postgres::Client;

use crate::Result;

/// A handle returned by a producer for observing a job and waiting for its outcome.
pub struct JobHandle {
    pub id: i64,
    /// False when a run with this idempotency key already existed, in any state.
    pub created: bool,
}

/// The terminal state observed while waiting. An operator can later reopen a failed job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOutcome {
    Completed,
    Failed { error: Option<String> },
    Cancelled { error: Option<String> },
}

impl JobHandle {
    /// Waits for the job to complete, fail, or be cancelled, polling every 250 ms.
    /// Retries and snoozes are not terminal outcomes. A separate worker must process the job.
    /// Commit any submission transaction before waiting, and use a client outside a transaction.
    ///
    /// The timeout covers database queries and polling waits. On timeout the error contains
    /// [`tokio::time::error::Elapsed`]; database errors propagate unchanged, and a missing job
    /// is an error. Timing out or dropping this future does not cancel the job. This only
    /// observes recorded state; it does not perform deadline cleanup or wait for failure handlers.
    ///
    /// ```no_run
    /// # async fn example(client: &tokio_postgres::Client) -> resume::Result<()> {
    /// use std::time::Duration;
    /// use resume::{JobOutcome, Producer};
    /// let handle = Producer::new(client, "checkout", "1")
    ///     .submit("order:42", &serde_json::json!({"order_id": 42}))
    ///     .await?;
    /// match handle.wait(client, Duration::from_secs(30)).await? {
    ///     JobOutcome::Completed => println!("Done"),
    ///     JobOutcome::Failed { error } => println!("Failed: {error:?}"),
    ///     JobOutcome::Cancelled { error } => println!("Cancelled: {error:?}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait(&self, client: &Client, timeout: Duration) -> Result<JobOutcome> {
        tokio::time::timeout(timeout, async {
            loop {
                let row = client
                    .query_opt(
                        "select completed_at is not null as completed,
                                failed_at is not null as failed,
                                cancelled_at is not null as cancelled, last_error
                         from resume.runs where id = $1",
                        &[&self.id],
                    )
                    .await?
                    .ok_or_else(|| format!("job {} does not exist", self.id))?;
                if row.try_get::<_, bool>("completed")? {
                    return Ok(JobOutcome::Completed);
                }
                if row.try_get::<_, bool>("cancelled")? {
                    return Ok(JobOutcome::Cancelled {
                        error: row.try_get("last_error")?,
                    });
                }
                if row.try_get::<_, bool>("failed")? {
                    return Ok(JobOutcome::Failed {
                        error: row.try_get("last_error")?,
                    });
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await?
    }
}
