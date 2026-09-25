//! Jobs executed by workflow handlers and handles used by callers to observe their outcomes.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Mutex, MutexGuard};
use tokio_postgres::{Client, Row, Transaction};
use tracing::Instrument;

use crate::{Permanent, Result, Snooze};

/// A job supplied by the worker to a workflow handler, with its input and durable steps.
/// Each instance belongs to one claimed attempt at a workflow run.
pub struct Job {
    pub id: i64,
    pub idempotency_key: String,
    pub input: Value,
    attempt: i64,
    attempts_used: i64,
    max_attempts: i32,
    lease: Duration,
    step_timeout: Duration,
    stopping: Arc<AtomicBool>,
    /// Next step position in this attempt.
    position: AtomicI32,
    /// Step keys already used in this attempt.
    keys: std::sync::Mutex<HashSet<String>>,
    client: Arc<Mutex<Client>>,
}

impl Job {
    pub(crate) fn from_claim(
        row: &Row,
        client: Arc<Mutex<Client>>,
        lease: Duration,
        step_timeout: Duration,
        stopping: Arc<AtomicBool>,
    ) -> Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            idempotency_key: row.try_get("idempotency_key")?,
            input: row.try_get("input")?,
            attempt: row.try_get("attempt")?,
            attempts_used: row.try_get("attempts_used")?,
            max_attempts: row.try_get("max_attempts")?,
            lease,
            step_timeout,
            stopping,
            position: AtomicI32::new(0),
            keys: std::sync::Mutex::default(),
            client,
        })
    }

    pub(crate) fn attempts_used(&self) -> i64 {
        self.attempts_used
    }

    pub(crate) fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    /// Use a key that is unique within the run and stable across attempts; it may be dynamic.
    /// Use the supplied transaction for the step's database effects.
    /// Those effects and the saved result commit together. External effects may repeat.
    ///
    /// To make sure an external effect exists without repeating it, look for it first and
    /// create it only if missing: a retry after a crash then finds it. To act only while the
    /// current state allows, check and act in the same step, locking what the check reads
    /// (`select ... for update` for rows, `lock_resource` for anything else); to skip, return
    /// an output that says so.
    pub async fn step(
        &self,
        key: &str,
        action: impl AsyncFnOnce(&Transaction<'_>) -> Result<Value>,
    ) -> Result<Value> {
        async {
            let mut client = self.client()?;
            let (tx, saved) = self.begin_step(&mut client, key).await?;
            if let Some(output) = saved {
                tx.commit().await?;
                tracing::info!("using saved result");
                return Ok(output);
            }

            tracing::info!("executing");
            let output = self.timed(key, action(&tx)).await?;
            let saved = self.save_step(&tx, key, &output).await?;
            tx.commit().await?;
            tracing::info!("committed");
            Ok(saved)
        }
        .instrument(step_span(key))
        .await
    }

    /// For external effects that must not repeat, such as a vendor without idempotency.
    /// The action runs at most once per run. If an attempt dies or times out after starting it
    /// and before saving its result, the outcome is unknown: the run fails instead of calling
    /// again, and someone must check the vendor and pass the output to resume.reopen_run.
    /// Returning `Snooze` also fails the run; use a regular step for readiness checks.
    pub async fn step_once(
        &self,
        key: &str,
        action: impl AsyncFnOnce() -> Result<Value>,
    ) -> Result<Value> {
        async {
            let mut client = self.client()?;
            let (tx, saved) = self.begin_step(&mut client, key).await?;
            // Commit the start before calling, so a later attempt knows the call may have happened.
            tx.commit().await?;
            if let Some(output) = saved {
                tracing::info!("using saved result");
                return Ok(output);
            }

            tracing::info!("executing once");
            let output = self.timed(key, action()).await.map_err(|error| {
                if error.is::<Snooze>() {
                    Permanent(format!(
                        "step {key} cannot snooze after starting a step_once action; \
                         its outcome is unknown; use a regular step for readiness checks"
                    ))
                    .into()
                } else {
                    error
                }
            })?;

            // save_step checks the claim itself, so this needs no transaction of its own.
            let saved = self.save_step(&*client, key, &output).await?;
            tracing::info!("committed");
            Ok(saved)
        }
        .instrument(step_span(key))
        .await
    }

    /// Whether no newer run of this workflow has the same subject (see `Producer::submit_for`),
    /// for use in a step. Locks the subject until the step's transaction ends, so a newer run's
    /// step waits for this one to commit and applies after it.
    pub async fn is_latest(&self, tx: &Transaction<'_>) -> Result<bool> {
        Ok(tx
            .query_one("select resume.lock_subject($1)", &[&self.id])
            .await?
            .try_get(0)?)
    }

    /// Rejects nested or concurrent steps rather than waiting for their connection.
    fn client(&self) -> Result<MutexGuard<'_, Client>> {
        self.client.try_lock().map_err(|_| {
            Permanent(
                "steps run one at a time: await each step before starting the next, \
                 and do not start one inside another's action"
                    .into(),
            )
            .into()
        })
    }

    /// Starts a transaction holding the run's lock, after checking this attempt owns it, and
    /// records the step's start. Returns the step's saved output if it already completed.
    /// If the run is past its deadline, or a step_once action's outcome is unknown, commits
    /// the run's failure and returns `RunFailed`.
    async fn begin_step<'c>(
        &self,
        client: &'c mut Client,
        key: &str,
    ) -> Result<(Transaction<'c>, Option<Value>)> {
        // A stopping worker lets the step in progress finish and starts no more.
        if self.stopping.load(Ordering::Relaxed) {
            return Err(Stopping.into());
        }
        if !self.keys.lock().unwrap().insert(key.to_string()) {
            return Err(Permanent(format!(
                "step key {key} is used twice; keys must be unique within a run"
            ))
            .into());
        }
        let position = self.position.fetch_add(1, Ordering::Relaxed);
        let tx = client.transaction().await?;
        let row = tx
            .query_one(
                "select output, failed from resume.begin_step($1, $2, $3, $4, $5)",
                &[
                    &self.id,
                    &self.attempt,
                    &key,
                    &position,
                    &self.lease.as_secs_f64(),
                ],
            )
            .await
            .map_err(|error| match error.as_db_error() {
                Some(db) if db.code().code() == "RS001" => Permanent(db.message().into()).into(),
                _ => crate::Error::from(error),
            })?;
        if let Some(reason) = row.try_get::<_, Option<String>>("failed")? {
            tx.commit().await?;
            return Err(RunFailed(reason).into());
        }
        let output = row.try_get("output")?;
        Ok((tx, output))
    }

    /// Bounds the action's duration. A timeout cannot undo external effects.
    async fn timed<T>(&self, key: &str, action: impl Future<Output = Result<T>>) -> Result<T> {
        match tokio::time::timeout(self.step_timeout, action).await {
            Ok(result) => result,
            Err(_) => Err(format!("step {key} timed out after {:?}", self.step_timeout).into()),
        }
    }

    async fn save_step(
        &self,
        db: &impl tokio_postgres::GenericClient,
        key: &str,
        output: &Value,
    ) -> Result<Value> {
        Ok(db
            .query_one(
                "select resume.save_step($1, $2, $3, $4)",
                &[&self.id, &self.attempt, &key, output],
            )
            .await?
            .try_get(0)?)
    }

    pub(crate) async fn complete_run(&self) -> Result<()> {
        self.client
            .lock()
            .await
            .execute(
                "select resume.complete_run($1, $2)",
                &[&self.id, &self.attempt],
            )
            .await?;
        Ok(())
    }

    /// Releases stopped or snoozing runs; records other errors for retry or terminal failure.
    pub(crate) async fn settle(&self, result: Result<()>) {
        let error = match result {
            Ok(()) => {
                tracing::info!("completed");
                return;
            }
            Err(error) => error,
        };
        if claim_lost(&error) {
            tracing::warn!("stopped: {error}");
            return;
        }
        if error.is::<RunFailed>() {
            tracing::warn!("failed: {error}; run failed");
            return;
        }

        let released = if error.is::<Stopping>() {
            Some((Duration::ZERO, "released for another worker".to_string()))
        } else {
            error.downcast_ref::<Snooze>().map(|Snooze(delay)| {
                (
                    *delay,
                    format!("snoozed for {delay:?} (bounded by the run's deadline)"),
                )
            })
        };
        let client = self.client.lock().await;
        if let Some((delay, done)) = released {
            match client
                .execute(
                    "select resume.release_run($1, $2, $3)",
                    &[&self.id, &self.attempt, &delay.as_secs_f64()],
                )
                .await
            {
                Ok(_) => tracing::info!("{done}"),
                Err(e) => {
                    tracing::warn!("could not release ({e}); it continues after its lease expires")
                }
            }
            return;
        }

        let permanent = error.is::<Permanent>();
        let ended = client
            .query_one(
                "select resume.end_attempt($1, $2, $3, $4)",
                &[&self.id, &self.attempt, &error.to_string(), &permanent],
            )
            .await
            .and_then(|row| row.try_get::<_, Option<f64>>(0));
        match ended {
            Ok(Some(seconds)) => tracing::warn!("failed: {error}; retry in {seconds:.1}s"),
            Ok(None) if permanent => tracing::warn!("failed: {error}; permanent; run failed"),
            Ok(None) => tracing::warn!("failed: {error}; attempt limit reached; run failed"),
            Err(e) if e.code().is_some_and(|c| c.code() == "RS002") => {
                tracing::warn!("failed: {error}; not recorded: {e}")
            }
            Err(e) => tracing::warn!(
                "failed: {error}; could not record it ({e}), so it retries after the lease expires"
            ),
        }
    }
}

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

/// A lost claim cannot record further progress or errors.
fn claim_lost(error: &crate::Error) -> bool {
    error
        .downcast_ref::<tokio_postgres::Error>()
        .and_then(tokio_postgres::Error::code)
        .is_some_and(|code| code.code() == "RS002")
}

fn step_span(key: &str) -> tracing::Span {
    tracing::info_span!("step", key)
}

/// Locks `resource` (for example "customer:42") until the step's transaction ends, so steps
/// naming the same resource run one at a time, even across runs and workflows. A crashed
/// worker's lock is released when its connection closes. Take it before checking state, so
/// the check and the action it guards happen under the same lock. Names are hashed to 64 bits,
/// so two names sharing a lock is possible but very unlikely.
pub async fn lock_resource(tx: &Transaction<'_>, resource: &str) -> Result<()> {
    tx.execute(
        "select pg_advisory_xact_lock(hashtextextended($1, 0))",
        &[&resource],
    )
    .await?;
    Ok(())
}

/// The run was marked failed, so no later attempt will retry it.
#[derive(Debug)]
struct RunFailed(String);

/// The worker is stopping, so the run stops before its next step and is released.
#[derive(Debug)]
struct Stopping;

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

impl std::error::Error for RunFailed {}
impl std::error::Error for Stopping {}
