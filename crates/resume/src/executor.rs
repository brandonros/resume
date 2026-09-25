//! Claimed attempts, durable step transactions, and generic resource locks.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Mutex, MutexGuard};
use tokio_postgres::{Client, Row, Transaction};
use tracing::Instrument;

use crate::{Permanent, Result, RunFailed, Snooze, Stopping};

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

    pub(crate) fn attempt(&self) -> i64 {
        self.attempt
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
            .map_err(permanent_if_workflow_changed)?;
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

    /// Marks the run complete. The database refuses if the attempt's history disagrees with
    /// the code: a step_once whose error the handler swallowed, or recorded steps this attempt
    /// never reached. That is a permanent failure, settled like any other step error.
    pub(crate) async fn complete_run(&self) -> Result<()> {
        let next_position = self.position.load(Ordering::Relaxed);
        self.client
            .lock()
            .await
            .execute(
                "select resume.complete_run($1, $2, $3)",
                &[&self.id, &self.attempt, &next_position],
            )
            .await
            .map_err(permanent_if_workflow_changed)?;
        Ok(())
    }
}

/// RS001 means the workflow's code and the run's history disagree: never retry.
fn permanent_if_workflow_changed(error: tokio_postgres::Error) -> crate::Error {
    match error.as_db_error() {
        Some(db) if db.code().code() == "RS001" => Permanent(db.message().into()).into(),
        _ => error.into(),
    }
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
