use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio_postgres::{Client, Transaction};
use tracing::Instrument;

use super::Job;
use crate::error::{ErrorKind, classify};
use crate::{Permanent, Result, RunFailed, Stopping};

/// Executes the durable steps of one claimed attempt, one at a time.
/// The worker lends this executor exclusively to the handler; job data is borrowed separately.
/// Steps require mutable access, so overlapping steps are rejected at compile time:
///
/// ```compile_fail,E0499
/// async fn concurrent(execution: &mut resume::Execution<'_>) -> resume::Result<()> {
///     let first = execution.step("first", async |_| Ok(serde_json::json!(1)));
///     let second = execution.step("second", async |_| Ok(serde_json::json!(2)));
///     first.await?;
///     second.await?;
///     Ok(())
/// }
/// ```
///
/// A step cannot start another step on the same executor inside its action either:
///
/// ```compile_fail
/// async fn nested(execution: &mut resume::Execution<'_>) -> resume::Result<()> {
///     execution.step("outer", async |_| {
///         execution.step("inner", async |_| Ok(serde_json::json!(1))).await
///     }).await?;
///     Ok(())
/// }
/// ```
pub struct Execution<'a> {
    job: &'a Job,
    client: &'a mut Client,
    stopping: &'a AtomicBool,
    lease: Duration,
    step_timeout: Duration,
    position: i32,
    keys: HashSet<String>,
}

impl<'a> Execution<'a> {
    pub(crate) fn new(
        job: &'a Job,
        client: &'a mut Client,
        stopping: &'a AtomicBool,
        lease: Duration,
        step_timeout: Duration,
    ) -> Self {
        Self {
            job,
            client,
            stopping,
            lease,
            step_timeout,
            position: 0,
            keys: HashSet::new(),
        }
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
        &mut self,
        key: &str,
        action: impl AsyncFnOnce(&Transaction<'_>) -> Result<Value>,
    ) -> Result<Value> {
        async {
            let job = self.job;
            let timeout = self.step_timeout;
            let (tx, saved) = self.begin_step(key).await?;
            if let Some(output) = saved {
                tx.commit().await?;
                tracing::info!("using saved result");
                return Ok(output);
            }

            tracing::info!("executing");
            let output = timed(timeout, key, action(&tx)).await?;
            let saved = save_step(job, &tx, key, &output).await?;
            tx.commit().await?;
            tracing::info!("committed");
            Ok(saved)
        }
        .instrument(step_span(key))
        .await
    }

    /// For external effects that must not repeat, such as a vendor without idempotency.
    /// The action runs at most once per run. Once it has started, the run never calls it
    /// again: if the attempt dies, or the action times out or returns any error (including
    /// `Snooze`), the outcome is unknown and the run fails at once, without retrying. Someone
    /// must check the vendor and pass the output to resume.reopen_run. Do readiness checks
    /// and anything that may legitimately fail in a regular step before this one.
    pub async fn step_once(
        &mut self,
        key: &str,
        action: impl AsyncFnOnce() -> Result<Value>,
    ) -> Result<Value> {
        async {
            let job = self.job;
            let timeout = self.step_timeout;
            let (tx, saved) = self.begin_step(key).await?;
            // Commit the start before calling, so a later attempt knows the call may have happened.
            tx.commit().await?;
            if let Some(output) = saved {
                tracing::info!("using saved result");
                return Ok(output);
            }

            tracing::info!("executing once");
            // The start is committed, so a retry could never call the action again: it would
            // only pay a backoff before begin_step fails the run for the unknown outcome.
            let output = timed(timeout, key, action())
                .await
                .map_err(|error| -> crate::Error {
                    let hint = if matches!(classify(error.as_ref()), ErrorKind::Snooze(_)) {
                        "; use a regular step for readiness checks"
                    } else {
                        ""
                    };
                    Permanent(format!(
                        "step {key} started and its outcome is unknown ({error}){hint}; \
                     check the effect and pass the output to resume.reopen_run"
                    ))
                    .into()
                })?;

            // save_step checks the claim itself, so this needs no transaction of its own.
            let saved = save_step(job, &*self.client, key, &output).await?;
            tracing::info!("committed");
            Ok(saved)
        }
        .instrument(step_span(key))
        .await
    }

    /// Starts a transaction holding the run's lock, after checking this attempt owns it, and
    /// records the step's start. Returns the step's saved output if it already completed.
    /// If the run is past its deadline, or a step_once action's outcome is unknown, commits
    /// the run's failure and returns `RunFailed`.
    async fn begin_step(&mut self, key: &str) -> Result<(Transaction<'_>, Option<Value>)> {
        // A stopping worker lets the step in progress finish and starts no more.
        if self.stopping.load(Ordering::Relaxed) {
            return Err(Stopping.into());
        }
        if !self.keys.insert(key.to_string()) {
            return Err(Permanent(format!(
                "step key {key} is used twice; keys must be unique within a run"
            ))
            .into());
        }
        let position = self.position;
        self.position += 1;
        let tx = self.client.transaction().await?;
        let row = tx
            .query_one(
                "select output, failed from resume.begin_step($1, $2, $3, $4, $5)",
                &[
                    &self.job.id,
                    &self.job.attempt(),
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

    /// Marks the run complete. The database refuses if the attempt's history disagrees with
    /// the code: a step_once whose error the handler swallowed, or recorded steps this attempt
    /// never reached. That is a permanent failure, settled like any other step error.
    pub(crate) async fn complete_run(&self) -> Result<()> {
        let next_position = self.position;
        self.client
            .execute(
                "select resume.complete_run($1, $2, $3)",
                &[&self.job.id, &self.job.attempt(), &next_position],
            )
            .await
            .map_err(permanent_if_workflow_changed)?;
        Ok(())
    }
}

/// Bounds the action's duration. A timeout cannot undo external effects.
async fn timed<T>(
    timeout: Duration,
    key: &str,
    action: impl Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(timeout, action).await {
        Ok(result) => result,
        Err(_) => Err(format!("step {key} timed out after {:?}", timeout).into()),
    }
}

async fn save_step(
    job: &Job,
    db: &impl tokio_postgres::GenericClient,
    key: &str,
    output: &Value,
) -> Result<Value> {
    Ok(db
        .query_one(
            "select resume.save_step($1, $2, $3, $4)",
            &[&job.id, &job.attempt(), &key, output],
        )
        .await?
        .try_get(0)?)
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
