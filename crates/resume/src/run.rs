use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio_postgres::{Client, GenericClient, Transaction};
use tracing::Instrument;

use crate::{Check, Permanent, Result, RunFailed, Stopping};

pub struct Run {
    pub id: i64,
    pub idempotency_key: String,
    pub input: Value,
    pub(crate) attempt: i64,
    pub(crate) released: i32,
    pub(crate) max_attempts: i32,
    pub(crate) lease: Duration,
    pub(crate) step_timeout: Duration,
    pub(crate) stopping: Arc<AtomicBool>,
    /// The position the next step will have in this attempt.
    pub(crate) position: AtomicI32,
}

impl Run {
    /// Use a key that is unique within the run and stable across attempts; it may be dynamic.
    /// Use the supplied transaction for the step's database effects.
    /// Those effects and the saved result commit together. External effects may repeat.
    pub async fn step(
        &self,
        client: &mut Client,
        idempotency_key: &str,
        action: impl AsyncFnOnce(&Transaction<'_>) -> Result<Value>,
    ) -> Result<Value> {
        async {
            let (tx, existing, _, _) = self.begin_step(client, idempotency_key).await?;
            if let Some(output) = existing {
                tx.commit().await?;
                tracing::info!("using saved result");
                return Ok(output);
            }

            tracing::info!("executing");
            let output = self.timed(idempotency_key, action(&tx)).await?;
            let saved = self.save_step(&tx, idempotency_key, &output, None).await?;
            tx.commit().await?;
            tracing::info!("committed");
            Ok(saved)
        }
        .instrument(step_span(idempotency_key))
        .await
    }

    /// Makes sure an effect exists. `check` looks for it and `action` runs only when `check`
    /// returns None; whichever output they return is saved. Both receive `context`, so they
    /// can share a client that two closures could not both capture, and the step's transaction.
    /// If the worker dies after `action` and before the commit, the retry runs `check` again,
    /// so a check that asks the vendor finds the effect instead of repeating it.
    pub async fn ensure<C>(
        &self,
        client: &mut Client,
        idempotency_key: &str,
        context: &mut C,
        check: impl AsyncFnOnce(&mut C, &Transaction<'_>) -> Result<Option<Value>>,
        action: impl AsyncFnOnce(&mut C, &Transaction<'_>) -> Result<Value>,
    ) -> Result<Value> {
        async {
            let (tx, existing, _, _) = self.begin_step(client, idempotency_key).await?;
            if let Some(output) = existing {
                tx.commit().await?;
                tracing::info!("using saved result");
                return Ok(output);
            }

            let output = self
                .timed(idempotency_key, async {
                    match check(&mut *context, &tx).await? {
                        Some(output) => {
                            tracing::info!("already done");
                            Ok(output)
                        }
                        None => {
                            tracing::info!("executing");
                            action(&mut *context, &tx).await
                        }
                    }
                })
                .await?;
            let saved = self.save_step(&tx, idempotency_key, &output, None).await?;
            tx.commit().await?;
            tracing::info!("committed");
            Ok(saved)
        }
        .instrument(step_span(idempotency_key))
        .await
    }

    /// For a change the current state must still allow, such as shipping an order only while it
    /// is paid. `check` decides, in the same transaction as `action`, so nothing can change in
    /// between as long as `check` locks what it reads: `select ... for update` for rows,
    /// `lock_resource` for anything else. `Check::Skip` saves the reason and returns None, now and
    /// on every replay; `Check::Fail` fails the run. Both closures receive `context`, as in
    /// `ensure`.
    pub async fn step_if<C>(
        &self,
        client: &mut Client,
        idempotency_key: &str,
        context: &mut C,
        check: impl AsyncFnOnce(&mut C, &Transaction<'_>) -> Result<Check>,
        action: impl AsyncFnOnce(&mut C, &Transaction<'_>) -> Result<Value>,
    ) -> Result<Option<Value>> {
        async {
            let (tx, existing, _, skipped) = self.begin_step(client, idempotency_key).await?;
            if let Some(output) = existing {
                tx.commit().await?;
                return Ok(match skipped {
                    Some(reason) => {
                        tracing::info!("skipped earlier: {reason}");
                        None
                    }
                    None => {
                        tracing::info!("using saved result");
                        Some(output)
                    }
                });
            }

            let outcome = self
                .timed(idempotency_key, async {
                    match check(&mut *context, &tx).await? {
                        Check::Proceed => {
                            tracing::info!("executing");
                            Ok(Ok(action(&mut *context, &tx).await?))
                        }
                        Check::Skip(reason) => Ok(Err(reason)),
                        Check::Fail(reason) => Err(Permanent(reason).into()),
                    }
                })
                .await?;
            let saved = match outcome {
                Ok(output) => Some(self.save_step(&tx, idempotency_key, &output, None).await?),
                Err(reason) => {
                    self.save_step(&tx, idempotency_key, &Value::Null, Some(&reason))
                        .await?;
                    tracing::info!("skipped: {reason}");
                    None
                }
            };
            tx.commit().await?;
            if saved.is_some() {
                tracing::info!("committed");
            }
            Ok(saved)
        }
        .instrument(step_span(idempotency_key))
        .await
    }

    /// Whether no newer run of this workflow has the same subject (see `Producer::submit_for`),
    /// for use in `step_if`'s check. Locks the subject until the step's transaction ends, so a
    /// newer run's step waits for this one to commit and applies after it.
    pub async fn is_latest(&self, tx: &Transaction<'_>) -> Result<bool> {
        Ok(tx
            .query_one("select resume.lock_subject($1)", &[&self.id])
            .await?
            .try_get(0)?)
    }

    /// For external effects that must not repeat, such as a vendor without idempotency.
    /// The action runs at most once per run. If an attempt dies or times out after starting it
    /// and before saving its result, the outcome is unknown: the run fails instead of calling
    /// again, and someone must check the vendor and call resume.resolve_step.
    pub async fn step_once(
        &self,
        client: &mut Client,
        idempotency_key: &str,
        action: impl AsyncFnOnce() -> Result<Value>,
    ) -> Result<Value> {
        async {
            let (tx, existing, interrupted, _) = self.begin_step(client, idempotency_key).await?;
            if let Some(output) = existing {
                tx.commit().await?;
                tracing::info!("using saved result");
                return Ok(output);
            }

            if interrupted {
                let error = RunFailed(format!(
                    "step {idempotency_key} started in an earlier attempt and its outcome is unknown"
                ));
                self.fail_run(&tx, &error.0).await?;
                tx.commit().await?;
                return Err(error.into());
            }
            // Commit the start before calling, so a later attempt knows the call may have happened.
            tx.commit().await?;

            tracing::info!("executing once");
            let output = self.timed(idempotency_key, action()).await?;

            // save_step checks the claim itself, so this needs no transaction of its own.
            let saved = self.save_step(&*client, idempotency_key, &output, None).await?;
            tracing::info!("committed");
            Ok(saved)
        }
        .instrument(step_span(idempotency_key))
        .await
    }

    /// This attempt's number for logs: claims so far, less claims given back.
    pub(crate) fn number(&self) -> i64 {
        self.attempt - i64::from(self.released)
    }

    /// Starts a transaction holding the run's lock, after checking this attempt owns it, and
    /// records the step's start. Returns the step's saved output if it already completed, and
    /// whether an earlier attempt started it without completing it, and why step_if skipped it,
    /// if it did. Fails the run if it is past its deadline, or if the step's position changed
    /// since the run first reached it.
    async fn begin_step<'c>(
        &self,
        client: &'c mut Client,
        key: &str,
    ) -> Result<(Transaction<'c>, Option<Value>, bool, Option<String>)> {
        // A stopping worker lets the step in progress finish and starts no more.
        if self.stopping.load(Ordering::Relaxed) {
            return Err(Stopping.into());
        }
        let position = self.position.fetch_add(1, Ordering::Relaxed);
        let tx = client.transaction().await?;
        let row = tx
            .query_one(
                "select output, interrupted, past_deadline, skipped
                 from resume.begin_step($1, $2, $3, $4, $5)",
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
        if row.try_get("past_deadline")? {
            let error = RunFailed("the run passed its deadline".into());
            self.fail_run(&tx, &error.0).await?;
            tx.commit().await?;
            return Err(error.into());
        }
        Ok((
            tx,
            row.try_get("output")?,
            row.try_get("interrupted")?,
            row.try_get("skipped")?,
        ))
    }

    /// Fails the step if its action takes longer than the step timeout. Timing out stops the
    /// wait but cannot undo what the action already did, so the outcome is unknown, as after
    /// a crash.
    async fn timed<T>(&self, key: &str, action: impl Future<Output = Result<T>>) -> Result<T> {
        match tokio::time::timeout(self.step_timeout, action).await {
            Ok(result) => result,
            Err(_) => Err(format!("step {key} timed out after {:?}", self.step_timeout).into()),
        }
    }

    async fn save_step(
        &self,
        db: &impl GenericClient,
        key: &str,
        output: &Value,
        skipped: Option<&str>,
    ) -> Result<Value> {
        Ok(db
            .query_one(
                "select resume.save_step($1, $2, $3, $4, $5)",
                &[&self.id, &self.attempt, &key, output, &skipped],
            )
            .await?
            .try_get(0)?)
    }

    pub(crate) async fn complete_run(&self, db: &impl GenericClient) -> Result<()> {
        db.execute(
            "select resume.complete_run($1, $2)",
            &[&self.id, &self.attempt],
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn fail_run(&self, db: &impl GenericClient, error: &str) -> Result<()> {
        db.execute(
            "select resume.fail_run($1, $2, $3)",
            &[&self.id, &self.attempt, &error],
        )
        .await?;
        Ok(())
    }

    /// Returns the delay in seconds before the next attempt.
    pub(crate) async fn retry_run(&self, db: &impl GenericClient, error: &str) -> Result<f64> {
        Ok(db
            .query_one(
                "select resume.retry_run($1, $2, $3)",
                &[&self.id, &self.attempt, &error],
            )
            .await?
            .try_get(0)?)
    }

    pub(crate) async fn release_run(&self, db: &impl GenericClient) -> Result<()> {
        db.execute(
            "select resume.release_run($1, $2)",
            &[&self.id, &self.attempt],
        )
        .await?;
        Ok(())
    }
}

/// Log lines inside a step carry its key, under the worker's run span.
fn step_span(key: &str) -> tracing::Span {
    tracing::info_span!("step", key)
}
