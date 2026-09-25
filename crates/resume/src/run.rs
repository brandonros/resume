use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio_postgres::{Client, GenericClient, Transaction};
use tracing::Instrument;

use crate::{Result, RunFailed, Stopping};

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
            let (tx, existing, _) = self.begin_step(client, idempotency_key).await?;
            if let Some(output) = existing {
                tx.commit().await?;
                tracing::info!("using saved result");
                return Ok(output);
            }

            tracing::info!("executing");
            let output = self.timed(idempotency_key, action(&tx)).await?;
            let saved = self.save_step(&tx, idempotency_key, &output).await?;
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
            let (tx, existing, _) = self.begin_step(client, idempotency_key).await?;
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
            let saved = self.save_step(&tx, idempotency_key, &output).await?;
            tx.commit().await?;
            tracing::info!("committed");
            Ok(saved)
        }
        .instrument(step_span(idempotency_key))
        .await
    }

    /// For a change that a newer request replaces, such as setting a customer's plan. The
    /// action runs only if no newer run of this workflow has the same subject (see
    /// `Producer::submit_for`); otherwise the step returns None and saves nothing, because a
    /// newer run makes the change. The subject stays locked until the step commits, so a newer
    /// run's step waits and applies after this one.
    pub async fn step_latest(
        &self,
        client: &mut Client,
        idempotency_key: &str,
        action: impl AsyncFnOnce(&Transaction<'_>) -> Result<Value>,
    ) -> Result<Option<Value>> {
        async {
            let (tx, existing, _) = self.begin_step(client, idempotency_key).await?;
            if let Some(output) = existing {
                tx.commit().await?;
                tracing::info!("using saved result");
                return Ok(Some(output));
            }

            let latest: bool = tx
                .query_one("select resume.lock_subject($1)", &[&self.id])
                .await?
                .try_get(0)?;
            if !latest {
                // Nothing to keep: a replay checks again and finds the same newer run.
                tx.rollback().await?;
                tracing::info!("superseded by a newer run");
                return Ok(None);
            }

            tracing::info!("executing");
            let output = self.timed(idempotency_key, action(&tx)).await?;
            let saved = self.save_step(&tx, idempotency_key, &output).await?;
            tx.commit().await?;
            tracing::info!("committed");
            Ok(Some(saved))
        }
        .instrument(step_span(idempotency_key))
        .await
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
            let (tx, existing, interrupted) = self.begin_step(client, idempotency_key).await?;
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
            let saved = self.save_step(&*client, idempotency_key, &output).await?;
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
    /// whether an earlier attempt started it without completing it.
    async fn begin_step<'c>(
        &self,
        client: &'c mut Client,
        key: &str,
    ) -> Result<(Transaction<'c>, Option<Value>, bool)> {
        // A stopping worker lets the step in progress finish and starts no more.
        if self.stopping.load(Ordering::Relaxed) {
            return Err(Stopping.into());
        }
        let tx = client.transaction().await?;
        let row = tx
            .query_one(
                "select output, interrupted from resume.begin_step($1, $2, $3, $4)",
                &[&self.id, &self.attempt, &key, &self.lease.as_secs_f64()],
            )
            .await?;
        Ok((tx, row.try_get("output")?, row.try_get("interrupted")?))
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

    async fn save_step(&self, db: &impl GenericClient, key: &str, output: &Value) -> Result<Value> {
        Ok(db
            .query_one(
                "select resume.save_step($1, $2, $3, $4)",
                &[&self.id, &self.attempt, &key, output],
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
