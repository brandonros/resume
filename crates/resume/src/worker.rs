//! The consumer side: a Worker claims runs and executes them, and a Run is the handle workflow
//! code uses to execute its steps.

use std::fmt;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio_postgres::{Client, GenericClient, Transaction};
use tracing::Instrument;

use crate::{Check, Permanent, Result, RunFailed};

pub struct Worker {
    client: Client,
    workflow: String,
    version: String,
    lease: Duration,
    step_timeout: Duration,
    poll_interval: Duration,
}

impl Worker {
    /// Claims only runs submitted for `version`, so a run's input and its steps come from the
    /// same code. When a workflow's input or steps change, bump the version: run new workers
    /// beside the old ones, switch producers, and retire the old workers once their runs finish.
    /// Defaults to a 60 second lease and a 30 second step timeout.
    pub fn new(client: Client, workflow: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            client,
            workflow: workflow.into(),
            version: version.into(),
            lease: Duration::from_secs(60),
            step_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(250),
        }
    }

    /// How long a claim lasts without progress. Each step renews it, so it must outlast one
    /// step, not the whole run. A crashed worker's run is retried once its lease expires.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// How long one step's action may take. It must be shorter than the lease, leaving time
    /// to save the result.
    pub fn step_timeout(mut self, step_timeout: Duration) -> Self {
        self.step_timeout = step_timeout;
        self
    }

    /// How long to wait before checking again when no run is ready. Defaults to 250 ms and
    /// must be greater than zero. Longer intervals reduce idle database queries but can delay
    /// picking up new work. Ready runs are processed without this delay.
    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Claims and executes runs until `shutdown` resolves. The run in progress then finishes
    /// its current step and is released, so another worker continues it at once.
    pub async fn run(
        mut self,
        shutdown: impl Future<Output = ()>,
        mut execute: impl AsyncFnMut(&mut Client, &Run) -> Result<()>,
    ) -> Result<()> {
        if self.step_timeout >= self.lease {
            return Err("step timeout must be shorter than the lease".into());
        }
        if self.poll_interval.is_zero() {
            return Err("poll interval must be greater than zero".into());
        }
        // If this worker hangs or its host disappears mid-step, Postgres ends the step's
        // transaction, releasing the run's row lock, instead of holding it until the connection
        // closes. A healthy step finishes within the lease, since the step timeout is shorter.
        let lease_ms = self.lease.as_millis();
        self.client
            .batch_execute(&format!(
                "set idle_in_transaction_session_timeout = {lease_ms};
                 set statement_timeout = {lease_ms};
                 set tcp_keepalives_idle = 10;
                 set tcp_keepalives_interval = 5;
                 set tcp_keepalives_count = 3;"
            ))
            .await?;

        let workflow = self.workflow.clone();
        let stopping = Arc::new(AtomicBool::new(false));
        let mut shutdown = pin!(shutdown);
        tracing::info!(
            "{workflow}: worker started (version {}, lease {:?}, step timeout {:?})",
            self.version,
            self.lease,
            self.step_timeout
        );
        let mut waiting = false;
        while !stopping.load(Ordering::Relaxed) {
            let Some((run, expired)) = self.claim_run(&stopping).await? else {
                if !waiting {
                    tracing::info!("{workflow}: waiting for work");
                    waiting = true;
                }
                tokio::select! {
                    () = tokio::time::sleep(self.poll_interval) => {}
                    () = &mut shutdown => stopping.store(true, Ordering::Relaxed),
                }
                continue;
            };

            waiting = false;
            // Every log line about this run, including its steps', carries these fields.
            let span = tracing::info_span!(
                "run",
                workflow = %workflow,
                id = run.id,
                attempt = run.number()
            );
            let claimed = format!("claimed ({}/{})", run.number(), run.max_attempts);
            if expired {
                tracing::warn!(parent: &span, "{claimed}; the previous attempt's lease expired");
            } else {
                tracing::info!(parent: &span, "{claimed}");
            }
            let result = {
                let mut work = pin!(
                    async {
                        execute(&mut self.client, &run).await?;
                        run.complete_run(&self.client).await
                    }
                    .instrument(span.clone())
                );
                let finished = tokio::select! {
                    result = &mut work => Some(result),
                    () = &mut shutdown => None,
                };
                match finished {
                    Some(result) => result,
                    None => {
                        tracing::info!(parent: &span, "worker stopping; finishing the current step");
                        stopping.store(true, Ordering::Relaxed);
                        work.await
                    }
                }
            };
            self.settle(&run, result).instrument(span).await;
        }
        tracing::info!("{workflow}: worker stopped");
        Ok(())
    }

    /// Returns the claimed run, and whether the previous attempt's lease expired.
    async fn claim_run(&self, stopping: &Arc<AtomicBool>) -> Result<Option<(Run, bool)>> {
        let row = self
            .client
            .query_opt(
                "select id, idempotency_key, input, attempt, released, max_attempts, expired
                 from resume.claim_run($1, $2, $3)",
                &[&self.workflow, &self.version, &self.lease.as_secs_f64()],
            )
            .await?;

        row.map(|row| {
            let run = Run {
                id: row.try_get("id")?,
                idempotency_key: row.try_get("idempotency_key")?,
                input: row.try_get("input")?,
                attempt: row.try_get("attempt")?,
                released: row.try_get("released")?,
                max_attempts: row.try_get("max_attempts")?,
                lease: self.lease,
                step_timeout: self.step_timeout,
                stopping: stopping.clone(),
                position: AtomicI32::new(0),
            };
            Ok((run, row.try_get("expired")?))
        })
        .transpose()
    }

    /// Records how the attempt ended: a retryable error schedules a retry after a backoff,
    /// a permanent one fails the run, and a stopping worker releases it.
    async fn settle(&self, run: &Run, result: Result<()>) {
        let error = match result {
            Ok(()) => {
                tracing::info!("completed");
                return;
            }
            Err(error) => error,
        };

        if error.is::<Stopping>() {
            match run.release_run(&self.client).await {
                Ok(()) => tracing::info!("released for another worker"),
                Err(e) => {
                    tracing::warn!("could not release ({e}); it continues after its lease expires")
                }
            }
            return;
        }

        let next = if error.is::<RunFailed>() {
            Ok("run failed".to_string())
        } else if error.is::<Permanent>() {
            run.fail_run(&self.client, &error.to_string())
                .await
                .map(|()| "permanent; run failed".to_string())
        } else if run.number() >= i64::from(run.max_attempts) {
            run.fail_run(&self.client, &error.to_string())
                .await
                .map(|()| "attempt limit reached; run failed".to_string())
        } else {
            run.retry_run(&self.client, &error.to_string())
                .await
                .map(|seconds| format!("retry in {seconds:.1}s"))
        };
        match next {
            Ok(next) => tracing::warn!("failed: {error}; {next}"),
            Err(e) => tracing::warn!(
                "failed: {error}; could not record it ({e}), so it retries after the lease expires"
            ),
        }
    }
}

/// Resolves on Ctrl-C or, on Unix, SIGTERM, the signal process managers send to stop a
/// process. A second Ctrl-C exits at once.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::warn!("cannot listen for SIGTERM: {error}");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;

    tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::warn!("interrupted again; exiting now");
        std::process::exit(130);
    });
}

pub struct Run {
    pub id: i64,
    pub idempotency_key: String,
    pub input: Value,
    attempt: i64,
    released: i32,
    max_attempts: i32,
    lease: Duration,
    step_timeout: Duration,
    stopping: Arc<AtomicBool>,
    /// The position the next step will have in this attempt.
    position: AtomicI32,
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
    fn number(&self) -> i64 {
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

    async fn complete_run(&self, db: &impl GenericClient) -> Result<()> {
        db.execute(
            "select resume.complete_run($1, $2)",
            &[&self.id, &self.attempt],
        )
        .await?;
        Ok(())
    }

    async fn fail_run(&self, db: &impl GenericClient, error: &str) -> Result<()> {
        db.execute(
            "select resume.fail_run($1, $2, $3)",
            &[&self.id, &self.attempt, &error],
        )
        .await?;
        Ok(())
    }

    /// Returns the delay in seconds before the next attempt.
    async fn retry_run(&self, db: &impl GenericClient, error: &str) -> Result<f64> {
        Ok(db
            .query_one(
                "select resume.retry_run($1, $2, $3)",
                &[&self.id, &self.attempt, &error],
            )
            .await?
            .try_get(0)?)
    }

    async fn release_run(&self, db: &impl GenericClient) -> Result<()> {
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

/// Locks `resource` (for example "customer:42") until the step's transaction ends, so steps
/// naming the same resource run one at a time, even across runs and workflows. A crashed
/// worker's lock is released when its connection closes. Take it before checking state, so
/// the check and the action it guards happen under the same lock.
pub async fn lock_resource(tx: &Transaction<'_>, resource: &str) -> Result<()> {
    tx.execute("select resume.lock_resource($1)", &[&resource])
        .await?;
    Ok(())
}

/// The worker is stopping, so the run stops before its next step and is released.
#[derive(Debug)]
struct Stopping;

impl fmt::Display for Stopping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("worker is stopping")
    }
}

impl std::error::Error for Stopping {}
