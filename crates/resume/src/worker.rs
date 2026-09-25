//! The consumer side: a Worker claims runs and executes them, and a Run is the handle workflow
//! code uses to execute its steps.

use std::collections::HashSet;
use std::fmt;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Mutex, MutexGuard};
use tokio_postgres::{Client, Transaction};
use tracing::Instrument;

use crate::{Permanent, Result, Snooze};

pub struct Worker {
    /// Shared with the claimed run, which uses it for its steps.
    client: Arc<Mutex<Client>>,
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
            client: Arc::new(Mutex::new(client)),
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

    /// How long one step's action may take. It covers only the action, not starting, saving
    /// or committing the step, which the lease bounds through Postgres's statement timeout. It
    /// must be shorter than the lease, leaving time to save the result.
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
        self,
        shutdown: impl Future<Output = ()>,
        mut execute: impl AsyncFnMut(&Run) -> Result<()>,
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
            .lock()
            .await
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
            let claimed = match self.claim_run(&stopping).await {
                Ok(claimed) => claimed,
                Err(error) if self.client.lock().await.is_closed() => return Err(error),
                // A transient error, such as a statement timeout: try again after the interval.
                Err(error) => {
                    tracing::warn!("{workflow}: could not claim ({error}); trying again");
                    None
                }
            };
            let Some((run, expired)) = claimed else {
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
                        execute(&run).await?;
                        run.complete_run().await
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
            run.settle(result).instrument(span).await;
        }
        tracing::info!("{workflow}: worker stopped");
        Ok(())
    }

    /// Returns the claimed run, and whether the previous attempt's lease expired.
    async fn claim_run(&self, stopping: &Arc<AtomicBool>) -> Result<Option<(Run, bool)>> {
        let row = self
            .client
            .lock()
            .await
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
                keys: std::sync::Mutex::default(),
                client: self.client.clone(),
            };
            Ok((run, row.try_get("expired")?))
        })
        .transpose()
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
    /// The keys of the steps this attempt has started.
    keys: std::sync::Mutex<HashSet<String>>,
    client: Arc<Mutex<Client>>,
}

impl Run {
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

    /// This attempt's number for logs: claims so far, less claims given back.
    fn number(&self) -> i64 {
        self.attempt - i64::from(self.released)
    }

    /// The connection for a step. It is free unless another step of this run is in progress,
    /// started inside a step's action or alongside it, as with `join!`.
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

    async fn complete_run(&self) -> Result<()> {
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

    /// Records how the attempt ended: a snoozing or stopping worker releases the run, and
    /// any other error schedules a retry after a backoff, or fails the run if the error is
    /// permanent or the attempt was its last.
    async fn settle(&self, result: Result<()>) {
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

/// Whether this attempt no longer owns the run: it was cancelled or reclaimed after its lease
/// expired, so there is nothing left to record.
fn claim_lost(error: &crate::Error) -> bool {
    error
        .downcast_ref::<tokio_postgres::Error>()
        .and_then(tokio_postgres::Error::code)
        .is_some_and(|code| code.code() == "RS002")
}

/// Log lines inside a step carry its key, under the worker's run span.
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
