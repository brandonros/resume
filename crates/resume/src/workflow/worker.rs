//! Workers claim and execute workflow runs until shutdown.

use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio_postgres::Client;
use tracing::Instrument;

use crate::Result;
use crate::error::{ErrorKind, classify};
use crate::executor::{Execution, Job};

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

    /// Claims and executes runs until `shutdown` resolves. Once shutdown is observed, the
    /// current step finishes and the next step stops the run so another worker can continue
    /// it at once. A handler that finishes its last step can still complete the run.
    /// Shutdown is cooperative: the handler must propagate step errors with `?` and return.
    /// Put I/O and external effects inside `step` or `step_once`; waits between steps have
    /// no timeout and can delay shutdown indefinitely. Step timeouts require actions to yield
    /// to the async runtime; synchronous blocking work must not block the runtime thread.
    ///
    /// Claim queries retry serialization failures, deadlocks, lock unavailability, and query
    /// cancellation after the poll interval. Other claim errors and closed connections are
    /// returned to the caller, including schema and permission errors.
    ///
    /// The handler receives immutable job data and exclusive access to its step executor:
    ///
    /// ```no_run
    /// # async fn example(client: tokio_postgres::Client) -> resume::Result<()> {
    /// resume::Worker::new(client, "example", "1")
    ///     .run(resume::shutdown_signal(), async |job, execution| {
    ///         execution.step("save", async |tx| {
    ///             tx.execute("insert into results (id, input) values ($1, $2)",
    ///                 &[&job.id, &job.input]).await?;
    ///             Ok(serde_json::json!(true))
    ///         }).await?;
    ///         Ok(())
    ///     }).await
    /// # }
    /// ```
    pub async fn run(
        mut self,
        shutdown: impl Future<Output = ()>,
        mut execute: impl AsyncFnMut(&Job, &mut Execution<'_>) -> Result<()>,
    ) -> Result<()> {
        if self.step_timeout >= self.lease {
            return Err("step timeout must be shorter than the lease".into());
        }
        if self.poll_interval.is_zero() {
            return Err("poll interval must be greater than zero".into());
        }
        // Bound transaction and statement waits so a hung worker releases the run's row lock.
        // The shorter step timeout leaves healthy workers time to save and commit.
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
        let stopping = AtomicBool::new(false);
        let mut shutdown = pin!(shutdown);
        tracing::info!(
            "{workflow}: worker started (version {}, lease {:?}, step timeout {:?})",
            self.version,
            self.lease,
            self.step_timeout
        );
        let mut waiting = false;
        while !stopping.load(Ordering::Relaxed) {
            let claimed = match self.claim_run().await {
                Ok(claimed) => claimed,
                Err(error) if self.client.is_closed() => return Err(error),
                Err(error) if classify(error.as_ref()) == ErrorKind::TransientDatabase => {
                    tracing::warn!("{workflow}: could not claim ({error}); trying again");
                    None
                }
                Err(error) => return Err(error),
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
            let span = tracing::info_span!(
                "run",
                workflow = %workflow,
                id = run.id,
                attempt = run.attempts_used()
            );
            let claimed = format!("claimed ({}/{})", run.attempts_used(), run.max_attempts());
            if expired {
                tracing::warn!(parent: &span, "{claimed}; the previous attempt's lease expired");
            } else {
                tracing::info!(parent: &span, "{claimed}");
            }
            let result = {
                let mut execution = Execution::new(
                    &run,
                    &mut self.client,
                    &stopping,
                    self.lease,
                    self.step_timeout,
                );
                let mut work = pin!(
                    async {
                        execute(&run, &mut execution).await?;
                        execution.complete_run().await
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

    /// Releases stopped or snoozing runs; records other errors for retry or terminal failure.
    async fn settle(&self, run: &Job, result: Result<()>) {
        let error = match result {
            Ok(()) => {
                tracing::info!("completed");
                return;
            }
            Err(error) => error,
        };
        let kind = classify(error.as_ref());
        let released = match kind {
            ErrorKind::ClaimLost => {
                tracing::warn!("stopped: {error}");
                return;
            }
            ErrorKind::RunFailed => {
                tracing::warn!("failed: {error}; run failed");
                return;
            }
            ErrorKind::Stopping => {
                Some((Duration::ZERO, "released for another worker".to_string()))
            }
            ErrorKind::Snooze(delay) => Some((
                delay,
                format!("snoozed for {delay:?} (bounded by the run's deadline)"),
            )),
            _ => None,
        };
        let client = &self.client;
        if let Some((delay, done)) = released {
            match client
                .execute(
                    "select resume.release_run($1, $2, $3)",
                    &[&run.id, &run.attempt(), &delay.as_secs_f64()],
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

        let permanent = kind == ErrorKind::Permanent;
        let ended = client
            .query_one(
                "select resume.fail_attempt($1, $2, $3, $4)",
                &[&run.id, &run.attempt(), &error.to_string(), &permanent],
            )
            .await
            .and_then(|row| row.try_get::<_, Option<f64>>(0));
        match ended {
            Ok(Some(seconds)) => tracing::warn!("failed: {error}; retry in {seconds:.1}s"),
            Ok(None) if permanent => tracing::warn!("failed: {error}; permanent; run failed"),
            Ok(None) => tracing::warn!("failed: {error}; attempt limit reached; run failed"),
            Err(e) if classify(&e) == ErrorKind::ClaimLost => {
                tracing::warn!("failed: {error}; not recorded: {e}")
            }
            Err(e) => tracing::warn!(
                "failed: {error}; could not record it ({e}), so it retries after the lease expires"
            ),
        }
    }

    /// Returns the claimed run, and whether the previous attempt's lease expired.
    async fn claim_run(&self) -> Result<Option<(Job, bool)>> {
        let client = &self.client;
        client
            .execute("select resume.expire_runs($1)", &[&self.workflow])
            .await?;
        let row = client
            .query_opt(
                "select id, idempotency_key, input, attempt, attempts_used, max_attempts, expired
                 from resume.claim_run($1, $2, $3)",
                &[&self.workflow, &self.version, &self.lease.as_secs_f64()],
            )
            .await?;

        row.map(|row| {
            let run = Job::from_claim(&row)?;
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
