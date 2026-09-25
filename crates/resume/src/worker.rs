use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio_postgres::Client;
use tracing::Instrument;

use crate::{Permanent, Result, Run, RunFailed, Stopping};

pub struct Worker {
    client: Client,
    workflow: String,
    lease: Duration,
    step_timeout: Duration,
}

impl Worker {
    /// Defaults to a 60 second lease and a 30 second step timeout.
    pub fn new(client: Client, workflow: impl Into<String>) -> Self {
        Self {
            client,
            workflow: workflow.into(),
            lease: Duration::from_secs(60),
            step_timeout: Duration::from_secs(30),
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
            "{workflow}: worker started (lease {:?}, step timeout {:?})",
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
                    () = tokio::time::sleep(Duration::from_millis(250)) => {}
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
                 from resume.claim_run($1, $2)",
                &[&self.workflow, &self.lease.as_secs_f64()],
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
