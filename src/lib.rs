use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use serde_json::Value;
use tokio_postgres::{Client, GenericClient, Transaction};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// Data for the current job. Effects and nondeterministic decisions belong in steps.
pub struct Job {
    pub id: i64,
    /// Claim number, including attempts abandoned by crashed workers and resumed suspensions.
    pub attempt: i64,
    /// Reported attempt failures; use this rather than `attempt` for retry limits.
    pub failures: i64,
    pub key: String,
    pub input: Value,
}

/// Application decision after a reported attempt failure.
pub enum Retry {
    Stop,
    After(Duration),
}

/// Submit with a client or transaction. Submission commits with the caller's transaction.
pub async fn submit(
    db: &impl GenericClient,
    workflow: &str,
    key: &str,
    input: &Value,
) -> Result<i64> {
    Ok(db
        .query_one(
            "select resume.submit($1, $2, $3)",
            &[&workflow, &key, input],
        )
        .await?
        .try_get(0)?)
}

/// Submit a job that is not claimable before `at`. A duplicate keeps its original schedule.
/// For a recurring job, submit the next run from a step with a key derived from its time.
pub async fn submit_at(
    db: &impl GenericClient,
    workflow: &str,
    key: &str,
    input: &Value,
    at: SystemTime,
) -> Result<i64> {
    Ok(db
        .query_one(
            "select resume.submit($1, $2, $3, $4)",
            &[&workflow, &key, input, &at],
        )
        .await?
        .try_get(0)?)
}

/// Run jobs one at a time. Idle polls wait 250 milliseconds.
/// Recorded attempt errors go to `retry`; log them there. Infrastructure errors, including
/// failures to claim or record an outcome, return to the caller. Reconnect or restart there.
/// Run several workers, each with its own client, for concurrency.
pub async fn work(
    client: &mut Client,
    workflow: &str,
    handler: impl AsyncFn(&Job, &mut Steps<'_>) -> Result<Value>,
    retry: impl Fn(&Job, &Error) -> Retry,
) -> Result<()> {
    loop {
        match run_attempt(client, workflow, &handler, &retry).await? {
            Attempt::Processed => continue,
            Attempt::Idle | Attempt::Failed(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Process at most one job; return false when none is ready, true on completion or suspension.
/// The handler's output is saved on the job and returned to a parent's `wait_for`.
/// Handler or completion errors go to `retry`; the decision is saved before returning the error.
/// The callback receives the original error: use `downcast_ref` to classify application or
/// database errors, returning Stop for permanent failures and After for retryable ones.
/// Stop pauses the job; After schedules another attempt. The callback does not run on success
/// or idle polls. Database failures propagate; abandoned claims recover at lease expiry without
/// consulting this callback, so an application attempt limit is not a strict crash limit.
///
/// Use a dedicated client: this sets its statement and idle-transaction timeouts to
/// 60 seconds. Step actions have a 30-second timeout and must yield to the runtime.
/// Dropping this future abandons the attempt; it does not undo external effects.
pub async fn run_one(
    client: &mut Client,
    workflow: &str,
    handler: impl AsyncFnOnce(&Job, &mut Steps<'_>) -> Result<Value>,
    retry: impl FnOnce(&Job, &Error) -> Retry,
) -> Result<bool> {
    match run_attempt(client, workflow, handler, retry).await? {
        Attempt::Idle => Ok(false),
        Attempt::Processed => Ok(true),
        Attempt::Failed(error) => Err(error),
    }
}

// Failed means the retry decision was successfully persisted; Err means it wasn't handled.
enum Attempt {
    Idle,
    Processed,
    Failed(Error),
}

async fn run_attempt(
    client: &mut Client,
    workflow: &str,
    handler: impl AsyncFnOnce(&Job, &mut Steps<'_>) -> Result<Value>,
    retry: impl FnOnce(&Job, &Error) -> Retry,
) -> Result<Attempt> {
    client
        .batch_execute(
            "set statement_timeout = '60s'; set idle_in_transaction_session_timeout = '60s'",
        )
        .await?;
    let Some(row) = client
        .query_opt("select * from resume.claim($1)", &[&workflow])
        .await?
    else {
        return Ok(Attempt::Idle);
    };
    let job = Job {
        id: row.try_get("id")?,
        attempt: row.try_get("attempt")?,
        failures: row.try_get("failures")?,
        key: row.try_get("key")?,
        input: row.try_get("input")?,
    };
    let attempt = job.attempt;
    let mut steps = Steps {
        client,
        id: job.id,
        attempt,
        position: 0,
        keys: HashSet::new(),
        suspended: false,
    };
    let result: Result<()> = async {
        let output = handler(&job, &mut steps).await;
        if steps.suspended {
            return Ok(());
        }
        let output = output?;
        steps
            .client
            .execute(
                "select resume.finish($1, $2, null, $3, null, $4)",
                &[&job.id, &attempt, &steps.position, &output],
            )
            .await?;
        Ok(())
    }
    .await;
    if let Err(error) = result {
        let delay = match retry(&job, &error) {
            Retry::Stop => None,
            Retry::After(delay) => Some(delay.as_secs_f64()),
        };
        let message = error
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(|error| error.as_db_error())
            .map_or_else(|| error.to_string(), |db| db.message().to_owned());
        steps
            .client
            .execute(
                "select resume.finish($1, $2, $3, 0, $4)",
                &[&job.id, &attempt, &message, &delay],
            )
            .await?;
        return Ok(Attempt::Failed(error));
    }
    Ok(Attempt::Processed)
}

/// Sequential execution state, borrowed exclusively for one attempt.
pub struct Steps<'a> {
    client: &'a mut Client,
    id: i64,
    attempt: i64,
    position: i32,
    keys: HashSet<String>,
    suspended: bool,
}

/// Returned by `sleep` and `wait_for` after releasing the claim. Propagate it with `?`;
/// later steps fail and the handler's result is ignored.
#[derive(Debug)]
pub struct Suspended;

impl std::fmt::Display for Suspended {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("job suspended")
    }
}

impl std::error::Error for Suspended {}

impl Steps<'_> {
    /// Replay a saved result or atomically commit the action's database effects and output.
    /// Use unique, stable keys in the same order, and the supplied transaction for database effects.
    /// External effects may repeat and must be idempotent. Propagate errors with `?`.
    pub async fn step(
        &mut self,
        key: &str,
        action: impl AsyncFnOnce(&Transaction<'_>) -> Result<Value>,
    ) -> Result<Value> {
        let id = self.id;
        let attempt = self.attempt;
        let (tx, saved) = self.start(key, false).await?;
        if let Some(output) = saved {
            tx.commit().await?;
            return Ok(output);
        }
        let output = tokio::time::timeout(Duration::from_secs(30), action(&tx)).await??;
        let output = save(&tx, id, attempt, key, &output).await?;
        tx.commit().await?;
        Ok(output)
    }

    /// Invoke an external action at most once per job, replaying its output on retry.
    /// The start is committed before calling; no transaction is held during the action.
    /// Errors, timeouts, or crashes can leave an unknown outcome. Such a marker blocks
    /// this action, all later steps, and completion, even if the handler swallows errors.
    /// After verifying the external outcome, `resume.resolve_step(id, key, output)` records
    /// it and pauses the job. `resume.requeue(id, delay_seconds)` explicitly allows it to continue.
    /// Neither operation may take over an actively leased job.
    pub async fn step_once(
        &mut self,
        key: &str,
        action: impl AsyncFnOnce() -> Result<Value>,
    ) -> Result<Value> {
        let (tx, saved) = self.start(key, true).await?;
        tx.commit().await?;
        if let Some(output) = saved {
            return Ok(output);
        }
        let output = tokio::time::timeout(Duration::from_secs(30), action()).await??;
        save(&*self.client, self.id, self.attempt, key, &output).await
    }

    /// Durable sleep: the wake time is saved as step `key`, then the job is released until it.
    pub async fn sleep(&mut self, key: &str, duration: Duration) -> Result<()> {
        let seconds = duration.as_secs_f64();
        let wake = self
            .step(key, async |tx| {
                Ok(tx
                    .query_one(
                        "select to_jsonb(clock_timestamp() + make_interval(secs => $1))",
                        &[&seconds],
                    )
                    .await?
                    .try_get(0)?)
            })
            .await?;
        let suspended: bool = self
            .client
            .query_one(
                "select resume.suspend($1, $2, ($3::jsonb #>> '{}')::timestamptz)",
                &[&self.id, &self.attempt, &wake],
            )
            .await?
            .try_get(0)?;
        self.suspend(suspended)
    }

    /// Submit a child job as step `key`, returning its id. Its key is `<parent id>/<key>`.
    pub async fn spawn(&mut self, key: &str, workflow: &str, input: &Value) -> Result<i64> {
        let parent = self.id;
        let child_key = format!("{parent}/{key}");
        let id = self
            .step(key, async |tx| {
                Ok(tx
                    .query_one(
                        "select to_jsonb(resume.submit($1, $2, $3, p_parent_id => $4))",
                        &[&workflow, &child_key, input, &parent],
                    )
                    .await?
                    .try_get(0)?)
            })
            .await?;
        Ok(id.as_i64().ok_or("saved child id is not an integer")?)
    }

    /// Return a spawned child's output, or release the job until the child completes.
    /// A paused child leaves the parent waiting.
    pub async fn wait_for(&mut self, child: i64) -> Result<Value> {
        if self.suspended {
            return Err(Suspended.into());
        }
        let output: Option<Value> = self
            .client
            .query_one(
                "select resume.wait_for($1, $2, $3)",
                &[&self.id, &self.attempt, &child],
            )
            .await?
            .try_get(0)?;
        match output {
            Some(output) => Ok(output),
            None => self.suspend(true).map(|()| Value::Null),
        }
    }

    fn suspend(&mut self, suspended: bool) -> Result<()> {
        self.suspended |= suspended;
        if self.suspended {
            Err(Suspended.into())
        } else {
            Ok(())
        }
    }

    async fn start(&mut self, key: &str, once: bool) -> Result<(Transaction<'_>, Option<Value>)> {
        if self.suspended {
            return Err(Suspended.into());
        }
        if key.is_empty() || !self.keys.insert(key.to_owned()) {
            return Err("step keys must be nonempty and unique within an attempt".into());
        }
        let next = self.position.checked_add(1).ok_or("too many steps")?;
        let tx = self.client.transaction().await?;
        let saved = tx
            .query_one(
                "select resume.start_step($1, $2, $3, $4, $5)",
                &[&self.id, &self.attempt, &key, &self.position, &once],
            )
            .await?
            .try_get(0)?;
        self.position = next;
        Ok((tx, saved))
    }
}

async fn save(
    db: &impl GenericClient,
    id: i64,
    attempt: i64,
    key: &str,
    output: &Value,
) -> Result<Value> {
    Ok(db
        .query_one(
            "select resume.save_step($1, $2, $3, $4)",
            &[&id, &attempt, &key, output],
        )
        .await?
        .try_get(0)?)
}
