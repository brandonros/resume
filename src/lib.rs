use std::collections::HashSet;
use std::time::Duration;

use serde_json::Value;
use tokio_postgres::{Client, GenericClient, Transaction};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// Data for the current job. Effects and nondeterministic decisions belong in steps.
pub struct Job {
    pub id: i64,
    pub key: String,
    pub input: Value,
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

/// Process at most one job; return false when none is ready, true on completion.
/// Handler errors release the job for retry and are returned to the caller.
/// Database failures propagate; an unreleased job recovers after its lease expires.
///
/// Use a dedicated client: this sets its statement and idle-transaction timeouts to
/// 60 seconds. Step actions have a 30-second timeout and must yield to the runtime.
/// Dropping this future abandons the attempt; it does not undo external effects.
pub async fn run_one(
    client: &mut Client,
    workflow: &str,
    handler: impl AsyncFnOnce(&Job, &mut Steps<'_>) -> Result<()>,
) -> Result<bool> {
    client
        .batch_execute(
            "set statement_timeout = '60s'; set idle_in_transaction_session_timeout = '60s'",
        )
        .await?;
    let Some(row) = client
        .query_opt("select * from resume.claim($1)", &[&workflow])
        .await?
    else {
        return Ok(false);
    };
    let job = Job {
        id: row.try_get("id")?,
        key: row.try_get("key")?,
        input: row.try_get("input")?,
    };
    let attempt = row.try_get("attempt")?;
    let result = handler(
        &job,
        &mut Steps {
            client,
            id: job.id,
            attempt,
            keys: HashSet::new(),
        },
    )
    .await;
    let error = result.as_ref().err().map(ToString::to_string);
    client
        .execute(
            "select resume.finish($1, $2, $3)",
            &[&job.id, &attempt, &error],
        )
        .await?;
    result?;
    Ok(true)
}

/// Sequential execution state, borrowed exclusively for one attempt.
pub struct Steps<'a> {
    client: &'a mut Client,
    id: i64,
    attempt: i64,
    keys: HashSet<String>,
}

impl Steps<'_> {
    /// Replay a saved result or atomically commit the action's database effects and output.
    /// Use unique, stable keys and the supplied transaction for all database effects.
    /// External effects may repeat and must be idempotent. Propagate errors with `?`.
    pub async fn step(
        &mut self,
        key: &str,
        action: impl AsyncFnOnce(&Transaction<'_>) -> Result<Value>,
    ) -> Result<Value> {
        if key.is_empty() || !self.keys.insert(key.to_owned()) {
            return Err("step keys must be nonempty and unique within an attempt".into());
        }
        let tx = self.client.transaction().await?;
        tx.execute(
            "select resume.begin_step($1, $2)",
            &[&self.id, &self.attempt],
        )
        .await?;
        if let Some(row) = tx
            .query_opt(
                "select output from resume.steps where job_id = $1 and key = $2",
                &[&self.id, &key],
            )
            .await?
        {
            let output = row.try_get(0)?;
            tx.commit().await?;
            return Ok(output);
        }
        let output = tokio::time::timeout(Duration::from_secs(30), action(&tx)).await??;
        tx.execute(
            "insert into resume.steps (job_id, key, output) values ($1, $2, $3)",
            &[&self.id, &key, &output],
        )
        .await?;
        tx.commit().await?;
        Ok(output)
    }
}
