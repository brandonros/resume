use serde_json::Value;
use tokio_postgres::{Client, GenericClient, Transaction};

use crate::{Result, RunFailed};

pub struct Run {
    pub id: i64,
    pub idempotency_key: String,
    pub input: Value,
    pub(crate) attempt: i64,
    pub(crate) max_attempts: i32,
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
        let (tx, existing) = self.begin_step(client, idempotency_key).await?;
        if let Some(output) = existing {
            tx.commit().await?;
            tracing::info!("run {} step {idempotency_key}: using saved result", self.id);
            return Ok(output);
        }

        tracing::info!("run {} step {idempotency_key}: executing", self.id);
        let output = action(&tx).await?;
        let saved = self.save_step(&tx, idempotency_key, &output).await?;
        tx.commit().await?;
        tracing::info!("run {} step {idempotency_key}: committed", self.id);
        Ok(saved)
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
        let (tx, existing) = self.begin_step(client, idempotency_key).await?;
        if let Some(output) = existing {
            tx.commit().await?;
            tracing::info!("run {} step {idempotency_key}: using saved result", self.id);
            return Ok(output);
        }

        let output = match check(&mut *context, &tx).await? {
            Some(output) => {
                tracing::info!("run {} step {idempotency_key}: already done", self.id);
                output
            }
            None => {
                tracing::info!("run {} step {idempotency_key}: executing", self.id);
                action(&mut *context, &tx).await?
            }
        };
        let saved = self.save_step(&tx, idempotency_key, &output).await?;
        tx.commit().await?;
        tracing::info!("run {} step {idempotency_key}: committed", self.id);
        Ok(saved)
    }

    /// For external effects that must not repeat, such as a vendor without idempotency.
    /// The action runs at most once per run. If an attempt dies after starting it and
    /// before saving its result, the outcome is unknown: the run fails instead of calling
    /// again, and someone must check the vendor.
    pub async fn step_once(
        &self,
        client: &mut Client,
        idempotency_key: &str,
        action: impl AsyncFnOnce() -> Result<Value>,
    ) -> Result<Value> {
        let (tx, existing) = self.begin_step(client, idempotency_key).await?;
        if let Some(output) = existing {
            tx.commit().await?;
            tracing::info!("run {} step {idempotency_key}: using saved result", self.id);
            return Ok(output);
        }

        if !self.start_step(&tx, idempotency_key).await? {
            self.fail_run(&tx).await?;
            tx.commit().await?;
            return Err(RunFailed(format!(
                "step {idempotency_key} started in an earlier attempt and its outcome is unknown"
            ))
            .into());
        }
        // Commit the start before calling, so a later attempt knows the call may have happened.
        tx.commit().await?;

        tracing::info!("run {} step {idempotency_key}: executing once", self.id);
        let output = action().await?;

        // save_step checks the claim itself, so this needs no transaction of its own.
        let saved = self.save_step(&*client, idempotency_key, &output).await?;
        tracing::info!("run {} step {idempotency_key}: committed", self.id);
        Ok(saved)
    }

    /// Starts a transaction holding the run's lock, after checking this attempt owns it.
    /// Returns the step's saved output if it already completed.
    async fn begin_step<'c>(
        &self,
        client: &'c mut Client,
        key: &str,
    ) -> Result<(Transaction<'c>, Option<Value>)> {
        let tx = client.transaction().await?;
        let existing = tx
            .query_one(
                "select resume.begin_step($1, $2, $3)",
                &[&self.id, &self.attempt, &key],
            )
            .await?
            .try_get(0)?;
        Ok((tx, existing))
    }

    async fn start_step(&self, tx: &Transaction<'_>, key: &str) -> Result<bool> {
        Ok(tx
            .query_one(
                "select resume.start_step($1, $2, $3)",
                &[&self.id, &self.attempt, &key],
            )
            .await?
            .try_get(0)?)
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

    async fn fail_run(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute("select resume.fail_run($1, $2)", &[&self.id, &self.attempt])
            .await?;
        Ok(())
    }
}
