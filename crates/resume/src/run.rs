use serde_json::Value;
use tokio_postgres::{Client, Transaction};

use crate::Result;

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
        let tx = self.begin(client).await?;
        if let Some(output) = self.load_step(&tx, idempotency_key).await? {
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
        let tx = self.begin(client).await?;
        if let Some(output) = self.load_step(&tx, idempotency_key).await? {
            tx.commit().await?;
            tracing::info!("run {} step {idempotency_key}: using saved result", self.id);
            return Ok(output);
        }

        if !self.start_step(&tx, idempotency_key).await? {
            self.fail_run(&tx).await?;
            tx.commit().await?;
            return Err(format!(
                "step {idempotency_key} started in an earlier attempt and its outcome is unknown; run failed"
            )
            .into());
        }
        // Commit the start before calling, so a later attempt knows the call may have happened.
        tx.commit().await?;

        tracing::info!("run {} step {idempotency_key}: executing once", self.id);
        let output = action().await?;

        let tx = self.begin(client).await?;
        let saved = self.save_step(&tx, idempotency_key, &output).await?;
        tx.commit().await?;
        tracing::info!("run {} step {idempotency_key}: committed", self.id);
        Ok(saved)
    }

    /// Starts a transaction holding the run's lock, after checking this attempt owns it.
    async fn begin<'c>(&self, client: &'c mut Client) -> Result<Transaction<'c>> {
        let tx = client.transaction().await?;
        tx.execute("select resume.lock_run($1, $2)", &[&self.id, &self.attempt])
            .await?;
        Ok(tx)
    }

    async fn load_step(&self, tx: &Transaction<'_>, key: &str) -> Result<Option<Value>> {
        Ok(tx
            .query_one("select resume.load_step($1, $2)", &[&self.id, &key])
            .await?
            .try_get(0)?)
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

    async fn save_step(&self, tx: &Transaction<'_>, key: &str, output: &Value) -> Result<Value> {
        Ok(tx
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
