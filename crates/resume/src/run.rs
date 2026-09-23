use serde_json::Value;
use tokio_postgres::{Client, Transaction};

use crate::Result;

pub struct Run {
    pub id: i64,
    pub input: Value,
    pub(crate) attempt: i64,
    pub(crate) max_attempts: i32,
}

impl Run {
    /// Use a stable name and the supplied transaction for the step's database effects.
    /// Those effects and the saved result commit together. External effects may repeat.
    pub async fn step(
        &self,
        client: &mut Client,
        name: &str,
        action: impl AsyncFnOnce(&Transaction<'_>) -> Result<Value>,
    ) -> Result<Value> {
        let tx = self.begin(client).await?;
        if let Some(output) = self.saved_output(&tx, name).await? {
            tx.commit().await?;
            tracing::info!("run {} step {name}: using saved result", self.id);
            return Ok(output);
        }

        tracing::info!("run {} step {name}: executing", self.id);
        let output = action(&tx).await?;
        let saved = self.save(&tx, name, &output).await?;
        tx.commit().await?;
        tracing::info!("run {} step {name}: committed", self.id);
        Ok(saved)
    }

    /// For external effects that must not repeat, such as a vendor without idempotency.
    /// The action runs at most once per run. If an attempt dies after starting it and
    /// before saving its result, the outcome is unknown: the run fails instead of calling
    /// again, and someone must check the vendor.
    pub async fn step_once(
        &self,
        client: &mut Client,
        name: &str,
        action: impl AsyncFnOnce() -> Result<Value>,
    ) -> Result<Value> {
        let tx = self.begin(client).await?;
        if let Some(output) = self.saved_output(&tx, name).await? {
            tx.commit().await?;
            tracing::info!("run {} step {name}: using saved result", self.id);
            return Ok(output);
        }

        let started = tx
            .execute(
                "insert into resume.step_starts (run_id, step_name) values ($1, $2)
                 on conflict do nothing",
                &[&self.id, &name],
            )
            .await?
            == 1;
        if !started {
            tx.execute(
                "update resume.runs set failed_at = clock_timestamp() where id = $1",
                &[&self.id],
            )
            .await?;
            tx.commit().await?;
            return Err(format!(
                "step {name} started in an earlier attempt and its outcome is unknown; run failed"
            )
            .into());
        }
        // Commit the start before calling, so a later attempt knows the call may have happened.
        tx.commit().await?;

        tracing::info!("run {} step {name}: executing once", self.id);
        let output = action().await?;

        let tx = self.begin(client).await?;
        let saved = self.save(&tx, name, &output).await?;
        tx.commit().await?;
        tracing::info!("run {} step {name}: committed", self.id);
        Ok(saved)
    }

    /// Locks the run and checks that this attempt still owns it.
    async fn begin<'c>(&self, client: &'c mut Client) -> Result<Transaction<'c>> {
        let tx = client.transaction().await?;
        tx.query_one(
            "select 1 from resume.runs where id = $1 for update",
            &[&self.id],
        )
        .await?;

        // Check time after acquiring the lock, since acquiring it might have waited.
        let owned: bool = tx
            .query_one(
                "select attempt = $2 and finished_at is null and failed_at is null
                        and available_at > clock_timestamp()
                 from resume.runs where id = $1",
                &[&self.id, &self.attempt],
            )
            .await?
            .try_get(0)?;
        if !owned {
            return Err("run claim expired or was replaced".into());
        }
        Ok(tx)
    }

    async fn saved_output(&self, tx: &Transaction<'_>, name: &str) -> Result<Option<Value>> {
        let row = tx
            .query_opt(
                "select output from resume.steps where run_id = $1 and step_name = $2",
                &[&self.id, &name],
            )
            .await?;
        Ok(row.map(|row| row.try_get(0)).transpose()?)
    }

    async fn save(&self, tx: &Transaction<'_>, name: &str, output: &Value) -> Result<Value> {
        Ok(tx
            .query_one(
                "select resume.save_step($1, $2, $3, $4)",
                &[&self.id, &self.attempt, &name, output],
            )
            .await?
            .try_get(0)?)
    }
}
