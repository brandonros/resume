use std::time::Duration;

use serde_json::Value;
use tokio_postgres::{Client, Transaction};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub struct Run {
    pub id: i64,
    pub input: Value,
    attempt: i64,
}

pub async fn enqueue(client: &Client, workflow: &str, input: &Value) -> Result<i64> {
    Ok(client
        .query_one("select resume.enqueue($1, $2)", &[&workflow, input])
        .await?
        .try_get(0)?)
}

pub async fn claim(client: &Client, workflow: &str, lease_seconds: i32) -> Result<Option<Run>> {
    let row = client
        .query_opt(
            "select id, input, attempt from resume.claim($1, $2)",
            &[&workflow, &lease_seconds],
        )
        .await?;

    row.map(|row| {
        Ok(Run {
            id: row.try_get("id")?,
            input: row.try_get("input")?,
            attempt: row.try_get("attempt")?,
        })
    })
    .transpose()
}

pub async fn finish_run(client: &Client, run: &Run) -> Result<()> {
    client
        .query_one("select resume.finish_run($1, $2)", &[&run.id, &run.attempt])
        .await?;
    Ok(())
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
        let tx = client.transaction().await?;
        tx.query_one(
            "select 1 from resume.runs where id = $1 for update",
            &[&self.id],
        )
        .await?;

        // Check time after acquiring the lock, since acquiring it might have waited.
        let owned: bool = tx
            .query_one(
                "select attempt = $2 and finished_at is null
                        and available_at > clock_timestamp()
                 from resume.runs where id = $1",
                &[&self.id, &self.attempt],
            )
            .await?
            .try_get(0)?;
        if !owned {
            return Err("run claim expired or was replaced".into());
        }

        if let Some(row) = tx
            .query_opt(
                "select output from resume.steps where run_id = $1 and step_name = $2",
                &[&self.id, &name],
            )
            .await?
        {
            let output = row.try_get(0)?;
            tx.commit().await?;
            tracing::info!("run {} step {name}: using saved result", self.id);
            return Ok(output);
        }

        tracing::info!("run {} step {name}: executing", self.id);
        let output = action(&tx).await?;
        let saved = tx
            .query_one(
                "select resume.save_step($1, $2, $3, $4)",
                &[&self.id, &self.attempt, &name, &output],
            )
            .await?
            .try_get(0)?;
        tx.commit().await?;
        tracing::info!("run {} step {name}: committed", self.id);
        Ok(saved)
    }
}

pub async fn work(
    client: &mut Client,
    workflow: &str,
    lease_seconds: i32,
    mut execute: impl AsyncFnMut(&mut Client, &Run) -> Result<()>,
) -> Result<()> {
    tracing::info!("{workflow}: worker started");
    let mut waiting = false;
    loop {
        let Some(run) = claim(client, workflow, lease_seconds).await? else {
            if !waiting {
                tracing::info!("{workflow}: waiting for work");
                waiting = true;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };

        waiting = false;
        tracing::info!(
            "run {} attempt {}: claimed (lease {lease_seconds}s)",
            run.id,
            run.attempt
        );
        let result = async {
            execute(client, &run).await?;
            finish_run(client, &run).await
        }
        .await;

        match result {
            Ok(()) => tracing::info!("run {}: finished", run.id),
            Err(error) => {
                tracing::warn!(
                    "run {} attempt {}: failed: {error}; retry after lease expires",
                    run.id,
                    run.attempt
                );
            }
        }
    }
}
