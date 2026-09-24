use serde_json::Value;
use tokio_postgres::Client;

use crate::Result;

pub struct Producer<'a> {
    client: &'a Client,
    workflow: String,
}

pub struct Submitted {
    pub id: i64,
    /// False when a run with this idempotency key already existed, in any state.
    pub created: bool,
}

impl<'a> Producer<'a> {
    pub fn new(client: &'a Client, workflow: impl Into<String>) -> Self {
        Self {
            client,
            workflow: workflow.into(),
        }
    }

    /// Returns the run for `idempotency_key`, creating it if needed. Reusing a key with
    /// different input is an error. `max_attempts` counts claims, including recovery after
    /// a crash, and only applies when the run is created.
    pub async fn submit(
        &self,
        idempotency_key: &str,
        input: &Value,
        max_attempts: i32,
    ) -> Result<Submitted> {
        let row = self
            .client
            .query_one(
                "select run_id, created from resume.submit_run($1, $2, $3, $4)",
                &[&self.workflow, &idempotency_key, input, &max_attempts],
            )
            .await?;
        Ok(Submitted {
            id: row.try_get("run_id")?,
            created: row.try_get("created")?,
        })
    }
}
