use serde_json::Value;
use tokio_postgres::Client;

use crate::Result;

pub struct Producer<'a> {
    client: &'a Client,
    workflow: String,
}

impl<'a> Producer<'a> {
    pub fn new(client: &'a Client, workflow: impl Into<String>) -> Self {
        Self {
            client,
            workflow: workflow.into(),
        }
    }

    /// `max_attempts` counts claims, including recovery after a crash.
    pub async fn enqueue(&self, input: &Value, max_attempts: i32) -> Result<i64> {
        Ok(self
            .client
            .query_one(
                "select resume.enqueue($1, $2, $3)",
                &[&self.workflow, input, &max_attempts],
            )
            .await?
            .try_get(0)?)
    }
}
