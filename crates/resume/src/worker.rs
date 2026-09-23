use std::time::Duration;

use tokio_postgres::Client;

use crate::{Result, Run};

pub struct Worker {
    client: Client,
    workflow: String,
    lease_seconds: i32,
}

impl Worker {
    pub fn new(client: Client, workflow: impl Into<String>, lease_seconds: i32) -> Self {
        Self {
            client,
            workflow: workflow.into(),
            lease_seconds,
        }
    }

    pub async fn run(
        mut self,
        mut execute: impl AsyncFnMut(&mut Client, &Run) -> Result<()>,
    ) -> Result<()> {
        let workflow = &self.workflow;
        let lease_seconds = self.lease_seconds;
        tracing::info!("{workflow}: worker started");
        let mut waiting = false;
        loop {
            let Some(run) = self.claim().await? else {
                if !waiting {
                    tracing::info!("{}: waiting for work", self.workflow);
                    waiting = true;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            };

            waiting = false;
            tracing::info!(
                "run {} attempt {}/{}: claimed (lease {lease_seconds}s)",
                run.id,
                run.attempt,
                run.max_attempts
            );
            let result = async {
                execute(&mut self.client, &run).await?;
                self.finish(&run).await
            }
            .await;

            match result {
                Ok(()) => tracing::info!("run {}: finished", run.id),
                Err(error) => {
                    let next = if run.attempt < i64::from(run.max_attempts) {
                        "retry after lease expires"
                    } else {
                        "attempt limit reached"
                    };
                    tracing::warn!(
                        "run {} attempt {}/{}: failed: {error}; {next}",
                        run.id,
                        run.attempt,
                        run.max_attempts
                    );
                }
            }
        }
    }

    async fn claim(&self) -> Result<Option<Run>> {
        let row = self
            .client
            .query_opt(
                "select id, input, attempt, max_attempts from resume.claim($1, $2)",
                &[&self.workflow, &self.lease_seconds],
            )
            .await?;

        row.map(|row| {
            Ok(Run {
                id: row.try_get("id")?,
                input: row.try_get("input")?,
                attempt: row.try_get("attempt")?,
                max_attempts: row.try_get("max_attempts")?,
            })
        })
        .transpose()
    }

    async fn finish(&self, run: &Run) -> Result<()> {
        self.client
            .query_one("select resume.finish_run($1, $2)", &[&run.id, &run.attempt])
            .await?;
        Ok(())
    }
}
