use std::time::Duration;

use resume::Result;
use tokio_postgres::Client;

pub struct MockIssuer {
    client: Client,
}

impl MockIssuer {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn issue(&mut self, request_id: &str, attendee: &str) -> Result<i64> {
        // This connection and transaction belong to the issuer, not to resume.
        let tx = self.client.transaction().await?;
        let inserted = tx
            .query_opt(
                "insert into tickets.issued (request_id, attendee) values ($1, $2)
                 on conflict (request_id) do nothing returning id",
                &[&request_id, &attendee],
            )
            .await?;

        let (id, created): (i64, bool) = match inserted {
            Some(row) => (row.try_get(0)?, true),
            None => {
                // A separate statement sees the winning insert after a conflict.
                let row = tx
                    .query_one(
                        "select id, attendee from tickets.issued where request_id = $1",
                        &[&request_id],
                    )
                    .await?;
                if row.try_get::<_, &str>("attendee")? != attendee {
                    return Err("request_id already used for a different attendee".into());
                }
                (row.try_get("id")?, false)
            }
        };
        tx.commit().await?;

        if created {
            tracing::info!(
                "mock issuer: issued ticket {id} for request {request_id}; committed, delaying reply for 10s"
            );
            // Kill here: the ticket exists, but the workflow has no saved result.
            tokio::time::sleep(Duration::from_secs(10)).await;
        } else {
            tracing::info!("mock issuer: reusing ticket {id} for request {request_id}");
        }
        Ok(id)
    }
}
