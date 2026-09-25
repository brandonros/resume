use std::time::Duration;

use resume::Result;
use tokio_postgres::Client;

pub struct MockVendor {
    client: Client,
}

impl MockVendor {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Repeating a request returns the same export without moving its ready time. This
    /// covers a crash after the vendor accepted the request but before the step saved it.
    pub async fn start(&self, key: &str, ready_after: Duration) -> Result<i64> {
        Ok(self
            .client
            .query_one(
                "insert into exports.jobs (idempotency_key, ready_at)
             values ($1, clock_timestamp() + make_interval(secs => $2))
             on conflict (idempotency_key) do update
             set idempotency_key = excluded.idempotency_key
             returning id",
                &[&key, &ready_after.as_secs_f64()],
            )
            .await?
            .try_get(0)?)
    }

    pub async fn download_url(&self, export_id: i64) -> Result<Option<String>> {
        let ready: bool = self
            .client
            .query_one(
                "select ready_at <= clock_timestamp() from exports.jobs where id = $1",
                &[&export_id],
            )
            .await?
            .try_get(0)?;
        Ok(ready.then(|| format!("https://exports.example/{export_id}.csv")))
    }
}
