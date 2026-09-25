use std::time::Duration;

use harness::Rng;
use resume::Result;
use tokio_postgres::Client;

/// A stand-in for a billing vendor that sets a customer's plan, overwriting the last one. Its
/// state lives in the billing schema, on a connection that belongs to it, not to resume.
pub struct Billing {
    client: Client,
    rng: Rng,
    /// Every call waits a random time up to this long, so runs finish in varying order.
    latency_ms: u64,
    /// The chance a call fails before doing anything, so the run retries after a backoff.
    error_chance: f64,
}

impl Billing {
    pub fn new(client: Client, seed: u64, latency_ms: u64, error_chance: f64) -> Self {
        Self {
            client,
            rng: Rng::new(seed),
            latency_ms,
            error_chance,
        }
    }

    pub async fn set_plan(&mut self, customer_id: i64, plan: &str) -> Result<()> {
        if self.latency_ms > 0 {
            let ms = self.rng.below(self.latency_ms + 1);
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        if self.rng.chance(self.error_chance) {
            return Err("billing: 503 service unavailable".into());
        }
        self.client
            .execute(
                "insert into billing.subscriptions (customer_id, plan) values ($1, $2)
                 on conflict (customer_id) do update set plan = excluded.plan",
                &[&customer_id, &plan],
            )
            .await?;
        tracing::info!("billing: customer {customer_id} is now on {plan}");
        Ok(())
    }
}
