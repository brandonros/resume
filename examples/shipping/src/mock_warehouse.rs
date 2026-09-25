use harness::Rng;
use resume::Result;
use tokio_postgres::Client;

/// A stand-in for a warehouse that ships whatever it is asked to. Its state lives in the
/// warehouse schema, on a connection that belongs to it, not to resume.
pub struct Warehouse {
    client: Client,
    rng: Rng,
    /// Every call waits a random time up to this long, to widen race windows.
    latency_ms: u64,
}

impl Warehouse {
    pub fn new(client: Client, seed: u64, latency_ms: u64) -> Self {
        Self {
            client,
            rng: Rng::new(seed),
            latency_ms,
        }
    }

    pub async fn ship(&mut self, order_id: i64) -> Result<i64> {
        self.rng.pause(self.latency_ms).await;
        let id: i64 = self
            .client
            .query_one(
                "insert into warehouse.shipments (order_id) values ($1) returning id",
                &[&order_id],
            )
            .await?
            .try_get(0)?;
        tracing::info!("warehouse: shipped order {order_id} as shipment {id}");
        Ok(id)
    }
}
