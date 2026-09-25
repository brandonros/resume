use harness::Rng;
use resume::Result;
use tokio_postgres::Client;

/// Mock cloud with separate lookup and non-idempotent creation calls, and no quota enforcement.
/// Commits independently of workflow steps, in the cloud schema.
pub struct Cloud {
    client: Client,
    rng: Rng,
    /// Every call waits a random time up to this long, to widen race windows.
    latency_ms: u64,
    /// The chance that the worker crashes right after the cloud creates a VM.
    crash_chance: f64,
}

impl Cloud {
    pub fn new(client: Client, seed: u64, latency_ms: u64, crash_chance: f64) -> Self {
        Self {
            client,
            rng: Rng::new(seed),
            latency_ms,
            crash_chance,
        }
    }

    /// The VM created for a request, found by the tag the create call sets.
    pub async fn find_vm(&mut self, request_id: &str) -> Result<Option<i64>> {
        self.rng.pause(self.latency_ms).await;
        let row = self
            .client
            .query_opt(
                "select id from cloud.vms where request_id = $1",
                &[&request_id],
            )
            .await?;
        Ok(row.map(|row| row.try_get(0)).transpose()?)
    }

    pub async fn count_vms(&mut self, team: &str) -> Result<i64> {
        self.rng.pause(self.latency_ms).await;
        Ok(self
            .client
            .query_one("select count(*) from cloud.vms where team = $1", &[&team])
            .await?
            .try_get(0)?)
    }

    pub async fn create_vm(&mut self, team: &str, request_id: &str) -> Result<i64> {
        self.rng.pause(self.latency_ms).await;
        let id: i64 = self
            .client
            .query_one(
                "insert into cloud.vms (team, request_id) values ($1, $2) returning id",
                &[&team, &request_id],
            )
            .await?
            .try_get(0)?;
        tracing::info!("cloud: created VM {id} for team {team}");
        if self.rng.chance(self.crash_chance) {
            tracing::error!("fault: the cloud created VM {id}; crashing before the workflow saves");
            std::process::exit(1);
        }
        Ok(id)
    }
}
