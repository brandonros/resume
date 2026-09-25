use std::time::Duration;

use resume::Result;
use tokio_postgres::Client;

use harness::Rng;

/// Vendor calls a fault can target.
pub const CALLS: [&str; 7] = [
    "crm_find",
    "crm_create",
    "billing_customer",
    "charge_find",
    "charge",
    "email",
    "slack",
];

/// `error` fails before the vendor does anything. The rest act after the vendor commits and
/// before the workflow saves: `crash` exits the worker, `slow` replies after the step timeout,
/// and `freeze` stops the worker process with its connection still open, as a hung process
/// or paused VM would.
pub const KINDS: [&str; 4] = ["error", "crash", "slow", "freeze"];

/// Which faults fire, from a spec such as "seed=7 latency_ms=100 charge.crash=0.1
/// email.error=once". A number is the chance the fault fires on each call; `once` fires on
/// the first call only. `latency_ms` adds a random delay of up to that long to every call.
pub struct Faults {
    rng: Rng,
    latency_ms: u64,
    /// Each point with its chance, or None for once.
    rules: Vec<(String, Option<f64>)>,
}

impl Faults {
    pub fn parse(spec: &str) -> Result<Self> {
        let mut faults = Self {
            rng: Rng::new(0),
            latency_ms: 0,
            rules: Vec::new(),
        };
        for token in spec.split_whitespace() {
            let (name, value) = token
                .split_once('=')
                .ok_or_else(|| format!("expected name=value, got {token:?}"))?;
            match name {
                "seed" => faults.rng = Rng::new(value.parse()?),
                "latency_ms" => faults.latency_ms = value.parse()?,
                point => {
                    let known = point
                        .split_once('.')
                        .is_some_and(|(call, kind)| CALLS.contains(&call) && KINDS.contains(&kind));
                    if !known {
                        return Err(format!(
                            "unknown fault {point}; expected <call>.<kind> with calls {CALLS:?} and kinds {KINDS:?}"
                        )
                        .into());
                    }
                    let chance = if value == "once" {
                        None
                    } else {
                        Some(value.parse()?)
                    };
                    faults.rules.push((point.to_string(), chance));
                }
            }
        }
        Ok(faults)
    }

    fn fires(&mut self, call: &str, kind: &str) -> bool {
        let point = format!("{call}.{kind}");
        let Some(i) = self.rules.iter().position(|(p, _)| *p == point) else {
            return false;
        };
        match self.rules[i].1 {
            None => {
                self.rules.remove(i);
                true
            }
            Some(chance) => self.rng.chance(chance),
        }
    }
}

/// Stand-ins for external services. Their state lives in the vendors schema, on a connection
/// that belongs to them, not to resume.
pub struct Vendors {
    client: Client,
    faults: Faults,
}

impl Vendors {
    pub fn new(client: Client, faults: Faults) -> Self {
        Self { client, faults }
    }

    pub async fn find_crm_contact(&mut self, customer_id: i64) -> Result<Option<i64>> {
        self.before("crm_find").await?;
        let row = self
            .client
            .query_opt(
                "select id from vendors.crm_contacts where customer_id = $1",
                &[&customer_id],
            )
            .await?;
        self.after("crm_find").await;
        Ok(row.map(|row| row.try_get(0)).transpose()?)
    }

    /// Not idempotent: every call creates a contact.
    pub async fn create_crm_contact(&mut self, customer_id: i64, email: &str) -> Result<i64> {
        self.before("crm_create").await?;
        let id: i64 = self
            .client
            .query_one(
                "insert into vendors.crm_contacts (customer_id, email) values ($1, $2) returning id",
                &[&customer_id, &email],
            )
            .await?
            .try_get(0)?;
        tracing::info!("crm: created contact {id}");
        self.after("crm_create").await;
        Ok(id)
    }

    pub async fn create_billing_customer(
        &mut self,
        idempotency_key: &str,
        email: &str,
    ) -> Result<i64> {
        self.before("billing_customer").await?;
        let id = self
            .insert_or_get(
                "insert into vendors.billing_customers (idempotency_key, email) values ($1, $2)
                 on conflict (idempotency_key) do nothing returning id",
                &[&idempotency_key, &email],
                "select id from vendors.billing_customers where idempotency_key = $1",
                idempotency_key,
            )
            .await?;
        tracing::info!("billing: customer {id} for key {idempotency_key}");
        self.after("billing_customer").await;
        Ok(id)
    }

    pub async fn find_charge(&mut self, idempotency_key: &str) -> Result<Option<i64>> {
        self.before("charge_find").await?;
        let row = self
            .client
            .query_opt(
                "select id from vendors.charges where idempotency_key = $1",
                &[&idempotency_key],
            )
            .await?;
        self.after("charge_find").await;
        Ok(row.map(|row| row.try_get(0)).transpose()?)
    }

    pub async fn charge(
        &mut self,
        idempotency_key: &str,
        billing_customer_id: i64,
        amount_cents: i64,
    ) -> Result<i64> {
        self.before("charge").await?;
        let id = self
            .insert_or_get(
                "insert into vendors.charges (idempotency_key, billing_customer_id, amount_cents)
                 values ($1, $2, $3)
                 on conflict (idempotency_key) do nothing returning id",
                &[&idempotency_key, &billing_customer_id, &amount_cents],
                "select id from vendors.charges where idempotency_key = $1",
                idempotency_key,
            )
            .await?;
        tracing::info!("billing: charge {id} of {amount_cents} cents for key {idempotency_key}");
        self.after("charge").await;
        Ok(id)
    }

    /// No idempotency key and no way to look up what was sent.
    pub async fn send_email(&mut self, recipient: &str, template: &str) -> Result<i64> {
        self.before("email").await?;
        let id: i64 = self
            .client
            .query_one(
                "insert into vendors.emails (recipient, template) values ($1, $2) returning id",
                &[&recipient, &template],
            )
            .await?
            .try_get(0)?;
        tracing::info!("email: sent {template} to {recipient} as message {id}");
        self.after("email").await;
        Ok(id)
    }

    pub async fn post_slack(&mut self, text: &str) -> Result<()> {
        self.before("slack").await?;
        self.client
            .execute(
                "insert into vendors.slack_messages (text) values ($1)",
                &[&text],
            )
            .await?;
        tracing::info!("slack: posted {text:?}");
        self.after("slack").await;
        Ok(())
    }

    /// Inserts with an idempotency key, or returns the row an earlier call created.
    async fn insert_or_get(
        &self,
        insert: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        select: &str,
        idempotency_key: &str,
    ) -> Result<i64> {
        if let Some(row) = self.client.query_opt(insert, params).await? {
            return Ok(row.try_get(0)?);
        }
        // A separate statement sees the winning insert after a conflict.
        Ok(self
            .client
            .query_one(select, &[&idempotency_key])
            .await?
            .try_get(0)?)
    }

    async fn before(&mut self, call: &str) -> Result<()> {
        self.faults.rng.pause(self.faults.latency_ms).await;
        if self.faults.fires(call, "error") {
            tracing::warn!("fault {call}.error: failing before the vendor does anything");
            return Err(format!("{call}: 503 service unavailable").into());
        }
        Ok(())
    }

    async fn after(&mut self, call: &str) {
        if self.faults.fires(call, "crash") {
            tracing::error!(
                "fault {call}.crash: vendor committed; crashing before the workflow saves"
            );
            std::process::exit(1);
        }
        if self.faults.fires(call, "slow") {
            tracing::warn!("fault {call}.slow: vendor committed; replying in 10s");
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        if self.faults.fires(call, "freeze") {
            tracing::error!("fault {call}.freeze: vendor committed; freezing this worker");
            let _ = std::process::Command::new("kill")
                .args(["-STOP", &std::process::id().to_string()])
                .status();
        }
    }
}
