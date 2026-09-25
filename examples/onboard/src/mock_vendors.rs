use std::time::Duration;

use resume::Result;
use tokio_postgres::Client;

/// Failures a worker can inject with FAULT. Each fires once per worker process.
/// `*_unavailable` errors before the vendor does anything. `*_crash` exits the worker
/// after the vendor commits, before the workflow saves the result. `*_slow` replies after
/// the vendor commits, but later than the step timeout.
pub const FAULTS: [&str; 6] = [
    "crm_crash",
    "charge_unavailable",
    "charge_crash",
    "charge_slow",
    "email_unavailable",
    "email_crash",
];

/// Stand-ins for external services. Their state lives in the vendors schema, on a connection
/// that belongs to them, not to resume.
pub struct Vendors {
    client: Client,
    fault: Option<String>,
}

impl Vendors {
    pub fn new(client: Client, fault: Option<String>) -> Self {
        Self { client, fault }
    }

    pub async fn find_crm_contact(&mut self, customer_id: i64) -> Result<Option<i64>> {
        let row = self
            .client
            .query_opt(
                "select id from vendors.crm_contacts where customer_id = $1",
                &[&customer_id],
            )
            .await?;
        Ok(row.map(|row| row.try_get(0)).transpose()?)
    }

    pub async fn create_crm_contact(&mut self, customer_id: i64, email: &str) -> Result<i64> {
        let id: i64 = self
            .client
            .query_one(
                "insert into vendors.crm_contacts (customer_id, email) values ($1, $2) returning id",
                &[&customer_id, &email],
            )
            .await?
            .try_get(0)?;
        tracing::info!("crm: created contact {id}");
        self.crash("crm_crash");
        Ok(id)
    }

    pub async fn create_billing_customer(
        &mut self,
        idempotency_key: &str,
        email: &str,
    ) -> Result<i64> {
        let inserted = self
            .client
            .query_opt(
                "insert into vendors.billing_customers (idempotency_key, email) values ($1, $2)
                 on conflict (idempotency_key) do nothing returning id",
                &[&idempotency_key, &email],
            )
            .await?;
        let id: i64 = match inserted {
            Some(row) => row.try_get(0)?,
            // A separate statement sees the winning insert after a conflict.
            None => self
                .client
                .query_one(
                    "select id from vendors.billing_customers where idempotency_key = $1",
                    &[&idempotency_key],
                )
                .await?
                .try_get(0)?,
        };
        tracing::info!("billing: customer {id} for key {idempotency_key}");
        Ok(id)
    }

    pub async fn find_charge(&mut self, idempotency_key: &str) -> Result<Option<i64>> {
        let row = self
            .client
            .query_opt(
                "select id from vendors.charges where idempotency_key = $1",
                &[&idempotency_key],
            )
            .await?;
        Ok(row.map(|row| row.try_get(0)).transpose()?)
    }

    pub async fn charge(
        &mut self,
        idempotency_key: &str,
        billing_customer_id: i64,
        amount_cents: i64,
    ) -> Result<i64> {
        self.unavailable("charge_unavailable")?;
        let inserted = self
            .client
            .query_opt(
                "insert into vendors.charges (idempotency_key, billing_customer_id, amount_cents)
                 values ($1, $2, $3)
                 on conflict (idempotency_key) do nothing returning id",
                &[&idempotency_key, &billing_customer_id, &amount_cents],
            )
            .await?;
        let id: i64 = match inserted {
            Some(row) => {
                let id = row.try_get(0)?;
                tracing::info!("billing: charged {amount_cents} cents as charge {id}");
                id
            }
            None => {
                let id = self
                    .client
                    .query_one(
                        "select id from vendors.charges where idempotency_key = $1",
                        &[&idempotency_key],
                    )
                    .await?
                    .try_get(0)?;
                tracing::info!(
                    "billing: charge {id} already exists for key {idempotency_key}; returning it"
                );
                id
            }
        };
        self.crash("charge_crash");
        self.slow("charge_slow").await;
        Ok(id)
    }

    pub async fn send_email(&mut self, recipient: &str, template: &str) -> Result<i64> {
        self.unavailable("email_unavailable")?;
        let id: i64 = self
            .client
            .query_one(
                "insert into vendors.emails (recipient, template) values ($1, $2) returning id",
                &[&recipient, &template],
            )
            .await?
            .try_get(0)?;
        tracing::info!("email: sent {template} to {recipient} as message {id}");
        self.crash("email_crash");
        Ok(id)
    }

    pub async fn post_slack(&mut self, text: &str) -> Result<()> {
        self.client
            .execute(
                "insert into vendors.slack_messages (text) values ($1)",
                &[&text],
            )
            .await?;
        tracing::info!("slack: posted {text:?}");
        Ok(())
    }

    fn unavailable(&mut self, fault: &str) -> Result<()> {
        if self.fault.take_if(|f| *f == fault).is_some() {
            tracing::warn!("fault {fault}: failing before the vendor does anything");
            return Err(format!("{fault}: 503 service unavailable").into());
        }
        Ok(())
    }

    async fn slow(&mut self, fault: &str) {
        if self.fault.take_if(|f| *f == fault).is_some() {
            tracing::warn!("fault {fault}: vendor committed; replying in 10s");
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    fn crash(&mut self, fault: &str) {
        if self.fault.take_if(|f| *f == fault).is_some() {
            tracing::error!("fault {fault}: vendor committed; crashing before the workflow saves");
            std::process::exit(1);
        }
    }
}
