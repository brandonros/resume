mod chaos;
mod mock_vendors;

use std::time::Duration;

use mock_vendors::{Faults, Vendors};

use resume::{Permanent, Producer, Result, Run, Worker, lock_resource, shutdown_signal};
use serde_json::json;

pub const VERSION: &str = "1";

const SETUP_FEE_CENTS: i64 = 5000;

// Short limits, so retries and recovery in the demo come quickly.
pub const LEASE: Duration = Duration::from_secs(5);
const STEP_TIMEOUT: Duration = Duration::from_secs(3);

async fn onboard(run: &Run, v: &mut Vendors) -> Result<()> {
    let email = run.input["email"]
        .as_str()
        .ok_or("email must be a string")?;
    let plan = run.input["plan"].as_str().ok_or("plan must be a string")?;

    // Invalid input cannot be fixed by retrying.
    run.step("validate", async |_| {
        if !email.contains('@') {
            return Err(Permanent(format!("invalid email {email:?}")).into());
        }
        if !matches!(plan, "free" | "pro") {
            return Err(Permanent(format!("unknown plan {plan:?}")).into());
        }
        Ok(json!(null))
    })
    .await?;

    // The insert and step result commit together.
    let customer_id = run
        .step("create_customer", async |tx| {
            let row = tx
                .query_one(
                    "insert into onboard.customers (email) values ($1) returning id",
                    &[&email],
                )
                .await?;
            Ok(json!(row.try_get::<_, i64>(0)?))
        })
        .await?
        .as_i64()
        .ok_or("customer ID must be an integer")?;

    // Lookup recovers a contact created before a crash. Lock across lookup and creation
    // to prevent concurrent runs from creating duplicates.
    let crm_contact_id = run
        .step("ensure_crm_contact", async |tx| {
            lock_resource(tx, &format!("crm_contact:{customer_id}")).await?;
            if let Some(id) = v.find_crm_contact(customer_id).await? {
                return Ok(json!(id));
            }
            Ok(json!(v.create_crm_contact(customer_id, email).await?))
        })
        .await?;

    // The vendor returns the same customer for every retry of this key.
    let billing_customer_id = run
        .step("ensure_billing_customer", async |_| {
            Ok(json!(
                v.create_billing_customer(&format!("billing_customer:{customer_id}"), email)
                    .await?
            ))
        })
        .await?;

    // Save branch decisions so retries follow the same steps even if customer state changes.
    let need = run
        .step("decide", async |tx| {
            let row = tx
                .query_one(
                    "select welcomed_at is null from onboard.customers where id = $1",
                    &[&customer_id],
                )
                .await?;
            Ok(json!({
                "setup_fee": plan == "pro",
                "welcome": row.try_get::<_, bool>(0)?,
            }))
        })
        .await?;

    // Lookup recovers a charge made before a crash.
    let setup_fee_key = format!("setup_fee:{customer_id}");
    let setup_fee_charge_id = if need["setup_fee"] == true {
        run.step("ensure_setup_fee", async |_| {
            if let Some(id) = v.find_charge(&setup_fee_key).await? {
                return Ok(json!(id));
            }
            let billing_customer_id = billing_customer_id
                .as_i64()
                .ok_or("billing customer ID must be an integer")?;
            Ok(json!(
                v.charge(&setup_fee_key, billing_customer_id, SETUP_FEE_CENTS)
                    .await?
            ))
        })
        .await?
    } else {
        json!(null)
    };

    // With no vendor key or lookup, send at most once. An interrupted send requires
    // an operator to check the outbox.
    let welcomed = need["welcome"] == true;
    if welcomed {
        run.step_once("send_welcome_email", async || {
            Ok(json!(v.send_email(email, "welcome").await?))
        })
        .await?;
    }

    run.step("link_customer", async |tx| {
        tx.execute(
            "update onboard.customers
             set crm_contact_id = $2, billing_customer_id = $3, setup_fee_charge_id = $4,
                 welcomed_at = case when $5 then coalesce(welcomed_at, now()) else welcomed_at end
             where id = $1",
            &[
                &customer_id,
                &crm_contact_id.as_i64(),
                &billing_customer_id.as_i64(),
                &setup_fee_charge_id.as_i64(),
                &welcomed,
            ],
        )
        .await?;
        Ok(json!(null))
    })
    .await?;

    // Duplicate notifications are harmless, so this step may repeat.
    run.step("notify_slack", async |_| {
        v.post_slack(&format!("onboarded {email} on {plan}"))
            .await?;
        Ok(json!(null))
    })
    .await?;

    tracing::info!("onboarded customer {customer_id}");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (database_url, client) = harness::start().await?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("submit") => {
            let email = args.next().ok_or("expected submit <email> [plan]")?;
            let plan = args.next().unwrap_or_else(|| "pro".into());
            // One run per email, so onboarding a customer again returns the first run.
            let run = Producer::new(&client, "onboard", VERSION)
                .submit(&email, &json!({"email": email, "plan": plan}))
                .await?;
            let status = if run.created {
                "submitted"
            } else {
                "already submitted"
            };
            tracing::info!("{status} run {} for {email}", run.id);
            Ok(())
        }
        Some("work") | None => {
            let spec = std::env::var("FAULTS").unwrap_or_default();
            let faults = Faults::parse(&spec)?;
            if !spec.trim().is_empty() {
                tracing::warn!("fault plan: {spec}");
            }
            let mut vendors = Vendors::new(harness::connect(&database_url).await?, faults);
            Worker::new(client, "onboard", VERSION)
                .lease(LEASE)
                .step_timeout(STEP_TIMEOUT)
                .run(shutdown_signal(), async |run| {
                    onboard(run, &mut vendors).await
                })
                .await
        }
        Some("each") => harness::passed(chaos::each(&client).await?),
        Some("chaos") => {
            let seed = args.next().map_or(Ok(1), |s| s.parse())?;
            let customers = args.next().map_or(Ok(50), |s| s.parse())?;
            let workers = args.next().map_or(Ok(3), |s| s.parse())?;
            harness::passed(chaos::random(&client, seed, customers, workers).await?)
        }
        Some("race") => {
            let producers = args.next().map_or(Ok(10), |s| s.parse())?;
            let workers = args.next().map_or(Ok(10), |s| s.parse())?;
            let customers = args.next().map_or(Ok(100), |s| s.parse())?;
            harness::passed(
                chaos::race(&database_url, &client, producers, workers, customers).await?,
            )
        }
        Some("check") => harness::passed(chaos::check(&client).await?),
        _ => Err(
            "expected submit <email> [plan], work, each, chaos [seed] [customers] [workers], \
             race [producers] [workers] [customers], or check"
                .into(),
        ),
    }
}
