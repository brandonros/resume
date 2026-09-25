mod mock_vendors;

use mock_vendors::{FAULTS, Vendors};
use resume::{Producer, Result, Run, Worker, lock_resource};
use serde_json::json;
use tokio_postgres::{Client, NoTls};

const SETUP_FEE_CENTS: i64 = 5000;

async fn onboard(client: &mut Client, run: &Run, v: &mut Vendors) -> Result<()> {
    let email = run.input["email"]
        .as_str()
        .ok_or("email must be a string")?;
    let plan = run.input["plan"].as_str().ok_or("plan must be a string")?;

    // 1. Pure check. Bad input fails every attempt the same way, so the run fails once its
    //    attempts are used up.
    run.step(client, "validate", async |_| {
        if !email.contains('@') {
            return Err(format!("invalid email {email:?}").into());
        }
        if !matches!(plan, "free" | "pro") {
            return Err(format!("unknown plan {plan:?}").into());
        }
        Ok(json!(null))
    })
    .await?;

    // 2. Our own database. The insert commits with the step's result: exactly once.
    let customer_id = run
        .step(client, "create_customer", async |tx| {
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

    // 3. A vendor we can search but that has no idempotency key: find the contact or create
    //    it. A retry after a crash finds the contact instead of creating a second one. The
    //    lock stops another run or workflow for this customer from checking at the same time
    //    and creating a duplicate.
    let crm_contact_id = run
        .ensure(
            client,
            "ensure_crm_contact",
            v,
            async |v, tx| {
                lock_resource(tx, &format!("crm_contact:{customer_id}")).await?;
                Ok(v.find_crm_contact(customer_id).await?.map(|id| json!(id)))
            },
            async |v, _| Ok(json!(v.create_crm_contact(customer_id, email).await?)),
        )
        .await?;

    // 4. A vendor that accepts idempotency keys needs no check: the key names the customer, so
    //    every retry gets the same billing customer back.
    let billing_customer_id = run
        .step(client, "ensure_billing_customer", async |_| {
            Ok(json!(
                v.create_billing_customer(&format!("billing_customer:{customer_id}"), email)
                    .await?
            ))
        })
        .await?;

    // 5. Decide what this run needs to do. Saving the answer keeps every attempt on the same
    //    branches, even if the customer's state changes in between.
    let need = run
        .step(client, "decide", async |tx| {
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

    // 6. Ask billing whether the fee was already charged, and charge it only if not. After a
    //    crash between the charge and the commit, the retry finds the charge.
    let setup_fee_key = format!("setup_fee:{customer_id}");
    let setup_fee_charge_id = if need["setup_fee"] == true {
        run.ensure(
            client,
            "ensure_setup_fee",
            v,
            async |v, _| Ok(v.find_charge(&setup_fee_key).await?.map(|id| json!(id))),
            async |v, _| {
                let billing_customer_id = billing_customer_id
                    .as_i64()
                    .ok_or("billing customer ID must be an integer")?;
                Ok(json!(
                    v.charge(&setup_fee_key, billing_customer_id, SETUP_FEE_CENTS)
                        .await?
                ))
            },
        )
        .await?
    } else {
        json!(null)
    };

    // 7. The email vendor has no key and no lookup, so the send runs at most once. If an
    //    attempt stops after starting it, the run fails and someone must check the outbox.
    let welcomed = need["welcome"] == true;
    if welcomed {
        run.step_once(client, "send_welcome_email", async || {
            Ok(json!(v.send_email(email, "welcome").await?))
        })
        .await?;
    }

    // 8. Record what the vendors hold in our own database, in one transaction.
    run.step(client, "link_customer", async |tx| {
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

    // 9. A duplicate message is harmless, so a plain step is enough: at least once.
    run.step(client, "notify_slack", async |_| {
        v.post_slack(&format!("onboarded {email} on {plan}"))
            .await?;
        Ok(json!(null))
    })
    .await?;

    tracing::info!("run {}: onboarded customer {customer_id}", run.id);
    Ok(())
}

async fn connect(database_url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!("postgres: {error}");
        }
    });
    Ok(client)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let database_url = std::env::var("DATABASE_URL")?;
    let client = connect(&database_url).await?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("submit") => {
            let email = args.next().ok_or("expected submit <email> [plan]")?;
            let plan = args.next().unwrap_or_else(|| "pro".into());
            // One run per email, so onboarding a customer again returns the first run.
            let run = Producer::new(&client, "onboard")
                .submit(&email, &json!({"email": email, "plan": plan}), 3)
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
            let fault = std::env::var("FAULT").ok().filter(|f| !f.is_empty());
            if let Some(fault) = &fault {
                if !FAULTS.contains(&fault.as_str()) {
                    return Err(format!("unknown FAULT {fault}; expected one of {FAULTS:?}").into());
                }
                tracing::warn!("injecting fault {fault}");
            }
            let mut vendors = Vendors::new(connect(&database_url).await?, fault);
            // A short lease, so a retry after a failure or crash comes quickly.
            Worker::new(client, "onboard", 5)
                .run(async |client, run| onboard(client, run, &mut vendors).await)
                .await
        }
        _ => Err("expected submit <email> [plan] or work".into()),
    }
}
