mod mock_billing;

use std::time::{Duration, Instant};

use harness::{Pool, Rng, check_invariants};
use mock_billing::Billing;
use resume::{Job, Producer, Result, RetryPolicy, Worker, lock_resource, shutdown_signal};
use serde_json::{Value, json};
use tokio_postgres::{Client, Transaction};

pub const VERSION: &str = "1";

const PLANS: [&str; 3] = ["free", "pro", "team"];
const LEASE: Duration = Duration::from_secs(5);
const STEP_TIMEOUT: Duration = Duration::from_secs(3);
const INVARIANTS: &str = include_str!("../invariants.sql");

/// Applies the latest requested plan. `PLAIN=1` omits `is_latest`, allowing stale requests
/// to overwrite newer plans.
async fn change_plan(run: &Job, billing: &mut Billing, plain: bool) -> Result<()> {
    let customer_id = run.input["customer_id"]
        .as_i64()
        .ok_or("customer_id must be an integer")?;
    let plan = run.input["plan"].as_str().ok_or("plan must be a string")?;

    let applied = run
        .step("set_plan", async |tx| {
            if !plain && !run.is_latest(tx).await? {
                return Ok(json!({"skipped": "superseded by a newer request"}));
            }
            set_plan(billing, tx, customer_id, plan).await
        })
        .await?;

    if applied.get("skipped").is_some() {
        tracing::info!("a newer request for customer {customer_id} replaced {plan}");
    } else {
        tracing::info!("customer {customer_id} is on {applied}");
    }
    Ok(())
}

/// Both modes lock the customer, so runs for one customer take turns. The lock alone does not
/// stop an older request from taking its turn last; the is_latest check does.
async fn set_plan(
    billing: &mut Billing,
    tx: &Transaction<'_>,
    customer_id: i64,
    plan: &str,
) -> Result<Value> {
    lock_resource(tx, &format!("customer:{customer_id}")).await?;
    billing.set_plan(customer_id, plan).await?;
    tx.execute(
        "insert into subscription.customers (id, plan) values ($1, $2)
         on conflict (id) do update set plan = excluded.plan",
        &[&customer_id, &plan],
    )
    .await?;
    Ok(json!(plan))
}

/// Changes plans while workers apply them under billing latency and errors, then checks
/// that each customer's final plan matches their latest request.
async fn race(
    client: &Client,
    customers: i64,
    changes: usize,
    workers: usize,
    plain: bool,
) -> Result<bool> {
    const LIMIT: Duration = Duration::from_secs(120);

    harness::reset(
        client,
        "subscription",
        "subscription.customers, billing.subscriptions",
    )
    .await?;

    let mode = if plain { "plain" } else { "is_latest" };
    let mut pool = Pool::new(&format!("subscription-{mode}"))?;
    for n in 0..workers {
        pool.spawn(&[
            ("SEED", &n.to_string()),
            ("LATENCY_MS", "50"),
            ("ERROR_CHANCE", "0.2"),
            ("PLAIN", if plain { "1" } else { "0" }),
        ])?;
    }

    let producer = Producer::new(client, "subscription", VERSION).retry(RetryPolicy {
        max_attempts: 20,
        delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(500),
    });
    let mut rng = Rng::new(0);
    let start = Instant::now();
    for change in 0..changes {
        for id in 0..customers {
            let plan = PLANS[rng.below(PLANS.len() as u64) as usize];
            producer
                .submit_for(
                    &format!("customer:{id}"),
                    &format!("customer-{id}-change-{change}"),
                    &json!({"customer_id": id, "plan": plan}),
                )
                .await?;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    harness::drive(
        client,
        "subscription",
        LIMIT,
        Duration::from_millis(100),
        async || Ok(()),
    )
    .await?;
    pool.stop().await;

    // Count from the saved steps, which record each skip exactly once.
    let superseded: i64 = client
        .query_one(
            "select count(*) from resume.steps s join resume.runs r on r.id = s.run_id
             where r.workflow = 'subscription' and s.output ? 'skipped'",
            &[],
        )
        .await?
        .try_get(0)?;
    println!(
        "{mode}: {customers} customers x {changes} changes, {workers} workers, {:.1}s; \
         {superseded} steps skipped as superseded",
        start.elapsed().as_secs_f64()
    );
    harness::verify(client, "subscription", INVARIANTS, &pool.logs).await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (database_url, client) = harness::start().await?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("work") | None => {
            let mut billing = Billing::new(
                harness::connect(&database_url).await?,
                harness::env_or("SEED", 0)?,
                harness::env_or("LATENCY_MS", 0)?,
                harness::env_or("ERROR_CHANCE", 0.0)?,
            );
            let plain = harness::env_or("PLAIN", 0)? == 1;
            Worker::new(client, "subscription", VERSION)
                .lease(LEASE)
                .step_timeout(STEP_TIMEOUT)
                .run(shutdown_signal(), async |run| {
                    change_plan(run, &mut billing, plain).await
                })
                .await
        }
        Some("race") => {
            let customers = args.next().map_or(Ok(20), |s| s.parse())?;
            let changes = args.next().map_or(Ok(10), |s| s.parse())?;
            let workers = args.next().map_or(Ok(10), |s| s.parse())?;
            let plain = args.next().as_deref() == Some("plain");
            harness::passed(race(&client, customers, changes, workers, plain).await?)
        }
        Some("check") => {
            harness::passed(check_invariants(&client, "subscription", INVARIANTS).await?)
        }
        _ => Err(
            "expected work, race [customers] [changes] [workers] [is_latest|plain], or check"
                .into(),
        ),
    }
}
