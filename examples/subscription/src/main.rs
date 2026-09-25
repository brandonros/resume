mod mock_billing;

use std::time::{Duration, Instant};

use harness::{Pool, Rng, check_history, check_invariants, log_files, pending};
use mock_billing::Billing;
use resume::{Producer, Result, RetryPolicy, Run, Worker, lock_resource, shutdown_signal};
use serde_json::json;
use tokio_postgres::{Client, NoTls, Transaction};

const PLANS: [&str; 3] = ["free", "pro", "team"];
const LEASE: Duration = Duration::from_secs(5);
const STEP_TIMEOUT: Duration = Duration::from_secs(3);
const INVARIANTS: &str = include_str!("../invariants.sql");

/// Sets a customer's plan at billing and in our record. With `step_latest`, a run whose
/// request a newer one replaced does nothing. `PLAIN=1` uses `step` instead, to show the stale
/// plans that `step_latest` prevents.
async fn change_plan(
    client: &mut Client,
    run: &Run,
    billing: &mut Billing,
    plain: bool,
) -> Result<()> {
    let customer_id = run.input["customer_id"]
        .as_i64()
        .ok_or("customer_id must be an integer")?;
    let plan = run.input["plan"].as_str().ok_or("plan must be a string")?;

    // Both modes lock the customer, so runs for one customer take turns. The lock alone does
    // not stop an older request from taking its turn last; step_latest's check does.
    let set_plan = async |tx: &Transaction<'_>| {
        lock_resource(tx, &format!("customer:{customer_id}")).await?;
        billing.set_plan(customer_id, plan).await?;
        tx.execute(
            "insert into subscription.customers (id, plan) values ($1, $2)
             on conflict (id) do update set plan = excluded.plan",
            &[&customer_id, &plan],
        )
        .await?;
        Ok(json!(plan))
    };
    let applied = if plain {
        Some(run.step(client, "set_plan", set_plan).await?)
    } else {
        run.step_latest(client, "set_plan", set_plan).await?
    };

    match applied {
        Some(plan) => tracing::info!("customer {customer_id} is on {plan}"),
        None => tracing::info!("a newer request for customer {customer_id} replaced {plan}"),
    }
    Ok(())
}

/// Plays the customers: each asks for a new plan `changes` times, one request after another,
/// while `workers` workers apply them with billing latency and errors. Then checks that every
/// customer ends on the plan they asked for last.
async fn race(
    client: &Client,
    customers: i64,
    changes: usize,
    workers: usize,
    plain: bool,
) -> Result<bool> {
    const LIMIT: Duration = Duration::from_secs(120);

    client
        .batch_execute(
            "truncate subscription.customers, billing.subscriptions;
             delete from resume.runs where workflow = 'subscription';",
        )
        .await?;

    let mode = if plain { "step" } else { "step_latest" };
    let mut pool = Pool::new(&format!("subscription-{mode}"))?;
    for n in 0..workers {
        pool.spawn(&[
            ("SEED", &n.to_string()),
            ("LATENCY_MS", "50"),
            ("ERROR_CHANCE", "0.2"),
            ("PLAIN", if plain { "1" } else { "0" }),
        ])?;
    }

    let producer = Producer::new(client, "subscription").retry(RetryPolicy {
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

    while pending(client, "subscription").await? > 0 && start.elapsed() < LIMIT {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    pool.stop().await;

    let mut superseded = 0;
    for log in log_files(&pool.logs)? {
        superseded += std::fs::read_to_string(log)?
            .matches("superseded by a newer run")
            .count();
    }
    println!(
        "{mode}: {customers} customers x {changes} changes, {workers} workers, {:.1}s; \
         {superseded} steps skipped as superseded",
        start.elapsed().as_secs_f64()
    );
    let history = check_history(&pool.logs)?;
    Ok(check_invariants(client, "subscription", INVARIANTS).await? && history)
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

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
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
        Some("work") | None => {
            let mut billing = Billing::new(
                connect(&database_url).await?,
                env_or("SEED", 0)?,
                env_or("LATENCY_MS", 0)?,
                env_or("ERROR_CHANCE", 0.0)?,
            );
            let plain = env_or("PLAIN", 0)? == 1;
            Worker::new(client, "subscription")
                .lease(LEASE)
                .step_timeout(STEP_TIMEOUT)
                .run(shutdown_signal(), async |client, run| {
                    change_plan(client, run, &mut billing, plain).await
                })
                .await
        }
        Some("race") => {
            let customers = args.next().map_or(Ok(20), |s| s.parse())?;
            let changes = args.next().map_or(Ok(10), |s| s.parse())?;
            let workers = args.next().map_or(Ok(10), |s| s.parse())?;
            let plain = args.next().as_deref() == Some("step");
            if race(&client, customers, changes, workers, plain).await? {
                Ok(())
            } else {
                Err("invariants violated".into())
            }
        }
        Some("check") => {
            if check_invariants(&client, "subscription", INVARIANTS).await? {
                Ok(())
            } else {
                Err("invariants violated".into())
            }
        }
        _ => Err(
            "expected work, race [customers] [changes] [workers] [step|step_latest], or check"
                .into(),
        ),
    }
}
