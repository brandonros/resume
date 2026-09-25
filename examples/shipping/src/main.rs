mod mock_warehouse;

use std::time::{Duration, Instant};

use harness::{Pool, Rng, check_invariants};
use mock_warehouse::Warehouse;
use resume::{Producer, Result, Run, Worker, shutdown_signal};
use serde_json::{Value, json};
use tokio_postgres::{Client, Transaction};

/// The version of this workflow's input and steps; producers and workers must agree.
pub const VERSION: &str = "1";
const LEASE: Duration = Duration::from_secs(5);
const STEP_TIMEOUT: Duration = Duration::from_secs(3);
const INVARIANTS: &str = include_str!("../invariants.sql");

/// Ships an order if it is still paid, checking and shipping in one step. `SEPARATE=1` checks
/// in one step and ships in the next, to show what a check made apart from its action lets
/// through.
async fn ship_order(run: &Run, warehouse: &mut Warehouse, separate: bool) -> Result<()> {
    let order_id = run.input["order_id"]
        .as_i64()
        .ok_or("order_id must be an integer")?;

    if separate {
        let paid = run
            .step("check_paid", async |tx| {
                Ok(json!(status(tx, order_id, false).await? == "paid"))
            })
            .await?;
        if paid == true {
            run.step("ship", async |tx| ship(warehouse, tx, order_id).await)
                .await?;
        }
        return Ok(());
    }

    let shipped = run
        .step("ship", async |tx| {
            // Lock the order, so a cancellation waits until this step commits.
            match status(tx, order_id, true).await?.as_str() {
                "paid" => ship(warehouse, tx, order_id).await,
                other => Ok(json!({"skipped": format!("order is {other}")})),
            }
        })
        .await?;
    if let Some(reason) = shipped.get("skipped") {
        tracing::info!("order {order_id} not shipped: {reason}");
    }
    Ok(())
}

async fn status(tx: &Transaction<'_>, order_id: i64, lock: bool) -> Result<String> {
    let query = if lock {
        "select status from shipping.orders where id = $1 for update"
    } else {
        "select status from shipping.orders where id = $1"
    };
    Ok(tx.query_one(query, &[&order_id]).await?.try_get(0)?)
}

async fn ship(warehouse: &mut Warehouse, tx: &Transaction<'_>, order_id: i64) -> Result<Value> {
    let shipment = warehouse.ship(order_id).await?;
    tx.execute(
        "update shipping.orders set status = 'shipped' where id = $1",
        &[&order_id],
    )
    .await?;
    Ok(json!(shipment))
}

/// Submits a shipping run per order while customers keep cancelling random orders that are
/// still paid. Then checks that no order shipped after its cancellation succeeded.
async fn race(client: &Client, orders: i64, workers: usize, separate: bool) -> Result<bool> {
    const LIMIT: Duration = Duration::from_secs(120);

    harness::reset(
        client,
        "shipping",
        "shipping.orders, shipping.cancellations, warehouse.shipments",
    )
    .await?;
    let producer = Producer::new(client, "shipping", VERSION);
    for id in 0..orders {
        client
            .execute(
                "insert into shipping.orders (id, status) values ($1, 'paid')",
                &[&id],
            )
            .await?;
        producer
            .submit(&format!("ship-{id}"), &json!({"order_id": id}))
            .await?;
    }

    let mode = if separate { "separate" } else { "same_step" };
    let mut pool = Pool::new(&format!("shipping-{mode}"))?;
    for n in 0..workers {
        pool.spawn(&[
            ("SEED", &n.to_string()),
            ("LATENCY_MS", "30"),
            ("SEPARATE", if separate { "1" } else { "0" }),
        ])?;
    }

    // A customer's cancellation succeeds only while the order is still paid.
    let mut rng = Rng::new(0);
    let start = Instant::now();
    harness::drive(
        client,
        "shipping",
        LIMIT,
        Duration::from_millis(5),
        async || {
            let id = rng.below(orders as u64) as i64;
            let cancelled = client
            .execute(
                "update shipping.orders set status = 'cancelled' where id = $1 and status = 'paid'",
                &[&id],
            )
            .await?;
            if cancelled == 1 {
                client
                    .execute(
                        "insert into shipping.cancellations (order_id) values ($1)",
                        &[&id],
                    )
                    .await?;
            }
            Ok(())
        },
    )
    .await?;
    pool.stop().await;

    let row = client
        .query_one(
            "select count(*) filter (where status = 'shipped'),
                    count(*) filter (where status = 'cancelled')
             from shipping.orders",
            &[],
        )
        .await?;
    println!(
        "{mode}: {orders} orders, {workers} workers, {:.1}s; {} shipped, {} cancelled",
        start.elapsed().as_secs_f64(),
        row.try_get::<_, i64>(0)?,
        row.try_get::<_, i64>(1)?
    );
    harness::verify(client, "shipping", INVARIANTS, &pool.logs).await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (database_url, client) = harness::start().await?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("work") | None => {
            let mut warehouse = Warehouse::new(
                harness::connect(&database_url).await?,
                harness::env_or("SEED", 0)?,
                harness::env_or("LATENCY_MS", 0)?,
            );
            let separate = harness::env_or("SEPARATE", 0)? == 1;
            Worker::new(client, "shipping", VERSION)
                .lease(LEASE)
                .step_timeout(STEP_TIMEOUT)
                .run(shutdown_signal(), async |run| {
                    ship_order(run, &mut warehouse, separate).await
                })
                .await
        }
        Some("race") => {
            let orders = args.next().map_or(Ok(200), |s| s.parse())?;
            let workers = args.next().map_or(Ok(10), |s| s.parse())?;
            let separate = args.next().as_deref() == Some("separate");
            harness::passed(race(&client, orders, workers, separate).await?)
        }
        Some("check") => harness::passed(check_invariants(&client, "shipping", INVARIANTS).await?),
        _ => Err("expected work, race [orders] [workers] [same_step|separate], or check".into()),
    }
}
