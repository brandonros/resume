//! Fan-out/join, durable sleep, and a recurring job. Run against a disposable database
//! with schema.sql installed: DATABASE_URL=postgresql://... cargo run --example flow
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use resume::{Error, Job, Result, Retry, submit, submit_at, work};
use serde_json::json;
use tokio_postgres::{Client, NoTls};

async fn connect() -> Result<Client> {
    let (client, connection) =
        tokio_postgres::connect(&std::env::var("DATABASE_URL")?, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("connection: {error}");
        }
    });
    Ok(client)
}

fn log_and_stop(job: &Job, error: &Error) -> Retry {
    eprintln!("job {} paused: {error}", job.id);
    Retry::Stop
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (mut orders, mut shipments, mut ticks) =
        (connect().await?, connect().await?, connect().await?);
    submit(
        &orders,
        "order",
        "order-1",
        &json!({"items": ["book", "lamp"]}),
    )
    .await?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    submit(&ticks, "tick", &now.to_string(), &json!({"at": now})).await?;

    let order = work(
        &mut orders,
        "order",
        async |job, steps| {
            // Children are submitted atomically with their spawn steps and run on other workers.
            let mut children = Vec::new();
            for item in job.input["items"].as_array().ok_or("missing items")? {
                let item = item.as_str().ok_or("item must be a string")?;
                children.push(
                    steps
                        .spawn(&format!("ship-{item}"), "ship", &json!(item))
                        .await?,
                );
            }
            // Releases the job; another claim replays the spawns and continues here.
            steps.sleep("cool-off", Duration::from_secs(2)).await?;
            let mut receipts = Vec::new();
            for child in children {
                receipts.push(steps.wait_for(child).await?);
            }
            println!("order {} complete: {receipts:?}", job.key);
            Ok(json!(receipts))
        },
        log_and_stop,
    );
    let ship = work(
        &mut shipments,
        "ship",
        async |job, steps| {
            steps
                .step("ship", async |_| Ok(json!({"shipped": job.input})))
                .await
        },
        log_and_stop,
    );
    let tick = work(
        &mut ticks,
        "tick",
        async |job, steps| {
            // Every 5 seconds. The next run's key is its time, so replay cannot double-schedule.
            let at = job.input["at"].as_u64().ok_or("missing at")?;
            let next = at + 5;
            steps
                .step("schedule-next", async |tx| {
                    let when = UNIX_EPOCH + Duration::from_secs(next);
                    submit_at(tx, "tick", &next.to_string(), &json!({"at": next}), when).await?;
                    Ok(json!(next))
                })
                .await?;
            println!("tick {at}");
            Ok(json!(null))
        },
        log_and_stop,
    );
    tokio::try_join!(order, ship, tick)?;
    Ok(())
}
