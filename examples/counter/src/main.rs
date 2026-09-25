use std::time::Duration;

use resume::{Producer, Result, RetryPolicy, Run, Worker, shutdown_signal};
use serde_json::json;
use tokio_postgres::{Client, NoTls};

async fn counter(client: &mut Client, run: &Run) -> Result<()> {
    let amount = run.input["amount"]
        .as_i64()
        .ok_or("amount must be an integer")?;

    run.step(client, "create", async |tx| {
        tx.execute(
            "insert into counter.results (run_id, value) values ($1, 0)",
            &[&run.id],
        )
        .await?;
        Ok(json!(0))
    })
    .await?;

    run.step(client, "add", async |tx| {
        let row = tx
            .query_one(
                "update counter.results set value = value + $2
                 where run_id = $1 returning value",
                &[&run.id, &amount],
            )
            .await?;
        // A pause to make it easy to kill the worker before this step commits.
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(json!(row.try_get::<_, i64>(0)?))
    })
    .await?;

    let output = run
        .step(client, "double", async |tx| {
            let row = tx
                .query_one(
                    "update counter.results set value = value * 2
                     where run_id = $1 returning value",
                    &[&run.id],
                )
                .await?;
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(json!(row.try_get::<_, i64>(0)?))
        })
        .await?;

    tracing::info!("run {}: result = {output}", run.id);
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let (client, connection) =
        tokio_postgres::connect(&std::env::var("DATABASE_URL")?, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!("postgres: {error}");
        }
    });

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("submit") => {
            let key = args.next().ok_or("expected submit <key>")?;
            let run = Producer::new(&client, "counter")
                .retry(RetryPolicy {
                    max_attempts: 1,
                    ..RetryPolicy::default()
                })
                .submit(&key, &json!({"amount": 1}))
                .await?;
            let status = if run.created {
                "submitted"
            } else {
                "already submitted"
            };
            tracing::info!("{status} run {} for key {key}", run.id);
            Ok(())
        }
        Some("work") | None => {
            Worker::new(client, "counter")
                .run(shutdown_signal(), counter)
                .await
        }
        _ => Err("expected submit <key> or work".into()),
    }
}
