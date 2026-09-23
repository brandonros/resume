use std::time::Duration;

use resume::{Producer, Result, Run, Worker};
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

    match std::env::args().nth(1).as_deref() {
        Some("enqueue") => {
            let id = Producer::new(&client, "counter")
                .enqueue(&json!({"amount": 1}), 1)
                .await?;
            tracing::info!("enqueued run {id}");
            Ok(())
        }
        Some("work") | None => Worker::new(client, "counter", 30).run(counter).await,
        _ => Err("expected enqueue or work".into()),
    }
}
