use std::time::Duration;

use resume::{Permanent, Producer, Result, Run, Worker, shutdown_signal};
use serde_json::json;
use tokio_postgres::{Client, NoTls};

const VERSION: &str = "1";

async fn remind(client: &mut Client, run: &Run) -> Result<()> {
    let message = run.input["message"]
        .as_str()
        .ok_or_else(|| Permanent("message must be a string".into()))?;

    // The worker only reaches this workflow when the scheduled run is eligible.
    run.step(client, "deliver", async |tx| {
        tx.execute(
            "insert into reminders.deliveries (run_id, message) values ($1, $2)",
            &[&run.id, &message],
        )
        .await?;
        Ok(json!({"message": message}))
    })
    .await?;
    tracing::info!(message, "reminder delivered to the local inbox");
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
        Some(command @ ("submit" | "submit-at")) => {
            let key = args
                .next()
                .ok_or("expected submit <key> [delay_seconds] [message] or submit-at <key> <timestamp> [message]")?;
            let producer = Producer::new(&client, "reminders", VERSION);
            let (producer, schedule) = if command == "submit-at" {
                let timestamp = args.next().ok_or("expected a timestamp with Z or a UTC offset")?;
                (producer.at(timestamp.parse()?), format!("at {timestamp}"))
            } else {
                let delay_seconds: u64 = args.next().unwrap_or_else(|| "10".into()).parse()?;
                (producer.delay(Duration::from_secs(delay_seconds)), format!("in {delay_seconds}s"))
            };
            let message = args.next().unwrap_or_else(|| "Time to stretch".into());
            let run = producer
                .submit(&key, &json!({"message": message}))
                .await?;
            if run.created {
                tracing::info!(
                    "scheduled run {} for key {key}, eligible {schedule}",
                    run.id
                );
            } else {
                tracing::info!(
                    "run {} already submitted; keeping its original schedule",
                    run.id
                );
            }
            Ok(())
        }
        Some("work") | None => {
            Worker::new(client, "reminders", VERSION)
                .run(shutdown_signal(), remind)
                .await
        }
        _ => Err("expected submit <key> [delay_seconds] [message], submit-at <key> <timestamp> [message], or work".into()),
    }
}
