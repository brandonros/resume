mod mock_issuer;

use mock_issuer::MockIssuer;
use resume::{Producer, Result, Run, Worker, shutdown_signal};
use serde_json::json;
use tokio_postgres::{Client, NoTls};

async fn tickets(client: &mut Client, run: &Run, issuer: &mut MockIssuer) -> Result<()> {
    // The run's idempotency key, so duplicate submissions of a request share one run.
    let request_id = &run.idempotency_key;
    let attendee = run.input["attendee"]
        .as_str()
        .ok_or("attendee must be a string")?;

    run.step(client, "validate", async |_| {
        if request_id.trim().is_empty() || attendee.trim().is_empty() {
            return Err("request_id and attendee must not be empty".into());
        }
        Ok(json!(null))
    })
    .await?;

    let ticket = run
        .step_once(client, "issue", async || {
            Ok(json!(issuer.issue(request_id, attendee).await?))
        })
        .await?;
    let ticket_id = ticket.as_i64().ok_or("ticket ID must be an integer")?;

    let receipt = run
        .step(client, "receipt", async |_| {
            Ok(json!(format!("ticket #{ticket_id} for {attendee}")))
        })
        .await?;
    tracing::info!("run {}: receipt = {receipt}", run.id);
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
            let request_id = args
                .next()
                .ok_or("expected submit <request_id> [attendee]")?;
            let attendee = args.next().unwrap_or_else(|| "Ada".into());
            let run = Producer::new(&client, "tickets")
                .submit(&request_id, &json!({"attendee": attendee}))
                .await?;
            let status = if run.created {
                "submitted"
            } else {
                "already submitted"
            };
            tracing::info!("{status} run {} for request {request_id}", run.id);
            Ok(())
        }
        Some("work") | None => {
            let mut issuer = MockIssuer::new(connect(&database_url).await?);
            Worker::new(client, "tickets")
                .run(shutdown_signal(), async |client, run| {
                    tickets(client, run, &mut issuer).await
                })
                .await
        }
        _ => Err("expected submit <request_id> [attendee] or work".into()),
    }
}
