mod mock_issuer;

use mock_issuer::MockIssuer;
use resume::{Result, Run};
use serde_json::json;
use tokio_postgres::{Client, NoTls};

async fn tickets(client: &mut Client, run: &Run, issuer: &mut MockIssuer) -> Result<()> {
    // Supplied by the caller so duplicate enqueues can identify the same request.
    let request_id = run.input["request_id"]
        .as_str()
        .ok_or("request_id must be a string")?;
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
        .step(client, "issue", async |_| {
            // Intentionally outside the step's transaction.
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
    let mut client = connect(&database_url).await?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("enqueue") => {
            let request_id = args
                .next()
                .ok_or("expected enqueue <request_id> [attendee]")?;
            let attendee = args.next().unwrap_or_else(|| "Ada".into());
            let id = resume::enqueue(
                &client,
                "tickets",
                &json!({"request_id": request_id, "attendee": attendee}),
                1,
            )
            .await?;
            tracing::info!("enqueued run {id} for request {request_id}");
            Ok(())
        }
        Some("work") | None => {
            let mut issuer = MockIssuer::new(connect(&database_url).await?);
            resume::work(&mut client, "tickets", 30, async |client, run| {
                tickets(client, run, &mut issuer).await
            })
            .await
        }
        _ => Err("expected enqueue <request_id> [attendee] or work".into()),
    }
}
