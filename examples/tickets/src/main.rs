mod mock_issuer;

use mock_issuer::MockIssuer;
use resume::{Execution, Job, Producer, Result, Worker, shutdown_signal};
use serde_json::json;

pub const VERSION: &str = "1";

async fn tickets(run: &Job, execution: &mut Execution<'_>, issuer: &mut MockIssuer) -> Result<()> {
    // The run's idempotency key, so duplicate submissions of a request share one run.
    let request_id = &run.idempotency_key;
    let attendee = run.input["attendee"]
        .as_str()
        .ok_or("attendee must be a string")?;

    execution
        .step("validate", async |_| {
            if request_id.trim().is_empty() || attendee.trim().is_empty() {
                return Err("request_id and attendee must not be empty".into());
            }
            Ok(json!(null))
        })
        .await?;

    let ticket = execution
        .step_once("issue", async || {
            Ok(json!(issuer.issue(request_id, attendee).await?))
        })
        .await?;
    let ticket_id = ticket.as_i64().ok_or("ticket ID must be an integer")?;

    let receipt = execution
        .step("receipt", async |_| {
            Ok(json!(format!("ticket #{ticket_id} for {attendee}")))
        })
        .await?;
    tracing::info!("receipt = {receipt}");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (database_url, client) = harness::start().await?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("submit") => {
            let request_id = args
                .next()
                .ok_or("expected submit <request_id> [attendee]")?;
            let attendee = args.next().unwrap_or_else(|| "Ada".into());
            let run = Producer::new(&client, "tickets", VERSION)
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
            let mut issuer = MockIssuer::new(harness::connect(&database_url).await?);
            Worker::new(client, "tickets", VERSION)
                .run(shutdown_signal(), async |run, execution| {
                    tickets(run, execution, &mut issuer).await
                })
                .await
        }
        _ => Err("expected submit <request_id> [attendee] or work".into()),
    }
}
