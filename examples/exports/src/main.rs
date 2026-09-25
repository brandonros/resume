mod mock_vendor;

use std::time::Duration;

use mock_vendor::MockVendor;
use resume::{Permanent, Producer, Result, RetryPolicy, Run, Snooze, Worker, shutdown_signal};
use serde_json::json;

const VERSION: &str = "1";
const CHECK_AGAIN: Duration = Duration::from_secs(2);

async fn export(run: &Run, vendor: &MockVendor) -> Result<()> {
    let ready_after = run.input["ready_after_seconds"]
        .as_u64()
        .ok_or_else(|| Permanent("ready_after_seconds must be a nonnegative integer".into()))?;

    let started = run
        .step("start_export", async |_| {
            let export_id = vendor
                .start(&run.idempotency_key, Duration::from_secs(ready_after))
                .await?;
            tracing::info!(export_id, "vendor accepted export");
            Ok(json!({"export_id": export_id}))
        })
        .await?;
    let export_id = started["export_id"].as_i64().ok_or("missing export ID")?;

    let finished = run
        .step("wait_for_export", async |_| {
            let Some(url) = vendor.download_url(export_id).await? else {
                tracing::info!(export_id, "still processing; check again in 2 seconds");
                // The step stays unfinished. The worker releases this run and handles other work.
                return Err(Snooze(CHECK_AGAIN).into());
            };
            Ok(json!({"url": url}))
        })
        .await?;
    let url = finished["url"].as_str().ok_or("missing export URL")?;

    run.step("record_export", async |tx| {
        tx.execute(
            "insert into exports.results (run_id, export_id, url) values ($1, $2, $3)",
            &[&run.id, &export_id, &url],
        )
        .await?;
        Ok(json!({"url": url}))
    })
    .await?;
    tracing::info!(export_id, url, "export ready");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (database_url, client) = harness::start().await?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("submit") => {
            let key = args
                .next()
                .ok_or("expected submit <key> [ready_after_seconds]")?;
            let ready_after: u64 = args.next().unwrap_or_else(|| "10".into()).parse()?;
            let run = Producer::new(&client, "exports", VERSION)
                // Several snoozes still fit within a single allowed attempt.
                .retry(RetryPolicy {
                    max_attempts: 1,
                    ..RetryPolicy::default()
                })
                .deadline(Duration::from_secs(300))
                .submit(&key, &json!({"ready_after_seconds": ready_after}))
                .await?;
            let status = if run.created {
                "submitted"
            } else {
                "already submitted"
            };
            tracing::info!("{status} run {} for export {key}", run.id);
            Ok(())
        }
        Some("work") | None => {
            let vendor = MockVendor::new(harness::connect(&database_url).await?);
            Worker::new(client, "exports", VERSION)
                .run(shutdown_signal(), async |run| export(run, &vendor).await)
                .await
        }
        _ => Err("expected submit <key> [ready_after_seconds] or work".into()),
    }
}
