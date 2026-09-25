use std::time::Duration;

use resume::{Job, JobHandle, JobOutcome, Permanent, Producer, Result, Worker, shutdown_signal};
use serde_json::json;

const WORKFLOW: &str = "waiting";
const VERSION: &str = "1";
const USAGE: &str = "expected work or submit <key> [success|fail|slow] [timeout-ms]";

async fn process(job: &Job) -> Result<()> {
    job.step("prepare", async |_| {
        match job.input["mode"].as_str() {
            Some("success") => {}
            Some("slow") => tokio::time::sleep(Duration::from_secs(2)).await,
            Some("fail") => return Err(Permanent("example rejection".into()).into()),
            _ => return Err(Permanent("unknown mode".into()).into()),
        }
        Ok(json!({"prepared": true}))
    })
    .await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("work") if args.next().is_none() => {
            let (_, client) = harness::start().await?;
            Worker::new(client, WORKFLOW, VERSION)
                .run(shutdown_signal(), async |job| process(job).await)
                .await
        }
        Some("submit") => {
            let key = args.next().ok_or(USAGE)?;
            let mode = args.next().unwrap_or_else(|| "success".into());
            let timeout_ms: u64 = args.next().map(|s| s.parse()).transpose()?.unwrap_or(5_000);
            if !matches!(mode.as_str(), "success" | "fail" | "slow") || args.next().is_some() {
                return Err(USAGE.into());
            }
            let (_, client) = harness::start().await?;

            // Producer -> JobHandle: submission stores the job; a worker processes it.
            let handle: JobHandle = Producer::new(&client, WORKFLOW, VERSION)
                .submit(&key, &json!({"mode": mode}))
                .await?;
            println!("Job {} (created: {})", handle.id, handle.created);

            // JobHandle -> JobOutcome: wait without blocking the runtime's thread.
            match handle
                .wait(&client, Duration::from_millis(timeout_ms))
                .await
            {
                Ok(JobOutcome::Completed) => println!("Job {} completed", handle.id),
                Ok(JobOutcome::Failed { error }) => {
                    println!(
                        "Job {} failed: {}",
                        handle.id,
                        error.as_deref().unwrap_or("unknown reason")
                    );
                }
                Ok(JobOutcome::Cancelled { error }) => {
                    println!(
                        "Job {} cancelled: {}",
                        handle.id,
                        error.as_deref().unwrap_or("unknown reason")
                    );
                }
                Err(error) if error.is::<tokio::time::error::Elapsed>() => {
                    println!(
                        "Stopped waiting after {timeout_ms} ms; job {} has not been cancelled.",
                        handle.id
                    );
                    println!(
                        "Repeat this submit command with the same key and mode to wait again."
                    );
                }
                Err(error) => return Err(error),
            }
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}
