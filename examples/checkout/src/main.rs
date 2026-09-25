mod mock_vendor;

use std::time::Duration;

use mock_vendor::MockVendor;
use resume::{Job, Permanent, Producer, Result, Worker, shutdown_signal};
use serde_json::json;
use tokio::sync::watch;
use tokio::task::JoinSet;

const VERSION: &str = "1";
const WORKFLOWS: [&str; 2] = ["checkout", "checkout_failure"];

async fn checkout(run: &Job, vendor: &mut MockVendor) -> Result<()> {
    let order = run.input["order"].as_str().ok_or("missing order")?;
    run.step("charge", async |_| {
        vendor.apply(order, "payment", "charge").await?;
        Ok(json!({"charged": true}))
    })
    .await?;
    run.step("reserve", async |_| {
        vendor.apply(order, "reservation", "reserve").await?;
        Ok(json!({"reserved": true}))
    })
    .await?;

    run.step("ship", async |_| {
        Err(Permanent("shipping rejected the order".into()).into())
    })
    .await?;
    Ok(())
}

async fn undo(run: &Job, vendor: &mut MockVendor, refund: bool) -> Result<()> {
    let args = &run.input["input"];
    let order = args["order"].as_str().ok_or("missing original order")?;
    let (kind, action) = if refund {
        ("payment", "refund")
    } else {
        ("reservation", "release")
    };
    run.step(action, async |_| {
        vendor.undo(order, kind, action).await?;
        if refund && args["crash_refund"] == true && vendor.first_refund_crash(order).await? {
            // The vendor committed the refund, but the step has not saved its result yet.
            tracing::warn!(
                order,
                "simulated crash after refund; restart checkout-process"
            );
            std::process::exit(99);
        }
        Ok(json!({"undone": true}))
    })
    .await?;
    Ok(())
}

async fn notify_failure(run: &Job) -> Result<()> {
    let failed_run = run.input["failed_run"]
        .as_i64()
        .ok_or("missing failed run")?;
    let order = run.input["input"]["order"]
        .as_str()
        .ok_or("missing original order")?;
    let reason = run.input["error"]
        .as_str()
        .ok_or("missing failure reason")?;
    run.step("notify", async |tx| {
        let unfinished: bool = tx.query_one(
            "select exists (select 1 from checkout.effects where order_key = $1 and not undone)",
            &[&order],
        ).await?.try_get(0)?;
        if unfinished { return Err(Permanent("cleanup is not finished".into()).into()); }
        tx.execute(
            "insert into checkout.notifications (failed_run, order_key, reason) values ($1, $2, $3)",
            &[&failed_run, &order, &reason],
        ).await?;
        tx.execute("insert into checkout.events (order_key, action) values ($1, 'notify')", &[&order]).await?;
        Ok(json!({"notified": true}))
    }).await?;
    tracing::info!(
        order,
        "cleanup finished; failure recorded in the local notifications table"
    );
    Ok(())
}

async fn work(url: &str) -> Result<()> {
    // The failure handler is an ordinary workflow with its own saved steps and retries.
    let (stop, stopping) = watch::channel(false);
    let mut workers = JoinSet::new();
    for workflow in WORKFLOWS {
        let client = harness::connect(url).await?;
        let mut vendor = MockVendor::new(harness::connect(url).await?);
        let mut stopping = stopping.clone();
        workers.spawn_local(async move {
            Worker::new(client, workflow, VERSION)
                .lease(Duration::from_secs(3))
                .step_timeout(Duration::from_secs(2))
                .poll_interval(Duration::from_millis(100))
                .run(
                    async move {
                        while !*stopping.borrow() {
                            if stopping.changed().await.is_err() {
                                break;
                            }
                        }
                    },
                    async |run| match workflow {
                        "checkout" => checkout(run, &mut vendor).await,
                        _ => {
                            undo(run, &mut vendor, false).await?;
                            undo(run, &mut vendor, true).await?;
                            notify_failure(run).await
                        }
                    },
                )
                .await
        });
    }
    let first = tokio::select! {
        () = shutdown_signal() => None,
        result = workers.join_next() => result,
    };
    stop.send(true)?;
    let mut results = Vec::new();
    if let Some(result) = first {
        results.push(result);
    }
    while let Some(result) = workers.join_next().await {
        results.push(result);
    }
    for result in results {
        result??;
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let url = harness::init()?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("submit") => {
            let key = args
                .next()
                .ok_or("expected submit <order> [fail|crash-refund]")?;
            let mode = args.next().unwrap_or_else(|| "fail".into());
            if !matches!(mode.as_str(), "fail" | "crash-refund") {
                return Err("mode must be fail or crash-refund".into());
            }
            let client = harness::connect(&url).await?;
            let submitted = Producer::new(&client, "checkout", VERSION)
                .on_failure("checkout_failure", VERSION)
                .submit(
                    &key,
                    &json!({"order": key, "crash_refund": mode == "crash-refund"}),
                )
                .await?;
            tracing::info!(
                id = submitted.id,
                created = submitted.created,
                "checkout submitted"
            );
            Ok(())
        }
        Some("work") | None => tokio::task::LocalSet::new().run_until(work(&url)).await,
        _ => Err("expected submit <order> [fail|crash-refund] or work".into()),
    }
}
