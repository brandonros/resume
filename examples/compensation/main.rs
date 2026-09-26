//! A sequential saga with application-defined compensation, not a distributed transaction.
//! Install schema.sql in DATABASE_URL. PAYMENT_DATABASE_URL may point to a separate database.
//! cargo run --example compensation -- init
//! cargo run --example compensation -- submit order-1 2 500
//! cargo run --example compensation -- run              # charge succeeds; response lost; pauses
//! cargo run --example compensation -- status order-1
//! cargo run --example compensation -- reconcile order-1 # verify provider receipt; stays paused
//! cargo run --example compensation -- resume order-1
//! cargo run --example compensation -- run              # shipment rejected; refund response lost
//! cargo run --example compensation -- run              # same refund key; release inventory
//! The fake provider loses the first response to each charge/refund. No real money moves.
use std::time::Duration;

use resume::{Error, Job, Result, Retry, SagaOutcome, Steps, run_one, submit};
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls};

const WORKFLOW: &str = "compensation-app:order:v1";

#[derive(Debug)]
enum ResponseLost {
    Charge,
    Refund,
}
impl std::fmt::Display for ResponseLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} response lost", self)
    }
}
impl std::error::Error for ResponseLost {}

async fn connect(url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("database connection: {error}");
        }
    });
    Ok(client)
}

async fn connect_provider() -> Result<Client> {
    let url = std::env::var("PAYMENT_DATABASE_URL").or_else(|_| std::env::var("DATABASE_URL"))?;
    connect(&url).await
}

// A provider must implement deduplication itself; these keys are not distributed locks.
fn charge_key(order: &str) -> String {
    format!("resume-compensation:v1:{order}:charge")
}
fn refund_key(order: &str) -> String {
    format!("resume-compensation:v1:{order}:refund")
}

async fn charge(provider: &Client, key: &str, amount: i64) -> Result<Value> {
    let row = provider
        .query_opt(
            "insert into payment_provider.charges (key, amount) values ($1, $2)
        on conflict (key) do update set calls = charges.calls + 1
        where charges.amount = excluded.amount returning calls",
            &[&key, &amount],
        )
        .await?
        .ok_or("charge key reused with different amount")?;
    if row.get::<_, i64>(0) == 1 {
        return Err(ResponseLost::Charge.into());
    }
    Ok(json!({"charge_key": key, "amount": amount}))
}

async fn refund(provider: &Client, key: &str, payment: &Value) -> Result<Value> {
    let charge = payment["charge_key"]
        .as_str()
        .ok_or("missing charge receipt")?;
    let amount = payment["amount"].as_i64().ok_or("missing charge amount")?;
    let row = provider
        .query_opt(
            "insert into payment_provider.refunds (key, charge_key, amount)
        select $1, key, amount from payment_provider.charges where key = $2 and amount = $3
        on conflict (key) do update set calls = refunds.calls + 1
        where refunds.charge_key = excluded.charge_key and refunds.amount = excluded.amount
        returning calls",
            &[&key, &charge, &amount],
        )
        .await?
        .ok_or("refund parameters do not match the verified charge")?;
    if row.get::<_, i64>(0) == 1 {
        return Err(ResponseLost::Refund.into());
    }
    Ok(json!({"refund_key": key, "charge_key": charge, "amount": amount}))
}

fn retry(job: &Job, error: &Error) -> Retry {
    // Refund retries are safe because the provider enforces the same key+parameters.
    // An uncertain step_once charge requires reconciliation, even though it also has a key.
    if matches!(
        error.downcast_ref::<ResponseLost>(),
        Some(ResponseLost::Refund)
    ) && job.failures < 3
    {
        Retry::After(Duration::ZERO)
    } else {
        Retry::Stop
    }
}

async fn fulfill(job: &Job, steps: &mut Steps<'_>, provider: &Client) -> Result<Value> {
    let quantity = job.input["quantity"]
        .as_i64()
        .filter(|n| *n > 0)
        .ok_or("invalid quantity")?;
    let amount = job.input["amount"]
        .as_i64()
        .filter(|n| *n > 0)
        .ok_or("invalid amount")?;
    let outcome = steps.saga(
        async |saga| {
            saga.step("reserve", "release-inventory", async |tx| {
                let changed = tx.execute("update compensation_app.inventory set available = available - $1
                    where sku = 'book' and available >= $1", &[&quantity]).await?;
                if changed != 1 { return Err("insufficient stock".into()); }
                tx.execute("insert into compensation_app.orders (job_id, quantity, state) values ($1, $2, 'reserved')",
                    &[&job.id, &quantity]).await?;
                Ok(json!({"reservation_id": job.id, "quantity": quantity}))
            }).await?;
            saga.step_once("charge", "refund", async || charge(provider, &charge_key(&job.key), amount).await).await?;
            let shipping = saga.checkpoint("shipment", async |_| {
                // A confirmed rejection, not an unknown external outcome.
                Ok(json!({"accepted": false, "reason": "destination unsupported"}))
            }).await?;
            if shipping["accepted"] == json!(false) {
                saga.compensate(json!({"reason": shipping["reason"]})).await?;
            }
            Ok(shipping)
        },
        async |name, receipt, tx| {
            match name {
                "refund" => refund(provider, &refund_key(&job.key), receipt).await,
                "release-inventory" => {
                    let id = receipt["reservation_id"].as_i64().ok_or("invalid reservation")?;
                    let row = tx.query_one("update compensation_app.orders set released = true, state = 'compensated'
                        where job_id = $1 and not released returning quantity", &[&id]).await?;
                    let quantity: i64 = row.get(0);
                    tx.execute("update compensation_app.inventory set available = available + $1 where sku = 'book'", &[&quantity]).await?;
                    Ok(json!({"released": id}))
                }
                _ => Err(format!("unknown compensation: {name}").into()),
            }
        },
    ).await?;
    Ok(match outcome {
        SagaOutcome::Completed(shipping) => json!({"state": "shipped", "shipping": shipping}),
        SagaOutcome::Compensated(reason) => {
            json!({"state": "compensated", "reason": reason["reason"]})
        }
    })
}

async fn initialize(client: &Client, provider: &Client) -> Result<()> {
    client.batch_execute(include_str!("schema.sql")).await?;
    client
        .execute(
            "insert into compensation_app.inventory values ('book', 10) on conflict do nothing",
            &[],
        )
        .await?;
    provider.batch_execute(include_str!("provider.sql")).await?;
    Ok(())
}

async fn enqueue(client: &Client, key: &str, quantity: i64, amount: i64) -> Result<i64> {
    if quantity <= 0 || amount <= 0 {
        return Err("quantity and amount in cents must be positive".into());
    }
    submit(
        client,
        WORKFLOW,
        key,
        &json!({"quantity": quantity, "amount": amount}),
    )
    .await
}

async fn reconcile(client: &Client, provider: &Client, key: &str) -> Result<()> {
    let job = client
        .query_one(
            "select id, input from resume.jobs where workflow = $1 and key = $2",
            &[&WORKFLOW, &key],
        )
        .await?;
    let id: i64 = job.get(0);
    let input: Value = job.get(1);
    let expected = input["amount"].as_i64().ok_or("invalid amount")?;
    let payment_key = charge_key(key);
    // Absence does not prove failure: a real provider might still be processing the request.
    let row = provider
        .query_opt(
            "select amount from payment_provider.charges where key = $1",
            &[&payment_key],
        )
        .await?
        .ok_or("charge outcome still unknown; do not refund or release inventory")?;
    let amount: i64 = row.get(0);
    if amount != expected {
        return Err("provider receipt does not match order".into());
    }
    let verified = json!({"charge_key": payment_key, "amount": amount});
    client
        .execute(
            "select resume.resolve_step($1, 'charge', $2)",
            &[&id, &verified],
        )
        .await?;
    Ok(())
}

async fn status(client: &Client, key: &str) -> Result<Value> {
    Ok(client.query_one("select jsonb_build_object('job', j.id, 'job_status', j.status,
        'state', case when j.saga_phase = 'compensating' then 'compensating' else o.state end,
        'released', o.released, 'reason', j.saga_result -> 'reason', 'error', j.last_error,
        'failures', j.failures, 'available', (select available from compensation_app.inventory where sku = 'book'))
        from resume.job_status j left join compensation_app.orders o on o.job_id = j.id
        where j.workflow = $1 and j.key = $2", &[&WORKFLOW, &key]).await?.get(0))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let url = std::env::var("DATABASE_URL")?;
    let mut client = connect(&url).await?;
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["init"] => initialize(&client, &connect_provider().await?).await?,
        ["submit", key, quantity, amount] => println!("job {}", enqueue(&client, key, quantity.parse()?, amount.parse()?).await?),
        ["run"] => { let provider = connect_provider().await?; println!("processed: {}", run_one(&mut client, WORKFLOW, async |job, steps| fulfill(job, steps, &provider).await, retry).await?); }
        ["reconcile", key] => { reconcile(&client, &connect_provider().await?, key).await?; println!("verified receipt saved; job remains paused"); }
        ["resume", key] => { client.query_one("select resume.requeue(id) from resume.jobs where workflow = $1 and key = $2", &[&WORKFLOW, &key]).await?; }
        ["status", key] => println!("{}", serde_json::to_string_pretty(&status(&client, key).await?)?),
        _ => return Err("usage: compensation init | submit KEY QUANTITY CENTS | run | reconcile KEY | resume KEY | status KEY".into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires disposable DATABASE_URL and optionally PAYMENT_DATABASE_URL"]
    async fn unknown_charge_and_interrupted_compensation_recover_without_duplicate_effects()
    -> Result<()> {
        let url = std::env::var("DATABASE_URL")?;
        let mut worker = connect(&url).await?;
        let observer = connect(&url).await?;
        let provider =
            connect(&std::env::var("PAYMENT_DATABASE_URL").unwrap_or(url.clone())).await?;
        initialize(&worker, &provider).await?;

        // A missing provider record is not proof of failure and must not resolve a marker.
        let missing = enqueue(&worker, "missing-proof", 1, 100).await?;
        assert!(
            run_one(
                &mut worker,
                WORKFLOW,
                async |_, steps| {
                    steps
                        .step_once("charge", async || Err(ResponseLost::Charge.into()))
                        .await
                },
                retry
            )
            .await
            .is_err()
        );
        assert!(
            reconcile(&observer, &provider, "missing-proof")
                .await
                .is_err()
        );
        assert!(
            observer
                .query_one(
                    "select output is null from resume.steps where job_id = $1 and key = 'charge'",
                    &[&missing]
                )
                .await?
                .get::<_, bool>(0)
        );

        let key = "test-order";
        let id = enqueue(&worker, key, 2, 500).await?;
        assert_eq!(id, enqueue(&worker, key, 2, 500).await?);
        let handler = async |job: &Job, steps: &mut Steps<'_>| fulfill(job, steps, &provider).await;
        let error = run_one(&mut worker, WORKFLOW, &handler, retry)
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ResponseLost>(),
            Some(ResponseLost::Charge)
        ));
        let state = status(&observer, key).await?;
        assert_eq!(state["state"], "reserved");
        assert_eq!(state["job_status"], "paused");
        assert_eq!(state["available"], 8);
        assert_eq!(state["released"], false);
        assert_eq!(
            provider
                .query_one("select count(*) from payment_provider.refunds", &[])
                .await?
                .get::<_, i64>(0),
            0
        );
        assert!(
            observer
                .execute("select resume.requeue($1)", &[&id])
                .await
                .is_err()
        );
        assert!(charge(&provider, &charge_key(key), 999).await.is_err());
        reconcile(&observer, &provider, key).await?;
        assert!(!run_one(&mut worker, WORKFLOW, &handler, retry).await?);
        assert_eq!(status(&observer, key).await?["job_status"], "paused");
        observer
            .execute("select resume.requeue($1)", &[&id])
            .await?;

        let error = run_one(&mut worker, WORKFLOW, &handler, retry)
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ResponseLost>(),
            Some(ResponseLost::Refund)
        ));
        let state = status(&observer, key).await?;
        assert_eq!(state["state"], "compensating");
        assert_eq!(state["reason"], "destination unsupported");
        assert_eq!(state["released"], false);
        assert_eq!(state["available"], 8);
        assert_eq!(state["job_status"], "ready");
        assert!(
            observer
                .query_opt(
                    "select output from resume.steps where job_id = $1 and key = '$undo:1:refund'",
                    &[&id]
                )
                .await?
                .is_none()
        );
        let wrong = json!({"charge_key": charge_key(key), "amount": 999});
        assert!(refund(&provider, &refund_key(key), &wrong).await.is_err());
        assert_eq!(
            provider
                .query_one(
                    "select calls from payment_provider.refunds where key = $1",
                    &[&refund_key(key)]
                )
                .await?
                .get::<_, i64>(0),
            1
        );

        // Block inventory release, allowing the idempotent refund retry and its checkpoint
        // to commit first. Then kill the worker: the release transaction must roll back.
        let pid: i32 = worker
            .query_one("select pg_backend_pid()", &[])
            .await?
            .get(0);
        let mut locker = connect(&url).await?;
        let lock = locker.transaction().await?;
        lock.query_one(
            "select sku from compensation_app.inventory where sku = 'book' for update",
            &[],
        )
        .await?;
        let run = run_one(&mut worker, WORKFLOW, &handler, retry);
        let interrupt = async {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if observer
                        .query_one("select cardinality(pg_blocking_pids($1)) > 0", &[&pid])
                        .await?
                        .get::<_, bool>(0)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, Error>(())
            })
            .await??;
            assert!(observer.query_one("select output is not null from resume.steps where job_id = $1 and key = '$undo:1:refund'", &[&id]).await?.get::<_, bool>(0));
            assert!(
                observer
                    .query_one("select pg_terminate_backend($1, 5000)", &[&pid])
                    .await?
                    .get::<_, bool>(0)
            );
            lock.rollback().await?;
            Ok::<_, Error>(())
        };
        let (result, interrupted) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(run, interrupt)
        })
        .await?;
        interrupted?;
        assert!(result.is_err());
        let state = status(&observer, key).await?;
        assert_eq!(state["released"], false);
        assert_eq!(state["available"], 8);
        // Simulate lease expiry so this test does not wait a full minute.
        observer.execute("update resume.jobs set available_at = clock_timestamp() - interval '1 second' where id = $1", &[&id]).await?;
        let mut replacement = connect(&url).await?;
        assert!(run_one(&mut replacement, WORKFLOW, &handler, retry).await?);
        let state = status(&observer, key).await?;
        assert_eq!(state["state"], "compensated");
        assert_eq!(state["job_status"], "completed");
        assert_eq!(state["released"], true);
        assert_eq!(state["available"], 10);
        // One charge invocation, two refund requests, one refund effect. Recovery after the
        // saved refund must not call the provider a third time or release inventory twice.
        assert_eq!(
            provider
                .query_one(
                    "select calls from payment_provider.charges where key = $1",
                    &[&charge_key(key)]
                )
                .await?
                .get::<_, i64>(0),
            1
        );
        assert_eq!(
            provider
                .query_one(
                    "select calls from payment_provider.refunds where key = $1",
                    &[&refund_key(key)]
                )
                .await?
                .get::<_, i64>(0),
            2
        );
        assert_eq!(
            provider
                .query_one("select count(*) from payment_provider.refunds", &[])
                .await?
                .get::<_, i64>(0),
            1
        );
        assert_eq!(id, enqueue(&replacement, key, 2, 500).await?);
        assert!(!run_one(&mut replacement, WORKFLOW, &handler, retry).await?);
        assert_eq!(status(&observer, key).await?["available"], 10);
        Ok(())
    }
}
