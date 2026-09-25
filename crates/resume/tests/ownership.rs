//! Ownership rules of the SQL functions: which attempt may start steps, save them, and record
//! how it ended. Needs a database with the resume schema in DATABASE_URL; `just test` creates one.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls};

async fn connect() -> Client {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL; run the tests with `just test`");
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

/// Submits a run to a workflow of its own and claims it. Returns the run's ID, the attempt, and
/// the workflow.
async fn claim(client: &Client, lease_seconds: f64) -> (i64, i64, String) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let workflow = format!("test-{}-{n}", std::process::id());
    client
        .execute(
            "select resume.submit_run($1, '1', 'key', '{}', 3)",
            &[&workflow],
        )
        .await
        .unwrap();
    let row = client
        .query_one(
            "select id, attempt from resume.claim_run($1, '1', $2)",
            &[&workflow, &lease_seconds],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1), workflow)
}

async fn begin_step(client: &Client, run: i64, attempt: i64, key: &str) -> Option<Value> {
    client
        .query_one(
            "select output from resume.begin_step($1, $2, $3, 0, 1)",
            &[&run, &attempt, &key],
        )
        .await
        .unwrap()
        .get(0)
}

async fn sleep(seconds: f64) {
    tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
}

#[tokio::test]
async fn new_claim_rejects_previous_attempt() {
    let client = connect().await;
    let (run, first, workflow) = claim(&client, 0.2).await;
    sleep(0.3).await;
    let second: i64 = client
        .query_one(
            "select attempt from resume.claim_run($1, '1', 30)",
            &[&workflow],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(second, first + 1);

    let old = client
        .execute("select resume.complete_run($1, $2)", &[&run, &first])
        .await;
    assert!(old.is_err(), "the replaced attempt completed the run");
}

#[tokio::test]
async fn attempt_that_handed_back_the_run_cannot_complete_it() {
    let client = connect().await;
    let (run, attempt, _) = claim(&client, 30.0).await;
    client
        .execute("select resume.retry_run($1, $2, 'boom')", &[&run, &attempt])
        .await
        .unwrap();

    let after_retry = client
        .execute("select resume.complete_run($1, $2)", &[&run, &attempt])
        .await;
    assert!(after_retry.is_err(), "completed a run it had handed back");
}

#[tokio::test]
async fn failed_step_keeps_ownership_to_record_the_failure() {
    let mut client = connect().await;
    let (run, attempt, _) = claim(&client, 1.0).await;
    sleep(0.5).await;

    // The step renews the lease, then fails and rolls back, restoring the older lease, which
    // expires while the step is still running.
    let tx = client.transaction().await.unwrap();
    tx.query_one(
        "select output from resume.begin_step($1, $2, 'step', 0, 2)",
        &[&run, &attempt],
    )
    .await
    .unwrap();
    sleep(0.7).await;
    tx.rollback().await.unwrap();

    client
        .execute(
            "select resume.fail_run($1, $2, 'permanent')",
            &[&run, &attempt],
        )
        .await
        .expect("could not record the failure");
}

#[tokio::test]
async fn step_saves_after_outlasting_its_lease() {
    let mut client = connect().await;
    let (run, attempt, _) = claim(&client, 1.0).await;

    // The step holds the run's lock the whole time, so no one else can claim it, even after
    // the lease runs out, as when waiting for another run's subject lock.
    let tx = client.transaction().await.unwrap();
    tx.query_one(
        "select output from resume.begin_step($1, $2, 'slow', 0, 1)",
        &[&run, &attempt],
    )
    .await
    .unwrap();
    sleep(1.2).await;
    tx.execute(
        "select resume.save_step($1, $2, 'slow', '42')",
        &[&run, &attempt],
    )
    .await
    .expect("could not save a step that outlasted its lease");
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn expired_lease_cannot_start_a_step() {
    let client = connect().await;
    let (run, attempt, _) = claim(&client, 0.2).await;
    sleep(0.4).await;

    let late = client
        .query_one(
            "select output from resume.begin_step($1, $2, 'late', 0, 1)",
            &[&run, &attempt],
        )
        .await;
    assert!(late.is_err(), "started a step after the lease expired");
}

#[tokio::test]
async fn completed_step_replays_its_output() {
    let client = connect().await;
    let (run, attempt, _) = claim(&client, 30.0).await;
    assert_eq!(begin_step(&client, run, attempt, "once").await, None);
    client
        .execute(
            "select resume.save_step($1, $2, 'once', '{\"n\": 1}')",
            &[&run, &attempt],
        )
        .await
        .unwrap();

    assert_eq!(
        begin_step(&client, run, attempt, "once").await,
        Some(json!({"n": 1}))
    );
}
