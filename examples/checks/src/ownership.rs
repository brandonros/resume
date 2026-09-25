//! Ownership rules of the SQL functions: which attempt may start steps, save them, and record
//! how it ended. Needs a database with the resume schema in DATABASE_URL; `just check` creates one.

use std::time::Duration;

use resume::Worker;
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tokio_postgres::Client;

use crate::common::{
    claim, claim_with, complete, connect, end_attempt, run_is, submit, wait_for, workflow,
};

/// Submits a run to a workflow of its own and claims it with this lease. Returns the run's ID,
/// the attempt, and the workflow.
async fn claimed(client: &Client, lease_seconds: f64) -> (i64, i64, String) {
    let workflow = workflow("ownership");
    submit(client, &workflow).await;
    let (run, attempt) = claim_with(client, &workflow, "1", lease_seconds)
        .await
        .unwrap();
    (run, attempt, workflow)
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

pub(super) async fn new_claim_rejects_previous_attempt() {
    let client = connect().await;
    let (run, first, workflow) = claimed(&client, 0.2).await;
    sleep(0.3).await;
    let (_, second) = claim(&client, &workflow).await.unwrap();
    assert_eq!(second, first + 1);
    assert!(
        complete(&client, run, first).await.is_err(),
        "the replaced attempt completed the run"
    );
}

pub(super) async fn attempt_that_handed_back_the_run_cannot_complete_it() {
    let client = connect().await;
    let (run, attempt, _) = claimed(&client, 30.0).await;
    end_attempt(&client, run, attempt, "boom", false)
        .await
        .unwrap();
    assert!(
        complete(&client, run, attempt).await.is_err(),
        "completed a run it had handed back"
    );
}

pub(super) async fn failed_step_keeps_ownership_to_record_the_failure() {
    let mut client = connect().await;
    let (run, attempt, _) = claimed(&client, 1.0).await;
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

    end_attempt(&client, run, attempt, "permanent", true)
        .await
        .expect("could not record the failure");
}

pub(super) async fn step_saves_after_outlasting_its_lease() {
    let mut client = connect().await;
    let (run, attempt, _) = claimed(&client, 1.0).await;

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

pub(super) async fn expired_lease_cannot_start_a_step() {
    let client = connect().await;
    let (run, attempt, _) = claimed(&client, 0.2).await;
    sleep(0.4).await;

    let late = client
        .query_one(
            "select output from resume.begin_step($1, $2, 'late', 0, 1)",
            &[&run, &attempt],
        )
        .await;
    assert!(late.is_err(), "started a step after the lease expired");
}

pub(super) async fn completed_step_replays_its_output() {
    let client = connect().await;
    let (run, attempt, _) = claimed(&client, 30.0).await;
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

pub(super) async fn passing_the_deadline_fails_the_run_without_starting_the_step() {
    let client = connect().await;
    let (run, attempt, _) = claimed(&client, 30.0).await;
    client
        .execute(
            "update resume.runs set deadline_at = clock_timestamp() where id = $1",
            &[&run],
        )
        .await
        .unwrap();

    let failed: Option<String> = client
        .query_one(
            "select failed from resume.begin_step($1, $2, 'late', 0, 30)",
            &[&run, &attempt],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(failed.as_deref(), Some("the run passed its deadline"));
    let row = client
        .query_one(
            "select needs_resolution, status from resume.run_status where id = $1",
            &[&run],
        )
        .await
        .unwrap();
    assert!(
        !row.get::<_, bool>(0),
        "a step that never ran needs resolution"
    );
    assert_eq!(row.get::<_, &str>(1), "failed");
}

pub(super) async fn a_step_key_used_twice_fails_the_run() {
    let client = connect().await;
    let name = workflow("duplicate-key");
    let run = submit(&client, &name).await;
    let (stop, shutdown) = oneshot::channel::<()>();
    let worker = Worker::new(connect().await, &name, "1")
        .poll_interval(Duration::from_millis(10))
        .run(
            async {
                let _ = shutdown.await;
            },
            async |run| {
                for _ in 0..2 {
                    run.step("same", async |_| Ok(json!(1))).await?;
                }
                Ok(())
            },
        );
    let observer = async {
        wait_for(&client, run, "failed_at is not null").await;
        assert!(
            run_is(
                &client,
                run,
                "attempt = 1 and last_error like '%used twice%'"
            )
            .await
        );
        stop.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(worker, observer);
    result.unwrap();
}
