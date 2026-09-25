//! Failure handlers are ordinary runs, created atomically with terminal failure.
use std::time::Duration;

use resume::{Producer, RetryPolicy, Worker};
use serde_json::json;
use tokio::sync::oneshot;
use tokio_postgres::{Client, NoTls};

async fn connect() -> Client {
    let (client, connection) = tokio_postgres::connect(
        &std::env::var("DATABASE_URL").expect("run with just check"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(connection);
    client
}

async fn submit(client: &Client, tag: &str, attempts: i32) -> (i64, String) {
    let workflow = format!("failure-{tag}-{}", std::process::id());
    let run = Producer::new(client, &workflow, "1")
        .retry(RetryPolicy {
            max_attempts: attempts,
            ..RetryPolicy::default()
        })
        .on_failure(format!("{workflow}-handler"), "2")
        .submit("key", &json!({"order": 42}))
        .await
        .unwrap()
        .id;
    (run, workflow)
}

async fn claim(client: &Client, workflow: &str, version: &str) -> (i64, i64) {
    let row = client
        .query_one(
            "select id, attempt from resume.claim_run($1, $2, 60)",
            &[&workflow, &version],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

async fn handlers(client: &Client, run: i64) -> i64 {
    client.query_one("select count(*) from resume.runs where idempotency_key = 'resume:on_failure:' || $1::bigint::text", &[&run])
        .await.unwrap().get(0)
}

pub(super) async fn failure_and_handler_commit_together() {
    let mut client = connect().await;
    let observer = connect().await;
    let (run, workflow) = submit(&client, "atomic", 1).await;
    let (_, attempt) = claim(&client, &workflow, "1").await;
    let tx = client.transaction().await.unwrap();
    tx.execute(
        "select resume.fail_run($1, $2, 'rejected')",
        &[&run, &attempt],
    )
    .await
    .unwrap();
    assert_eq!(handlers(&observer, run).await, 0);
    assert!(
        observer
            .query_one(
                "select failed_at is null from resume.runs where id=$1",
                &[&run]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    tx.rollback().await.unwrap();
    assert_eq!(handlers(&client, run).await, 0);
    client
        .execute(
            "select resume.fail_run($1, $2, 'rejected')",
            &[&run, &attempt],
        )
        .await
        .unwrap();
    assert!(
        client
            .execute(
                "select resume.fail_run($1, $2, 'rejected')",
                &[&run, &attempt]
            )
            .await
            .is_err()
    );
    client
        .execute(
            "update resume.runs set failed_at = failed_at where id=$1",
            &[&run],
        )
        .await
        .unwrap();
    assert_eq!(handlers(&client, run).await, 1);
    let handler = format!("{workflow}-handler");
    assert!(
        client
            .query_opt("select id from resume.claim_run($1, '1', 60)", &[&handler])
            .await
            .unwrap()
            .is_none()
    );
    let (id, _) = claim(&client, &handler, "2").await;
    let row = client
        .query_one(
            "select input, max_attempts from resume.runs where id=$1",
            &[&id],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, serde_json::Value>(0),
        json!({"failed_run":run, "error":"rejected", "input":{"order":42}})
    );
    assert_eq!(row.get::<_, i32>(1), 3);
    assert!(
        client
            .execute("select resume.reopen_run($1)", &[&run])
            .await
            .is_err()
    );
}

pub(super) async fn every_terminal_path_queues_the_handler() {
    let client = connect().await;
    for path in ["cancel", "deadline", "lease"] {
        let (run, workflow) = submit(&client, path, 1).await;
        match path {
            "cancel" => {
                client
                    .execute("select resume.cancel_run($1)", &[&run])
                    .await
                    .unwrap();
            }
            "deadline" => {
                client.execute("update resume.runs set deadline_at=clock_timestamp()-interval '1 second' where id=$1", &[&run]).await.unwrap();
            }
            _ => {
                claim(&client, &workflow, "1").await;
                client.execute("update resume.runs set available_at=clock_timestamp()-interval '1 second' where id=$1", &[&run]).await.unwrap();
            }
        }
        assert!(
            client
                .query_opt("select id from resume.claim_run($1, '1', 60)", &[&workflow])
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(handlers(&client, run).await, 1, "{path} lost the handler");
    }
}

pub(super) async fn success_retry_and_snooze_do_not_queue_handlers() {
    let client = connect().await;
    let (run, workflow) = submit(&client, "success", 3).await;
    let (_, first) = claim(&client, &workflow, "1").await;
    client
        .execute(
            "select resume.retry_run($1, $2, 'temporary')",
            &[&run, &first],
        )
        .await
        .unwrap();
    assert_eq!(handlers(&client, run).await, 0);
    client
        .execute(
            "update resume.runs set available_at=clock_timestamp() where id=$1",
            &[&run],
        )
        .await
        .unwrap();
    let (_, second) = claim(&client, &workflow, "1").await;
    client
        .execute("select resume.release_run($1, $2, 0)", &[&run, &second])
        .await
        .unwrap();
    assert_eq!(handlers(&client, run).await, 0);
    let (_, third) = claim(&client, &workflow, "1").await;
    client
        .execute("select resume.complete_run($1, $2)", &[&run, &third])
        .await
        .unwrap();
    assert_eq!(handlers(&client, run).await, 0);
}

pub(super) async fn failed_handler_reopens_with_saved_progress() {
    let client = connect().await;
    let (run, workflow) = submit(&client, "reopen", 1).await;
    client
        .execute("select resume.cancel_run($1)", &[&run])
        .await
        .unwrap();
    let handler = format!("{workflow}-handler");
    let (id, attempt) = claim(&client, &handler, "2").await;
    client
        .execute(
            "select resume.begin_step($1, $2, 'release', 0, 60)",
            &[&id, &attempt],
        )
        .await
        .unwrap();
    client
        .execute(
            "select resume.save_step($1, $2, 'release', 'true', null)",
            &[&id, &attempt],
        )
        .await
        .unwrap();
    client
        .execute(
            "select resume.fail_run($1, $2, 'refund unavailable')",
            &[&id, &attempt],
        )
        .await
        .unwrap();
    assert_eq!(handlers(&client, id).await, 0);
    client
        .execute("select resume.reopen_run($1)", &[&id])
        .await
        .unwrap();
    let (_, next) = claim(&client, &handler, "2").await;
    let saved = client
        .query_one(
            "select output from resume.begin_step($1, $2, 'release', 0, 60)",
            &[&id, &next],
        )
        .await
        .unwrap();
    assert_eq!(saved.get::<_, serde_json::Value>(0), json!(true));
    client
        .execute("select resume.complete_run($1, $2)", &[&id, &next])
        .await
        .unwrap();
    assert_eq!(handlers(&client, run).await, 1);
    assert!(
        client
            .execute("select resume.reopen_run($1)", &[&run])
            .await
            .is_err()
    );
}

pub(super) async fn worker_exhaustion_queues_handler_without_step_output() {
    let client = connect().await;
    let worker_client = connect().await;
    let vendor = connect().await;
    vendor
        .batch_execute("create table resume.check_vendor_effects (run_id bigint primary key)")
        .await
        .unwrap();
    let (id, workflow) = submit(&client, "worker", 1).await;
    let (stop, shutdown) = oneshot::channel();
    let worker = Worker::new(worker_client, &workflow, "1")
        .poll_interval(Duration::from_millis(10))
        .run(
            async {
                let _ = shutdown.await;
            },
            async |client, run| {
                run.step(client, "charge", async |_| {
                    vendor
                        .execute(
                            "insert into resume.check_vendor_effects values ($1)",
                            &[&run.id],
                        )
                        .await?;
                    Err("vendor committed, but its response was lost".into())
                })
                .await?;
                Ok(())
            },
        );
    let observer = async {
        tokio::time::timeout(Duration::from_secs(10), async {
            while handlers(&client, id).await == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(handlers(&client, id).await, 1);
        assert_eq!(
            client
                .query_one("select count(*) from resume.steps where run_id=$1", &[&id])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0
        );
        assert_eq!(
            client
                .query_one(
                    "select count(*) from resume.check_vendor_effects where run_id=$1",
                    &[&id]
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        stop.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(worker, observer);
    result.unwrap();
}
