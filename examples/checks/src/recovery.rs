//! Failure handlers are ordinary runs, created atomically with terminal failure.
use std::time::Duration;

use resume::{Producer, RetryPolicy, Worker};
use serde_json::json;
use tokio::sync::oneshot;
use tokio_postgres::Client;

use crate::common::{
    claim, claim_with, complete, connect, end_attempt, make_due, pass_deadline, run_is, workflow,
};

/// Submits a run with this many attempts and a failure handler, `<workflow>-handler` version 2.
/// Returns the run's ID and its workflow.
async fn submit(client: &Client, tag: &str, attempts: i32) -> (i64, String) {
    let workflow = workflow(&format!("failure-{tag}"));
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

/// Claims the next run of the workflow's handler.
async fn claim_handler(client: &Client, workflow: &str) -> (i64, i64) {
    claim_with(client, &format!("{workflow}-handler"), "2", 60.0)
        .await
        .unwrap()
}

async fn handlers(client: &Client, run: i64) -> i64 {
    client
        .query_one(
            "select count(*) from resume.runs
             where idempotency_key = 'resume:on_failure:' || $1::bigint::text",
            &[&run],
        )
        .await
        .unwrap()
        .get(0)
}

async fn cancel(client: &Client, run: i64) {
    client
        .execute("select resume.cancel_run($1)", &[&run])
        .await
        .unwrap();
}

async fn reopen(client: &Client, run: i64) -> Result<u64, tokio_postgres::Error> {
    client
        .execute("select resume.reopen_run($1)", &[&run])
        .await
}

pub(super) async fn failure_and_handler_commit_together() {
    let mut client = connect().await;
    let observer = connect().await;
    let (run, workflow) = submit(&client, "atomic", 1).await;
    let (_, attempt) = claim(&client, &workflow).await.unwrap();
    let tx = client.transaction().await.unwrap();
    tx.execute(
        "select resume.end_attempt($1, $2, 'rejected', true)",
        &[&run, &attempt],
    )
    .await
    .unwrap();
    assert_eq!(handlers(&observer, run).await, 0);
    assert!(run_is(&observer, run, "failed_at is null").await);
    tx.rollback().await.unwrap();
    assert_eq!(handlers(&client, run).await, 0);

    end_attempt(&client, run, attempt, "rejected", true)
        .await
        .unwrap();
    assert!(
        end_attempt(&client, run, attempt, "rejected", true)
            .await
            .is_err()
    );
    client
        .execute(
            "update resume.runs set failed_at = failed_at where id = $1",
            &[&run],
        )
        .await
        .unwrap();
    assert_eq!(handlers(&client, run).await, 1);

    // Only workers of the handler's version claim it.
    assert!(
        claim(&client, &format!("{workflow}-handler"))
            .await
            .is_none()
    );
    let (id, _) = claim_handler(&client, &workflow).await;
    let row = client
        .query_one(
            "select input, max_attempts from resume.runs where id = $1",
            &[&id],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, serde_json::Value>(0),
        json!({"failed_run": run, "error": "rejected", "input": {"order": 42}})
    );
    assert_eq!(row.get::<_, i32>(1), 3);
    assert!(reopen(&client, run).await.is_err());
}

pub(super) async fn every_terminal_path_queues_the_handler() {
    let client = connect().await;
    for path in ["cancel", "deadline", "lease"] {
        let (run, workflow) = submit(&client, path, 1).await;
        match path {
            "cancel" => cancel(&client, run).await,
            "deadline" => pass_deadline(&client, run).await,
            _ => {
                claim(&client, &workflow).await.unwrap();
                make_due(&client, run).await;
            }
        }
        assert!(claim(&client, &workflow).await.is_none());
        assert_eq!(handlers(&client, run).await, 1, "{path} lost the handler");
        let status: String = client
            .query_one(
                "select status from resume.run_status where id = $1",
                &[&run],
            )
            .await
            .unwrap()
            .get(0);
        let expected = if path == "cancel" {
            "cancelled"
        } else {
            "failed"
        };
        assert_eq!(status, expected, "{path}");
    }
}

pub(super) async fn success_retry_and_snooze_do_not_queue_handlers() {
    let client = connect().await;
    let (run, workflow) = submit(&client, "success", 3).await;
    let (_, first) = claim(&client, &workflow).await.unwrap();
    end_attempt(&client, run, first, "temporary", false)
        .await
        .unwrap();
    assert_eq!(handlers(&client, run).await, 0);
    make_due(&client, run).await;
    let (_, second) = claim(&client, &workflow).await.unwrap();
    client
        .execute("select resume.release_run($1, $2, 0)", &[&run, &second])
        .await
        .unwrap();
    assert_eq!(handlers(&client, run).await, 0);
    let (_, third) = claim(&client, &workflow).await.unwrap();
    complete(&client, run, third).await.unwrap();
    assert_eq!(handlers(&client, run).await, 0);
}

pub(super) async fn failed_handler_reopens_with_saved_progress() {
    let client = connect().await;
    let (run, workflow) = submit(&client, "reopen", 1).await;
    cancel(&client, run).await;
    let (id, attempt) = claim_handler(&client, &workflow).await;
    client
        .execute(
            "select resume.begin_step($1, $2, 'release', 0, 60)",
            &[&id, &attempt],
        )
        .await
        .unwrap();
    client
        .execute(
            "select resume.save_step($1, $2, 'release', 'true')",
            &[&id, &attempt],
        )
        .await
        .unwrap();
    end_attempt(&client, id, attempt, "refund unavailable", true)
        .await
        .unwrap();
    assert_eq!(handlers(&client, id).await, 0);
    reopen(&client, id).await.unwrap();

    let (_, next) = claim_handler(&client, &workflow).await;
    let saved = client
        .query_one(
            "select output from resume.begin_step($1, $2, 'release', 0, 60)",
            &[&id, &next],
        )
        .await
        .unwrap();
    assert_eq!(saved.get::<_, serde_json::Value>(0), json!(true));
    complete(&client, id, next).await.unwrap();
    assert_eq!(handlers(&client, run).await, 1);
    assert!(reopen(&client, run).await.is_err());
}

pub(super) async fn reserved_handler_keys_cannot_be_submitted() {
    let client = connect().await;
    let (run, workflow) = submit(&client, "reserved", 1).await;
    // Taking the key the handler will use would make the run's failure roll back.
    let error = Producer::new(&client, format!("{workflow}-handler"), "2")
        .submit(&format!("resume:on_failure:{run}"), &json!({}))
        .await
        .err()
        .expect("submitted a reserved key");
    let code = error
        .downcast_ref::<tokio_postgres::Error>()
        .and_then(tokio_postgres::Error::code)
        .map(|code| code.code().to_string());
    assert_eq!(code.as_deref(), Some("22023"));
    cancel(&client, run).await;
    assert_eq!(handlers(&client, run).await, 1);
}

pub(super) async fn worker_exhaustion_queues_handler_without_step_output() {
    let client = connect().await;
    let vendor = connect().await;
    vendor
        .batch_execute(
            "create schema if not exists checks;
             create table if not exists checks.vendor_effects (run_id bigint primary key)",
        )
        .await
        .unwrap();
    let (id, workflow) = submit(&client, "worker", 1).await;
    let (stop, shutdown) = oneshot::channel();
    let worker = Worker::new(connect().await, &workflow, "1")
        .poll_interval(Duration::from_millis(10))
        .run(
            async {
                let _ = shutdown.await;
            },
            async |run| {
                run.step("charge", async |_| {
                    vendor
                        .execute("insert into checks.vendor_effects values ($1)", &[&run.id])
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
        assert!(
            run_is(
                &client,
                id,
                "not exists (select 1 from resume.steps s where s.run_id = runs.id)
                 and (select count(*) from checks.vendor_effects v where v.run_id = runs.id) = 1"
            )
            .await
        );
        stop.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(worker, observer);
    result.unwrap();
}
