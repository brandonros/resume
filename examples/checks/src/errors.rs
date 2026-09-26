//! Error policy through contextual wrappers and worker claim failures.

use std::fmt;
use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use resume::{Permanent, Producer, RetryPolicy, Snooze, Worker};
use serde_json::json;
use tokio::sync::oneshot;

use crate::common::{connect, make_due, run_is, submit, wait_for, workflow};

#[derive(Debug)]
struct Context(resume::Error);

impl fmt::Display for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "handler context: {}", self.0)
    }
}

impl std::error::Error for Context {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

pub(super) fn context(error: resume::Error) -> resume::Error {
    Box::new(Context(Box::new(Context(error))))
}

pub(super) async fn wrapped_errors_preserve_failure_and_snooze_policy() {
    let client = connect().await;
    let name = workflow("wrapped-errors");
    let permanent = submit(&client, &name).await;
    let snoozing = Producer::new(&client, &name, "1")
        .retry(RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::default()
        })
        .submit("snoozing", &json!({}))
        .await
        .unwrap()
        .id;
    let once = Producer::new(&client, &name, "1")
        .submit("once", &json!({}))
        .await
        .unwrap()
        .id;
    let ready = AtomicBool::new(false);
    let (stop, shutdown) = oneshot::channel::<()>();
    let worker = Worker::new(connect().await, &name, "1")
        .poll_interval(Duration::from_millis(10))
        .run(
            async {
                let _ = shutdown.await;
            },
            async |run| {
                if run.id == once {
                    run.step_once("send", async || {
                        Err(context(Snooze(Duration::from_secs(300)).into()))
                    })
                    .await
                    .map_err(context)?;
                } else {
                    run.step("work", async |_| {
                        if run.id == permanent {
                            return Err(Permanent("invalid input".into()).into());
                        }
                        if !ready.load(Ordering::Relaxed) {
                            return Err(Snooze(Duration::from_secs(300)).into());
                        }
                        Ok(json!("ready"))
                    })
                    .await
                    .map_err(context)?;
                }
                Ok(())
            },
        );
    let observer = async {
        wait_for(&client, permanent, "failed_at is not null").await;
        assert!(run_is(&client, permanent,
            "attempt = 1 and attempts_used = 1 and last_error = 'handler context: handler context: invalid input'"
        ).await);
        wait_for(&client, snoozing, "attempt = 1 and not leased").await;
        assert!(
            run_is(
                &client,
                snoozing,
                "attempts_used = 0 and failed_at is null and last_error is null
             and available_at > clock_timestamp() + interval '4 minutes'
             and not exists (select 1 from resume.steps s where s.run_id = runs.id)"
            )
            .await
        );
        wait_for(&client, once, "failed_at is not null").await;
        assert!(
            run_is(
                &client,
                once,
                "attempt = 1 and attempts_used = 1
             and last_error like '%use a regular step for readiness checks%'
             and (select completed_at is null from resume.steps s where s.run_id = runs.id)"
            )
            .await
        );
        ready.store(true, Ordering::Relaxed);
        make_due(&client, snoozing).await;
        wait_for(&client, snoozing, "completed_at is not null").await;
        assert!(run_is(&client, snoozing, "attempt = 2 and attempts_used = 1").await);
        stop.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(worker, observer);
    result.unwrap();
}

pub(super) async fn nonretryable_claim_error_stops_the_worker() {
    let client = connect().await;
    // A session configuration error on an otherwise healthy connection. The first claim
    // query writes to runs, so PostgreSQL rejects it even when the queue is empty.
    client
        .batch_execute("set default_transaction_read_only = on")
        .await
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        Worker::new(client, workflow("read-only-worker"), "1")
            .poll_interval(Duration::from_millis(10))
            .run(pending(), async |_| {
                panic!("claimed on a read-only connection")
            }),
    )
    .await
    .expect("worker kept retrying a configuration error");
    let error = result.unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<tokio_postgres::Error>()
            .unwrap()
            .code()
            .unwrap()
            .code(),
        "25006"
    );
}

pub(super) async fn transient_claim_error_retries_then_processes_work() {
    let mut client = connect().await;
    let name = workflow("claim-lock-timeout");
    let run = submit(&client, &name).await;
    let worker_client = connect().await;
    worker_client
        .batch_execute("set lock_timeout = '50ms'")
        .await
        .unwrap();
    let pid: i32 = worker_client
        .query_one("select pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let tx = client.transaction().await.unwrap();
    tx.batch_execute("lock table resume.runs in access exclusive mode")
        .await
        .unwrap();
    let monitor = connect().await;
    let (stop, shutdown) = oneshot::channel::<()>();
    let worker = Worker::new(worker_client, &name, "1")
        .poll_interval(Duration::from_secs(1))
        .run(
            async {
                let _ = shutdown.await;
            },
            async |_| Ok(()),
        );
    let observer = async {
        // Observe the failed claim returning to idle before releasing the lock. This
        // proves the worker survives an actual lock timeout, rather than just waiting.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let idle: bool = monitor
                    .query_one(
                        "select state = 'idle' and query like 'select resume.expire_runs%'
                     from pg_stat_activity where pid = $1",
                        &[&pid],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if idle {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("did not observe a failed claim");
        tx.rollback().await.unwrap();
        wait_for(&monitor, run, "completed_at is not null").await;
        assert!(run_is(&monitor, run, "attempt = 1 and attempts_used = 1").await);
        stop.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(worker, observer);
    result.unwrap();
}
