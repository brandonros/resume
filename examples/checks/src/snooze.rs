//! Durable waiting through the worker and SQL claim path. Run with `just check`.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use resume::{Producer, RetryPolicy, Snooze, Worker};
use serde_json::json;
use tokio::sync::oneshot;

use crate::common::{
    claim, complete, connect, make_due, pass_deadline, run_is, submit, wait_for, workflow,
};

pub(super) async fn snooze_releases_worker_and_locks_preserves_progress_and_costs_no_retry() {
    let mut client = connect().await;
    let name = workflow("snooze-progress");
    let paused = Producer::new(&client, &name, "1")
        .retry(RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::default()
        })
        .submit("paused", &json!({}))
        .await
        .unwrap()
        .id;
    client
        .batch_execute(
            "create schema if not exists checks;
             create table if not exists checks.snooze_effects (run_id bigint primary key, writes integer not null)",
        )
        .await
        .unwrap();
    client
        .execute(
            "insert into checks.snooze_effects values ($1, 0)",
            &[&paused],
        )
        .await
        .unwrap();

    let ready = AtomicBool::new(false);
    let starts = AtomicU32::new(0);
    let checks = AtomicU32::new(0);
    let (stop, shutdown) = oneshot::channel::<()>();
    let worker = Worker::new(connect().await, &name, "1")
        .poll_interval(Duration::from_millis(10))
        .run(
            async {
                let _ = shutdown.await;
            },
            async |run| {
                if run.id != paused {
                    return Ok(());
                }
                let export = run
                    .step("start_export", async |tx| {
                        starts.fetch_add(1, Ordering::Relaxed);
                        tx.execute(
                            "update checks.snooze_effects set writes = writes + 1 where run_id = $1",
                            &[&run.id],
                        )
                        .await?;
                        Ok(json!({"id": "export-123"}))
                    })
                    .await?;
                assert_eq!(export, json!({"id": "export-123"}));
                run.step("check_export", async |tx| {
                    checks.fetch_add(1, Ordering::Relaxed);
                    tx.execute(
                        "update checks.snooze_effects set writes = writes + 100 where run_id = $1",
                        &[&run.id],
                    )
                    .await?;
                    if !ready.load(Ordering::Relaxed) {
                        return Err(Snooze(Duration::from_secs(300)).into());
                    }
                    Ok(json!({"ready": true}))
                })
                .await?;
                Ok(())
            },
        );

    let observer = async {
        wait_for(
            &client,
            paused,
            "attempt = 1 and attempts_used = 0 and not leased",
        )
        .await;
        let first_attempt: i64 = client
            .query_one("select attempt from resume.runs where id = $1", &[&paused])
            .await
            .unwrap()
            .get(0);
        assert!(
            run_is(
                &client,
                paused,
                "attempts_used = 0 and last_error is null
                 and available_at > clock_timestamp() + interval '4 minutes'"
            )
            .await
        );
        // The saved step remains; the unfinished check and its database writes rolled back.
        assert!(
            run_is(
                &client,
                paused,
                "(select count(*) from resume.steps s where s.run_id = runs.id) = 1"
            )
            .await
        );
        let tx = client.transaction().await.unwrap();
        tx.query_one(
            "select id from resume.runs where id = $1 for update nowait",
            &[&paused],
        )
        .await
        .unwrap();
        assert_eq!(
            tx.query_one(
                "select writes from checks.snooze_effects where run_id = $1 for update nowait",
                &[&paused],
            )
            .await
            .unwrap()
            .get::<_, i32>(0),
            1
        );
        tx.rollback().await.unwrap();
        assert!(
            complete(&client, paused, first_attempt).await.is_err(),
            "a snoozed claim still owned the run"
        );

        // The same worker must finish other work while this run is still snoozed.
        let other = Producer::new(&client, &name, "1")
            .submit("other", &json!({}))
            .await
            .unwrap()
            .id;
        wait_for(&client, other, "completed_at is not null").await;
        let competitor = connect().await;
        let (a, b) = tokio::join!(claim(&client, &name), claim(&competitor, &name));
        assert!(a.is_none());
        assert!(b.is_none());

        make_due(&client, paused).await;
        wait_for(
            &client,
            paused,
            "attempt = 2 and attempts_used = 0 and not leased",
        )
        .await;
        assert_eq!(
            starts.load(Ordering::Relaxed),
            1,
            "repeated a completed step"
        );
        assert_eq!(checks.load(Ordering::Relaxed), 2);
        ready.store(true, Ordering::Relaxed);
        make_due(&client, paused).await;
        wait_for(&client, paused, "completed_at is not null").await;
        assert!(
            run_is(
                &client,
                paused,
                &format!(
                    "attempt = {} and attempts_used = 1 and failed_at is null",
                    first_attempt + 2
                )
            )
            .await
        );
        assert_eq!(starts.load(Ordering::Relaxed), 1);
        assert_eq!(checks.load(Ordering::Relaxed), 3);
        assert_eq!(
            client
                .query_one(
                    "select writes from checks.snooze_effects where run_id = $1",
                    &[&paused],
                )
                .await
                .unwrap()
                .get::<_, i32>(0),
            101
        );
        stop.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(worker, observer);
    result.unwrap();
}

pub(super) async fn snooze_cannot_delay_the_deadline() {
    let client = connect().await;
    let name = workflow("snooze-deadline");
    let run = Producer::new(&client, &name, "1")
        .deadline(Duration::from_secs(3600))
        .submit("key", &json!({}))
        .await
        .unwrap()
        .id;
    let (_, attempt) = claim(&client, &name).await.unwrap();
    client
        .execute("select resume.release_run($1, $2, 7200)", &[&run, &attempt])
        .await
        .unwrap();
    assert!(run_is(&client, run, "available_at = deadline_at").await);
    pass_deadline(&client, run).await;
    assert!(claim(&client, &name).await.is_none());
    assert!(
        run_is(
            &client,
            run,
            "failed_at is not null and last_error = 'the run passed its deadline'"
        )
        .await
    );
}

pub(super) async fn step_once_cannot_snooze_and_repeat_its_action() {
    let client = connect().await;
    let name = workflow("snooze-once");
    let run = submit(&client, &name).await;
    let (stop, shutdown) = oneshot::channel::<()>();
    let worker = Worker::new(connect().await, &name, "1")
        .poll_interval(Duration::from_millis(10))
        .run(
            async {
                let _ = shutdown.await;
            },
            async |run| {
                run.step_once(
                    "send",
                    async || Err(Snooze(Duration::from_secs(300)).into()),
                )
                .await?;
                Ok(())
            },
        );
    let observer = async {
        wait_for(&client, run, "failed_at is not null").await;
        assert!(
            run_is(
                &client,
                run,
                "attempt = 1 and attempts_used = 1 and last_error like '%cannot snooze%'"
            )
            .await
        );
        assert!(
            run_is(
                &client,
                run,
                "(select completed_at is null from resume.steps s where s.run_id = runs.id)"
            )
            .await,
            "lost the unknown-outcome marker"
        );
        stop.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(worker, observer);
    result.unwrap();
}

pub(super) async fn release_validates_delay_and_zero_releases_immediately() {
    let client = connect().await;
    let name = workflow("snooze-release");
    let run = submit(&client, &name).await;
    let (_, attempt) = claim(&client, &name).await.unwrap();
    for delay in [
        None,
        Some(-1.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
    ] {
        let error = client
            .execute(
                "select resume.release_run($1, $2, $3)",
                &[&run, &attempt, &delay],
            )
            .await
            .unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "22023");
    }
    client
        .execute("select resume.release_run($1, $2, 0)", &[&run, &attempt])
        .await
        .unwrap();
    let reclaimed = client
        .query_one(
            "select id, attempt, attempts_used from resume.claim_run($1, '1', 60)",
            &[&name],
        )
        .await
        .unwrap();
    assert_eq!(reclaimed.get::<_, i64>("id"), run);
    assert_eq!(reclaimed.get::<_, i64>("attempt"), attempt + 1);
    assert_eq!(reclaimed.get::<_, i64>("attempts_used"), 1);
}
