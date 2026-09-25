//! Durable waiting through the real worker and SQL claim path. Uses the same scratch database
//! as ownership.rs; run with `just test`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use resume::{Producer, RetryPolicy, Snooze, Worker};
use serde_json::json;
use tokio::sync::oneshot;
use tokio_postgres::{Client, NoTls};

async fn connect() -> Client {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL; run the tests with `just test`");
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

fn workflow(name: &str) -> String {
    format!("snooze-{name}-{}", std::process::id())
}

async fn wait_for(client: &Client, run: i64, condition: &str) {
    let query = format!("select ({condition}) from resume.runs where id = $1");
    tokio::time::timeout(Duration::from_secs(10), async {
        while !client
            .query_one(&query, &[&run])
            .await
            .unwrap()
            .get::<_, bool>(0)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("run {run} never reached {condition}"));
}

async fn make_due(client: &Client, run: i64) {
    // Advance eligibility directly instead of sleeping for the snooze duration.
    client
        .execute(
            "update resume.runs set available_at = clock_timestamp() - interval '1 second'
             where id = $1",
            &[&run],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn snooze_releases_worker_and_locks_preserves_progress_and_costs_no_retry() {
    tokio::task::LocalSet::new().run_until(async {
    let mut client = connect().await;
    let name = workflow("progress");
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
            "create table resume.test_snooze_effects (run_id bigint primary key, writes integer not null)",
        )
        .await
        .unwrap();
    client
        .execute(
            "insert into resume.test_snooze_effects values ($1, 0)",
            &[&paused],
        )
        .await
        .unwrap();

    let ready = Arc::new(AtomicBool::new(false));
    let starts = Arc::new(AtomicU32::new(0));
    let checks = Arc::new(AtomicU32::new(0));
    let (worker_ready, worker_starts, worker_checks) =
        (ready.clone(), starts.clone(), checks.clone());
    let worker_client = connect().await;
    let worker_name = name.clone();
    let (stop, shutdown) = oneshot::channel::<()>();
    let worker = tokio::task::spawn_local(async move {
        Worker::new(worker_client, worker_name, "1")
            .poll_interval(Duration::from_millis(10))
            .run(async { let _ = shutdown.await; }, async |client, run| {
                if run.id != paused {
                    return Ok(());
                }
                let export = run.step(client, "start_export", async |tx| {
                    worker_starts.fetch_add(1, Ordering::Relaxed);
                    tx.execute(
                        "update resume.test_snooze_effects set writes = writes + 1 where run_id = $1",
                        &[&run.id],
                    ).await?;
                    Ok(json!({"id": "export-123"}))
                }).await?;
                assert_eq!(export, json!({"id": "export-123"}));
                run.step(client, "check_export", async |tx| {
                    worker_checks.fetch_add(1, Ordering::Relaxed);
                    tx.execute(
                        "update resume.test_snooze_effects set writes = writes + 100 where run_id = $1",
                        &[&run.id],
                    ).await?;
                    if !worker_ready.load(Ordering::Relaxed) {
                        return Err(Snooze(Duration::from_secs(300)).into());
                    }
                    Ok(json!({"ready": true}))
                }).await?;
                Ok(())
            })
            .await
    });

    wait_for(&client, paused, "released = 1 and not leased").await;
    let row = client
        .query_one(
            "select attempt, attempt - released as used,
                available_at > clock_timestamp() + interval '4 minutes' as future,
                last_error
         from resume.runs where id = $1",
            &[&paused],
        )
        .await
        .unwrap();
    let first_attempt: i64 = row.get("attempt");
    assert_eq!(row.get::<_, i64>("used"), 0);
    assert!(row.get::<_, bool>("future"));
    assert_eq!(row.get::<_, Option<String>>("last_error"), None);
    // The saved step remains; the unfinished check and its database writes rolled back.
    assert_eq!(
        client
            .query_one(
                "select count(*) from resume.steps where run_id = $1",
                &[&paused],
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
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
            "select writes from resume.test_snooze_effects where run_id = $1 for update nowait",
            &[&paused],
        )
        .await
        .unwrap()
        .get::<_, i32>(0),
        1
    );
    tx.rollback().await.unwrap();
    assert!(
        client
            .execute(
                "select resume.complete_run($1, $2)",
                &[&paused, &first_attempt],
            )
            .await
            .is_err(),
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
    let (a, b) = tokio::join!(
        async { client.query_opt("select id from resume.claim_run($1, '1', 60)", &[&name]).await },
        async { competitor.query_opt("select id from resume.claim_run($1, '1', 60)", &[&name]).await },
    );
    assert!(a.unwrap().is_none());
    assert!(b.unwrap().is_none());

    make_due(&client, paused).await;
    wait_for(&client, paused, "released = 2 and not leased").await;
    assert_eq!(
        starts.load(Ordering::Relaxed),
        1,
        "repeated a completed step"
    );
    assert_eq!(checks.load(Ordering::Relaxed), 2);
    ready.store(true, Ordering::Relaxed);
    make_due(&client, paused).await;
    wait_for(&client, paused, "completed_at is not null").await;
    let row = client
        .query_one(
            "select attempt, attempt - released as used, failed_at is null as healthy
         from resume.runs where id = $1",
            &[&paused],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>("attempt"), first_attempt + 2);
    assert_eq!(row.get::<_, i64>("used"), 1);
    assert!(row.get::<_, bool>("healthy"));
    assert_eq!(starts.load(Ordering::Relaxed), 1);
    assert_eq!(checks.load(Ordering::Relaxed), 3);
    assert_eq!(
        client
            .query_one(
                "select writes from resume.test_snooze_effects where run_id = $1",
                &[&paused],
            )
            .await
            .unwrap()
            .get::<_, i32>(0),
        101
    );
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    }).await;
}

#[tokio::test]
async fn snooze_cannot_delay_the_deadline() {
    let client = connect().await;
    let name = workflow("deadline");
    let run = Producer::new(&client, &name, "1")
        .deadline(Duration::from_secs(3600))
        .submit("key", &json!({}))
        .await
        .unwrap()
        .id;
    let attempt: i64 = client
        .query_one(
            "select attempt from resume.claim_run($1, '1', 60)",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    client
        .execute("select resume.release_run($1, $2, 7200)", &[&run, &attempt])
        .await
        .unwrap();
    assert!(
        client
            .query_one(
                "select available_at = deadline_at from resume.runs where id = $1",
                &[&run],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    client
        .execute(
            "update resume.runs
         set deadline_at = clock_timestamp() - interval '1 second',
             available_at = clock_timestamp() - interval '1 second'
         where id = $1",
            &[&run],
        )
        .await
        .unwrap();
    assert!(
        client
            .query_opt("select id from resume.claim_run($1, '1', 60)", &[&name],)
            .await
            .unwrap()
            .is_none()
    );
    let row = client
        .query_one(
            "select failed_at is not null, last_error from resume.runs where id = $1",
            &[&run],
        )
        .await
        .unwrap();
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, String>(1), "the run passed its deadline");
}

#[tokio::test]
async fn step_once_cannot_snooze_and_repeat_its_action() {
    let client = connect().await;
    let name = workflow("once");
    let run = Producer::new(&client, &name, "1")
        .submit("key", &json!({}))
        .await
        .unwrap()
        .id;
    let worker_client = connect().await;
    let (stop, shutdown) = oneshot::channel::<()>();
    let worker = tokio::spawn(async move {
        Worker::new(worker_client, name, "1")
            .poll_interval(Duration::from_millis(10))
            .run(
                async {
                    let _ = shutdown.await;
                },
                async |client, run| {
                    run.step_once(client, "send", async || {
                        Err(Snooze(Duration::from_secs(300)).into())
                    })
                    .await?;
                    Ok(())
                },
            )
            .await
    });
    wait_for(&client, run, "failed_at is not null").await;
    let row = client
        .query_one(
            "select attempt, released, last_error from resume.runs where id = $1",
            &[&run],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, i32>(1), 0);
    assert!(row.get::<_, String>(2).contains("cannot snooze"));
    assert!(
        client
            .query_one(
                "select completed_at is null from resume.steps where run_id = $1",
                &[&run],
            )
            .await
            .unwrap()
            .get::<_, bool>(0),
        "lost the unknown-outcome marker"
    );
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn release_validates_delay_and_zero_releases_immediately() {
    let client = connect().await;
    let name = workflow("release");
    let run = Producer::new(&client, &name, "1")
        .submit("key", &json!({}))
        .await
        .unwrap()
        .id;
    let attempt: i64 = client
        .query_one(
            "select attempt from resume.claim_run($1, '1', 60)",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
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
    let next: i64 = client
        .query_one(
            "select attempt from resume.claim_run($1, '1', 60)",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(next, attempt + 1);
}
