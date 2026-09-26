use std::time::Duration;

use resume::{Result, run_one, submit};
use serde_json::json;
use tokio_postgres::{Client, NoTls};

async fn connect() -> Client {
    let url = std::env::var("DATABASE_URL").expect("set DATABASE_URL to a disposable database");
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

async fn claim(client: &Client, workflow: &str) -> (i64, i64) {
    let row = client
        .query_one("select id, attempt from resume.claim($1)", &[&workflow])
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

async fn due(client: &Client, id: i64) {
    client.execute("update resume.jobs set available_at = clock_timestamp() - interval '1 second' where id = $1", &[&id]).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn submission_is_atomic_and_idempotent() -> Result<()> {
    let mut a = connect().await;
    let b = connect().await;
    let input = json!({"amount": 10});
    let (first, duplicate) = tokio::join!(
        submit(&a, "submit", "key", &input),
        submit(&b, "submit", "key", &input),
    );
    let id = first?;
    assert_eq!(id, duplicate?);
    assert!(
        submit(&a, "submit", "key", &json!({"amount": 11}))
            .await
            .is_err()
    );
    assert_ne!(id, submit(&a, "another", "key", &input).await?);
    let tx = a.transaction().await?;
    let rolled_back = submit(&tx, "submit", "rollback", &input).await?;
    tx.rollback().await?;
    assert!(
        a.query_opt("select id from resume.jobs where id = $1", &[&rolled_back])
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn failure_rolls_back_and_retry_replays_saved_output() -> Result<()> {
    let mut client = connect().await;
    client
        .batch_execute("create table effects (job bigint, step integer, primary key (job, step))")
        .await?;
    let id = submit(&client, "replay", "key", &json!(null)).await?;
    let result = run_one(&mut client, "replay", async |job, steps| {
        steps
            .step("first", async |tx| {
                tx.execute("insert into effects values ($1, 1)", &[&job.id])
                    .await?;
                Ok(json!(null))
            })
            .await?;
        steps
            .step("second", async |tx| {
                tx.execute("insert into effects values ($1, 2)", &[&job.id])
                    .await?;
                Err("try again".into())
            })
            .await?;
        Ok(())
    })
    .await;
    assert_eq!(result.unwrap_err().to_string(), "try again");
    let row = client.query_one(
        "select not leased and not completed and last_error = 'try again' and available_at > clock_timestamp(),
         (select count(*) from effects where job = $1), (select count(*) from resume.steps where job_id = $1)
         from resume.jobs where id = $1", &[&id],
    ).await?;
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, i64>(1), 1);
    assert_eq!(row.get::<_, i64>(2), 1);
    assert!(
        !run_one(&mut client, "replay", async |_, _| panic!(
            "claimed before retry delay"
        ))
        .await?
    );
    due(&client, id).await;
    assert!(
        run_one(&mut client, "replay", async |job, steps| {
            let saved = steps
                .step("first", async |_| panic!("repeated a committed action"))
                .await?;
            assert_eq!(saved, json!(null));
            steps
                .step("second", async |tx| {
                    tx.execute("insert into effects values ($1, 2)", &[&job.id])
                        .await?;
                    Ok(json!(42))
                })
                .await?;
            Ok(())
        })
        .await?
    );
    assert_eq!(id, submit(&client, "replay", "key", &json!(null)).await?);
    assert!(
        !run_one(&mut client, "replay", async |_, _| panic!(
            "reclaimed a completed job"
        ))
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn abandoned_claims_are_reclaimed_and_stale_tokens_are_rejected() -> Result<()> {
    let a = connect().await;
    let b = connect().await;
    let id = submit(&a, "fencing", "key", &json!(null)).await?;
    let (one, two) = tokio::join!(
        a.query_opt("select attempt from resume.claim('fencing')", &[]),
        b.query_opt("select attempt from resume.claim('fencing')", &[]),
    );
    let (one, two) = (one?, two?);
    assert_ne!(
        one.is_some(),
        two.is_some(),
        "exactly one worker must claim"
    );
    let first: i64 = one.or(two).unwrap().get(0);
    assert!(
        b.query_opt("select id from resume.claim('fencing')", &[])
            .await?
            .is_none()
    );
    due(&a, id).await;
    assert!(
        a.execute("select resume.begin_step($1, $2)", &[&id, &first])
            .await
            .is_err()
    );
    let (claimed, second) = claim(&b, "fencing").await;
    assert_eq!(claimed, id);
    assert!(second > first);
    assert!(
        a.execute("select resume.begin_step($1, $2)", &[&id, &first])
            .await
            .is_err()
    );
    for error in [None, Some("old error")] {
        assert!(
            a.execute("select resume.finish($1, $2, $3)", &[&id, &first, &error])
                .await
                .is_err()
        );
    }
    b.execute("select resume.finish($1, $2, null)", &[&id, &second])
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn a_step_lock_prevents_reclaim_after_the_visible_lease_expires() -> Result<()> {
    let mut a = connect().await;
    let b = connect().await;
    let id = submit(&a, "locked", "key", &json!(null)).await?;
    let (_, attempt) = claim(&a, "locked").await;
    a.execute("update resume.jobs set available_at = clock_timestamp() + interval '1 second' where id = $1", &[&id]).await?;
    let tx = a.transaction().await?;
    tx.execute("select resume.begin_step($1, $2)", &[&id, &attempt])
        .await?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        b.query_opt("select id from resume.claim('locked')", &[])
            .await?
            .is_none()
    );
    tx.execute(
        "insert into resume.steps values ($1, 'saved', '42')",
        &[&id],
    )
    .await?;
    tx.commit().await?;
    a.execute("select resume.finish($1, $2, null)", &[&id, &attempt])
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn dropping_a_handler_keeps_saved_progress_for_recovery() -> Result<()> {
    let mut client = connect().await;
    client
        .batch_execute("create table dropped_effects (job bigint)")
        .await?;
    let id = submit(&client, "dropped", "key", &json!(null)).await?;
    let (sent, received) = tokio::sync::oneshot::channel();
    {
        let work = run_one(&mut client, "dropped", async |job, steps| {
            steps.step("saved", async |_| Ok(json!(7))).await?;
            steps
                .step("unfinished", async |tx| {
                    tx.execute("insert into dropped_effects values ($1)", &[&job.id])
                        .await?;
                    sent.send(()).unwrap();
                    std::future::pending().await
                })
                .await?;
            Ok(())
        });
        tokio::select! {
            result = work => panic!("handler unexpectedly finished: {result:?}"),
            _ = received => {}
        }
    }
    assert_eq!(
        client
            .query_one("select count(*) from dropped_effects", &[])
            .await?
            .get::<_, i64>(0),
        0
    );
    due(&client, id).await;
    assert!(
        run_one(&mut client, "dropped", async |_, steps| {
            assert_eq!(
                steps
                    .step("saved", async |_| panic!("repeated saved step"))
                    .await?,
                json!(7)
            );
            steps.step("unfinished", async |_| Ok(json!(8))).await?;
            Ok(())
        })
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn duplicate_step_keys_release_the_attempt_with_an_error() -> Result<()> {
    let mut client = connect().await;
    let id = submit(&client, "keys", "key", &json!(null)).await?;
    let result = run_one(&mut client, "keys", async |_, steps| {
        steps.step("same", async |_| Ok(json!(1))).await?;
        steps
            .step("same", async |_| panic!("ran a duplicate step"))
            .await?;
        Ok(())
    })
    .await;
    assert!(result.unwrap_err().to_string().contains("unique"));
    assert!(
        client
            .query_one(
                "select not leased and not completed from resume.jobs where id = $1",
                &[&id]
            )
            .await?
            .get::<_, bool>(0)
    );
    Ok(())
}
