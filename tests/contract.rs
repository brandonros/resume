use std::time::Duration;

use resume::{Result, Retry, run_one, submit};
use serde_json::json;
use tokio_postgres::{Client, NoTls};

fn retry_now(_: &resume::Job, _: &resume::Error) -> Retry {
    Retry::After(Duration::ZERO)
}

async fn connect() -> Client {
    let url = std::env::var("DATABASE_URL").expect("set DATABASE_URL to a disposable database");
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

fn db_message(error: &resume::Error) -> &str {
    error
        .downcast_ref::<tokio_postgres::Error>()
        .unwrap()
        .as_db_error()
        .unwrap()
        .message()
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
    let result = run_one(
        &mut client,
        "replay",
        async |job, steps| {
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
        },
        |_, _| Retry::After(Duration::from_secs(1)),
    )
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
        !run_one(
            &mut client,
            "replay",
            async |_, _| panic!("claimed before retry delay"),
            retry_now
        )
        .await?
    );
    due(&client, id).await;
    assert!(
        run_one(
            &mut client,
            "replay",
            async |job, steps| {
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
            },
            retry_now
        )
        .await?
    );
    assert_eq!(id, submit(&client, "replay", "key", &json!(null)).await?);
    assert!(
        !run_one(
            &mut client,
            "replay",
            async |_, _| panic!("reclaimed a completed job"),
            retry_now
        )
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
        "insert into resume.steps values ($1, 'saved', 0, false, '42')",
        &[&id],
    )
    .await?;
    tx.commit().await?;
    a.execute("select resume.finish($1, $2, null, 1)", &[&id, &attempt])
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
        let work = run_one(
            &mut client,
            "dropped",
            async |job, steps| {
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
            },
            retry_now,
        );
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
        run_one(
            &mut client,
            "dropped",
            async |_, steps| {
                assert_eq!(
                    steps
                        .step("saved", async |_| panic!("repeated saved step"))
                        .await?,
                    json!(7)
                );
                steps.step("unfinished", async |_| Ok(json!(8))).await?;
                Ok(())
            },
            retry_now
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn duplicate_step_keys_release_the_attempt_with_an_error() -> Result<()> {
    let mut client = connect().await;
    let id = submit(&client, "keys", "key", &json!(null)).await?;
    let result = run_one(
        &mut client,
        "keys",
        async |_, steps| {
            steps.step("same", async |_| Ok(json!(1))).await?;
            steps
                .step("same", async |_| panic!("ran a duplicate step"))
                .await?;
            Ok(())
        },
        retry_now,
    )
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

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn application_controls_retry_count_and_receives_original_error() -> Result<()> {
    let mut client = connect().await;
    let id = submit(&client, "policy", "key", &json!(null)).await?;
    for expected in 1..=3 {
        let mut called = false;
        let error = run_one(
            &mut client,
            "policy",
            async |_, _| Err(std::io::Error::other("broken job").into()),
            |job, error| {
                called = true;
                assert_eq!(job.id, id);
                assert_eq!(job.attempt, expected);
                assert!(error.downcast_ref::<std::io::Error>().is_some());
                if job.attempt < 3 {
                    retry_now(job, error)
                } else {
                    Retry::Stop
                }
            },
        )
        .await
        .unwrap_err();
        assert!(called);
        assert!(error.downcast_ref::<std::io::Error>().is_some());
    }
    due(&client, id).await;
    assert_eq!(id, submit(&client, "policy", "key", &json!(null)).await?);
    assert!(
        !run_one(
            &mut client,
            "policy",
            async |_, _| panic!("restarted stopped job"),
            |_, _| panic!("policy on idle")
        )
        .await?
    );
    assert!(client.query_one("select paused and not leased and last_error = 'broken job' from resume.jobs where id = $1", &[&id]).await?.get::<_, bool>(0));
    submit(&client, "policy-success", "key", &json!(null)).await?;
    assert!(
        run_one(
            &mut client,
            "policy-success",
            async |_, _| Ok(()),
            |_, _| panic!("policy on success")
        )
        .await?
    );
    // There is no stored attempt budget: crashed claims recover independently of the callback.
    let id = submit(&client, "policy-crash", "key", &json!(null)).await?;
    for expected in 1..=6 {
        assert_eq!(claim(&client, "policy-crash").await.1, expected);
        due(&client, id).await;
    }
    assert!(
        run_one(
            &mut client,
            "policy-crash",
            async |job, _| {
                assert_eq!(job.attempt, 7);
                Ok(())
            },
            |_, _| panic!("policy on recovered success")
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn application_selects_delay_per_failure_and_sql_rejects_invalid_delays() -> Result<()> {
    let mut client = connect().await;
    let id = submit(&client, "delayed", "key", &json!(null)).await?;
    for delay in [Duration::from_millis(30_500), Duration::ZERO] {
        assert!(
            run_one(
                &mut client,
                "delayed",
                async |_, _| Err("failed".into()),
                |_, _| Retry::After(delay)
            )
            .await
            .is_err()
        );
        let remaining: f64 = client.query_one("select extract(epoch from available_at - clock_timestamp())::double precision from resume.jobs where id = $1", &[&id]).await?.get(0);
        assert!(remaining <= delay.as_secs_f64());
        assert!(remaining > delay.as_secs_f64() - 2.0);
        if !delay.is_zero() {
            assert!(
                !run_one(
                    &mut client,
                    "delayed",
                    async |_, _| panic!("retried too soon"),
                    retry_now
                )
                .await?
            );
            due(&client, id).await;
        }
    }
    let (_, attempt) = claim(&client, "delayed").await;
    for delay in [-1.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
        assert!(
            client
                .execute(
                    "select resume.finish($1, $2, 'failed', 0, $3)",
                    &[&id, &attempt, &delay]
                )
                .await
                .is_err()
        );
    }
    assert!(
        client
            .query_one(
                "select leased and not paused from resume.jobs where id = $1",
                &[&id]
            )
            .await?
            .get::<_, bool>(0)
    );
    client
        .execute("select resume.finish($1, $2, null)", &[&id, &attempt])
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn positions_reject_changed_history_and_allow_a_new_suffix() -> Result<()> {
    let mut client = connect().await;
    let id = submit(&client, "positions", "key", &json!(null)).await?;
    assert!(
        run_one(
            &mut client,
            "positions",
            async |_, steps| {
                steps.step("first", async |_| Ok(json!(1))).await?;
                steps.step("second", async |_| Ok(json!(2))).await?;
                Err("retry".into())
            },
            retry_now
        )
        .await
        .is_err()
    );
    for key in ["second", "replacement"] {
        let error = run_one(
            &mut client,
            "positions",
            async |_, steps| {
                steps
                    .step(key, async |_| panic!("executed changed history"))
                    .await?;
                Ok(())
            },
            retry_now,
        )
        .await
        .unwrap_err();
        assert!(db_message(&error).contains("history differs"));
    }
    let error = run_one(
        &mut client,
        "positions",
        async |_, steps| {
            steps
                .step_once("first", async || panic!("changed regular step to once"))
                .await?;
            Ok(())
        },
        retry_now,
    )
    .await
    .unwrap_err();
    assert!(db_message(&error).contains("history differs"));
    let error = run_one(
        &mut client,
        "positions",
        async |_, steps| {
            steps
                .step("first", async |_| panic!("repeated saved action"))
                .await?;
            Ok(()) // Missing the recorded suffix must not complete the job.
        },
        retry_now,
    )
    .await
    .unwrap_err();
    assert!(db_message(&error).contains("omitted"));
    assert!(
        !client
            .query_one("select completed from resume.jobs where id = $1", &[&id])
            .await?
            .get::<_, bool>(0)
    );
    assert!(
        run_one(
            &mut client,
            "positions",
            async |_, steps| {
                assert_eq!(
                    steps
                        .step("first", async |_| panic!("repeated first"))
                        .await?,
                    json!(1)
                );
                assert_eq!(
                    steps
                        .step("second", async |_| panic!("repeated second"))
                        .await?,
                    json!(2)
                );
                steps.step("third", async |_| Ok(json!(3))).await?;
                Ok(())
            },
            retry_now
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn step_once_replays_json_null_without_repeating_the_action() -> Result<()> {
    let mut client = connect().await;
    submit(&client, "once-saved", "key", &json!(null)).await?;
    assert!(
        run_one(
            &mut client,
            "once-saved",
            async |_, steps| {
                steps.step_once("send", async || Ok(json!(null))).await?;
                Err("retry after save".into())
            },
            retry_now
        )
        .await
        .is_err()
    );
    assert!(
        run_one(
            &mut client,
            "once-saved",
            async |_, steps| {
                assert_eq!(
                    steps
                        .step_once("send", async || panic!("repeated external effect"))
                        .await?,
                    json!(null)
                );
                Ok(())
            },
            retry_now
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn swallowed_once_error_blocks_later_actions_and_completion() -> Result<()> {
    let mut client = connect().await;
    let id = submit(&client, "once-error", "key", &json!(null)).await?;
    let error = run_one(
        &mut client,
        "once-error",
        async |_, steps| {
            steps.step("before", async |_| Ok(json!(1))).await?;
            assert!(
                steps
                    .step_once("send", async || Err("vendor response lost".into()))
                    .await
                    .is_err()
            );
            assert!(
                steps
                    .step("after", async |_| panic!("advanced past unknown outcome"))
                    .await
                    .is_err()
            );
            Ok(())
        },
        retry_now,
    )
    .await
    .unwrap_err();
    assert!(db_message(&error).contains("unresolved"));
    let error = run_one(
        &mut client,
        "once-error",
        async |_, steps| {
            steps
                .step("before", async |_| panic!("repeated prefix"))
                .await?;
            steps
                .step_once("send", async || panic!("repeated unknown action"))
                .await?;
            Ok(())
        },
        retry_now,
    )
    .await
    .unwrap_err();
    assert!(db_message(&error).contains("unknown"));
    assert!(
        run_one(&mut client, "once-error", async |_, _| Ok(()), retry_now)
            .await
            .is_err()
    );
    assert!(client.query_one("select not completed and (select output is null from resume.steps where job_id = $1 and key = 'send') from resume.jobs where id = $1", &[&id]).await?.get::<_, bool>(0));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn dropping_a_once_action_keeps_the_start_marker() -> Result<()> {
    let mut client = connect().await;
    let id = submit(&client, "once-drop", "key", &json!(null)).await?;
    let (sent, received) = tokio::sync::oneshot::channel();
    {
        let work = run_one(
            &mut client,
            "once-drop",
            async |_, steps| {
                steps
                    .step_once("send", async || {
                        sent.send(()).unwrap();
                        std::future::pending().await
                    })
                    .await?;
                Ok(())
            },
            retry_now,
        );
        tokio::select! {
            result = work => panic!("unexpected completion: {result:?}"),
            _ = received => {}
        }
    }
    due(&client, id).await;
    let error = run_one(
        &mut client,
        "once-drop",
        async |_, steps| {
            steps
                .step_once("send", async || panic!("repeated interrupted action"))
                .await?;
            Ok(())
        },
        retry_now,
    )
    .await
    .unwrap_err();
    assert!(db_message(&error).contains("unknown"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn once_action_releases_the_lock_and_cannot_save_after_reclaim() -> Result<()> {
    let mut client = connect().await;
    let mut other = connect().await;
    let id = submit(&client, "once-stale", "key", &json!(null)).await?;
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let work = run_one(
        &mut client,
        "once-stale",
        async |_, steps| {
            steps
                .step_once("send", async || {
                    started.send(()).unwrap();
                    released.await?;
                    Ok(json!(42))
                })
                .await?;
            Ok(())
        },
        retry_now,
    );
    let observer = async {
        waiting.await.unwrap();
        let tx = other.transaction().await.unwrap();
        tx.query_one(
            "select id from resume.jobs where id = $1 for update nowait",
            &[&id],
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        due(&other, id).await;
        let (_, attempt) = claim(&other, "once-stale").await;
        assert!(
            other
                .query_one(
                    "select resume.start_step($1, $2, 'send', 0, true)",
                    &[&id, &attempt]
                )
                .await
                .is_err()
        );
        release.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(work, observer);
    assert!(db_message(&result.unwrap_err()).contains("claim is no longer valid"));
    assert!(
        other
            .query_one(
                "select output is null from resume.steps where job_id = $1",
                &[&id]
            )
            .await?
            .get::<_, bool>(0)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn a_timed_out_once_action_is_never_invoked_again() -> Result<()> {
    let mut client = connect().await;
    submit(&client, "once-timeout", "key", &json!(null)).await?;
    let error = run_one(
        &mut client,
        "once-timeout",
        async |_, steps| {
            steps
                .step_once("send", async || std::future::pending().await)
                .await?;
            Ok(())
        },
        retry_now,
    )
    .await
    .unwrap_err();
    assert!(error.is::<tokio::time::error::Elapsed>());
    let error = run_one(
        &mut client,
        "once-timeout",
        async |_, steps| {
            steps
                .step_once("send", async || panic!("repeated timed-out action"))
                .await?;
            Ok(())
        },
        retry_now,
    )
    .await
    .unwrap_err();
    assert!(db_message(&error).contains("unknown"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn resolving_records_the_outcome_but_requires_explicit_requeue() -> Result<()> {
    let mut client = connect().await;
    let id = submit(&client, "resolve", "key", &json!(null)).await?;
    assert!(
        run_one(
            &mut client,
            "resolve",
            async |_, steps| {
                steps.step("before", async |_| Ok(json!(1))).await?;
                steps
                    .step_once("send", async || Err("response lost".into()))
                    .await?;
                Ok(())
            },
            retry_now
        )
        .await
        .is_err()
    );
    assert!(
        client
            .execute("select resume.requeue($1)", &[&id])
            .await
            .is_err()
    );
    // The whole operator action is transactional.
    let tx = client.transaction().await?;
    tx.execute("select resume.resolve_step($1, 'send', 'null')", &[&id])
        .await?;
    tx.rollback().await?;
    assert!(
        client
            .query_one(
                "select output is null from resume.steps where job_id = $1 and key = 'send'",
                &[&id]
            )
            .await?
            .get::<_, bool>(0)
    );
    for query in [
        "select resume.resolve_step($1, 'send', null)",
        "select resume.resolve_step($1, 'missing', '42')",
        "select resume.resolve_step($1, 'before', '42')",
    ] {
        assert!(client.execute(query, &[&id]).await.is_err());
    }
    client
        .execute("select resume.resolve_step($1, 'send', 'null')", &[&id])
        .await?;
    assert!(
        !run_one(
            &mut client,
            "resolve",
            async |_, _| panic!("resolution restarted the job"),
            retry_now
        )
        .await?
    );
    let row = client
        .query_one(
            "select paused, attempt, last_error from resume.jobs where id = $1",
            &[&id],
        )
        .await?;
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, i64>(1), 1);
    assert_eq!(row.get::<_, &str>(2), "response lost");
    assert!(
        client
            .execute("select resume.resolve_step($1, 'send', '99')", &[&id])
            .await
            .is_err()
    );
    client
        .execute("select resume.requeue($1, 0)", &[&id])
        .await?;
    // A repeated command must not silently change the schedule.
    assert!(
        client
            .execute("select resume.requeue($1, 9)", &[&id])
            .await
            .is_err()
    );
    assert!(
        run_one(
            &mut client,
            "resolve",
            async |_, steps| {
                assert_eq!(
                    steps
                        .step("before", async |_| panic!("repeated saved prefix"))
                        .await?,
                    json!(1)
                );
                assert_eq!(
                    steps
                        .step_once("send", async || panic!("repeated resolved action"))
                        .await?,
                    json!(null)
                );
                steps.step("after", async |_| Ok(json!(2))).await?;
                Ok(())
            },
            retry_now
        )
        .await?
    );
    let row = client
        .query_one(
            "select attempt, completed from resume.jobs where id = $1",
            &[&id],
        )
        .await?;
    assert_eq!(row.get::<_, i64>(0), 2);
    assert!(row.get::<_, bool>(1));
    assert!(
        client
            .execute("select resume.requeue($1)", &[&id])
            .await
            .is_err()
    );
    assert!(
        client
            .execute("select resume.resolve_step($1, 'send', '42')", &[&id])
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn requeue_is_transactional_and_preserves_saved_steps() -> Result<()> {
    let mut client = connect().await;
    let id = submit(&client, "operator-retry", "key", &json!(null)).await?;
    assert!(
        client
            .execute("select resume.requeue($1)", &[&id])
            .await
            .is_err()
    );
    assert!(
        run_one(
            &mut client,
            "operator-retry",
            async |_, steps| {
                steps.step("saved", async |_| Ok(json!(7))).await?;
                Err("needs operator".into())
            },
            |_, _| Retry::Stop
        )
        .await
        .is_err()
    );
    for delay in [
        None,
        Some(-1.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
    ] {
        assert!(
            client
                .execute("select resume.requeue($1, $2)", &[&id, &delay])
                .await
                .is_err()
        );
    }
    let tx = client.transaction().await?;
    tx.execute("select resume.requeue($1)", &[&id]).await?;
    tx.rollback().await?;
    assert!(
        !run_one(
            &mut client,
            "operator-retry",
            async |_, _| panic!("rollback restarted job"),
            retry_now
        )
        .await?
    );
    client
        .execute("select resume.requeue($1, 10)", &[&id])
        .await?;
    assert!(client.query_one("select not paused and not leased and attempt = 1 and last_error = 'needs operator' and available_at > clock_timestamp() + interval '9 seconds' from resume.jobs where id = $1", &[&id]).await?.get::<_, bool>(0));
    assert!(
        !run_one(
            &mut client,
            "operator-retry",
            async |_, _| panic!("ignored requeue delay"),
            retry_now
        )
        .await?
    );
    due(&client, id).await;
    assert!(
        run_one(
            &mut client,
            "operator-retry",
            async |job, steps| {
                assert_eq!(job.attempt, 2);
                assert_eq!(
                    steps
                        .step("saved", async |_| panic!("repeated saved effect"))
                        .await?,
                    json!(7)
                );
                Ok(())
            },
            retry_now
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn operator_actions_fence_expired_workers_and_reject_active_claims() -> Result<()> {
    let client = connect().await;
    let id = submit(&client, "operator-fence", "key", &json!(null)).await?;
    let (_, first) = claim(&client, "operator-fence").await;
    // An interrupted step_once has committed its start.
    client
        .query_one(
            "select resume.start_step($1, $2, 'send', 0, true)",
            &[&id, &first],
        )
        .await?;
    assert!(
        client
            .execute("select resume.resolve_step($1, 'send', '42')", &[&id])
            .await
            .is_err()
    );
    assert!(
        client
            .execute("select resume.requeue($1)", &[&id])
            .await
            .is_err()
    );
    due(&client, id).await;
    assert!(
        client
            .execute("select resume.requeue($1)", &[&id])
            .await
            .is_err()
    );
    client
        .execute("select resume.resolve_step($1, 'send', '42')", &[&id])
        .await?;
    // Resolution fences the old attempt immediately, even before another worker claims.
    assert!(
        client
            .execute(
                "select resume.save_step($1, $2, 'send', '99')",
                &[&id, &first]
            )
            .await
            .is_err()
    );
    assert!(
        client
            .execute("select resume.finish($1, $2, 'old error')", &[&id, &first])
            .await
            .is_err()
    );
    client.execute("select resume.requeue($1)", &[&id]).await?;
    assert!(
        client
            .execute("select resume.begin_step($1, $2)", &[&id, &first])
            .await
            .is_err()
    );
    let (_, second) = claim(&client, "operator-fence").await;
    assert!(second > first);
    assert!(
        client
            .execute(
                "select resume.save_step($1, $2, 'send', '99')",
                &[&id, &first]
            )
            .await
            .is_err()
    );
    assert_eq!(
        client
            .query_one(
                "select resume.start_step($1, $2, 'send', 0, true)",
                &[&id, &second]
            )
            .await?
            .get::<_, serde_json::Value>(0),
        json!(42)
    );
    client
        .execute("select resume.finish($1, $2, null, 1)", &[&id, &second])
        .await?;
    assert!(
        client
            .execute("select resume.requeue(-1)", &[])
            .await
            .is_err()
    );
    assert!(
        client
            .execute("select resume.resolve_step(-1, 'send', '42')", &[])
            .await
            .is_err()
    );
    Ok(())
}
