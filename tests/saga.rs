use std::time::Duration;

use resume::{Compensating, Job, Result, Retry, SagaOutcome, run_one, submit};
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls};

async fn connect() -> Result<Client> {
    let (client, connection) =
        tokio_postgres::connect(&std::env::var("DATABASE_URL")?, NoTls).await?;
    tokio::spawn(async move { connection.await.expect("saga test connection failed") });
    Ok(client)
}

fn retry(_: &Job, _: &resume::Error) -> Retry {
    Retry::After(Duration::ZERO)
}
fn message(error: &resume::Error) -> &str {
    error
        .downcast_ref::<tokio_postgres::Error>()
        .unwrap()
        .as_db_error()
        .unwrap()
        .message()
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn successful_saga_replays_its_result_after_later_handler_failure() -> Result<()> {
    let mut client = connect().await?;
    let id = submit(&client, "saga-success", "key", &json!(null)).await?;
    let result = run_one(
        &mut client,
        "saga-success",
        async |_, steps| {
            let outcome = steps
                .saga(
                    async |saga| {
                        saga.step("save", "undo-save", async |_| Ok(json!(42)))
                            .await
                    },
                    async |_, _, _| panic!("unexpected compensation"),
                )
                .await?;
            assert_eq!(outcome, SagaOutcome::Completed(json!(42)));
            Err("failure after saga completed".into())
        },
        retry,
    )
    .await;
    assert!(result.is_err());
    assert!(
        run_one(
            &mut client,
            "saga-success",
            async |_, steps| {
                let outcome = steps
                    .saga(
                        async |_| panic!("repeated finished saga"),
                        async |_, _, _| panic!("unexpected compensation"),
                    )
                    .await?;
                assert_eq!(outcome, SagaOutcome::Completed(json!(42)));
                steps.step("after", async |_| Ok(json!(null))).await?;
                Ok(json!(42))
            },
            retry
        )
        .await?
    );
    assert!(client.query_one("select completed and saga_phase = 'completed' and failures = 1 from resume.jobs where id = $1", &[&id]).await?.get::<_, bool>(0));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn compensation_is_durable_reversed_and_resumes_after_partial_failure() -> Result<()> {
    let mut client = connect().await?;
    client.batch_execute("create table saga_effects (seq bigint generated always as identity, job bigint, label text, unique(job, label))").await?;
    let id = submit(&client, "saga-reverse", "key", &json!(null)).await?;
    let result = run_one(
        &mut client,
        "saga-reverse",
        async |job, steps| {
            let result = steps
                .saga(
                    async |saga| {
                        for key in ["a", "b"] {
                            saga.step(key, &format!("undo-{key}"), async |tx| {
                                tx.execute(
                                    "insert into saga_effects (job, label) values ($1, $2)",
                                    &[&job.id, &key],
                                )
                                .await?;
                                Ok(json!(key))
                            })
                            .await?;
                        }
                        // Swallowing the control-flow error must not permit new forward actions.
                        assert!(
                            saga.compensate(json!("abandoned"))
                                .await
                                .unwrap_err()
                                .is::<Compensating>()
                        );
                        let error = saga
                            .step("later", "undo-later", async |_| {
                                panic!("forward action after decision")
                            })
                            .await
                            .unwrap_err();
                        assert!(error.is::<Compensating>());
                        Ok(json!("ignored handler result"))
                    },
                    async |name, receipt, tx| {
                        assert_eq!(name, "undo-b");
                        assert_eq!(*receipt, json!("b"));
                        tx.execute(
                            "insert into saga_effects (job, label) values ($1, $2)",
                            &[&job.id, &name],
                        )
                        .await?;
                        Err("undo b failed".into())
                    },
                )
                .await;
            assert!(result.is_err());
            // Even swallowing the undo failure cannot finish an incomplete saga.
            Ok(json!("incorrect success"))
        },
        retry,
    )
    .await;
    assert!(message(&result.unwrap_err()).contains("unfinished saga"));
    let row = client
        .query_one(
            "select saga_phase, saga_result from resume.jobs where id = $1",
            &[&id],
        )
        .await?;
    assert_eq!(row.get::<_, &str>(0), "compensating");
    assert_eq!(row.get::<_, Value>(1), json!("abandoned"));
    assert_eq!(
        client
            .query_one("select count(*) from saga_effects where job = $1", &[&id])
            .await?
            .get::<_, i64>(0),
        2
    );

    let result = run_one(
        &mut client,
        "saga-reverse",
        async |job, steps| {
            steps
                .saga(
                    async |_| panic!("forward handler ran during compensation"),
                    async |name, _, tx| {
                        tx.execute(
                            "insert into saga_effects (job, label) values ($1, $2)",
                            &[&job.id, &name],
                        )
                        .await?;
                        if name == "undo-a" {
                            return Err("undo a failed".into());
                        }
                        Ok(json!(null))
                    },
                )
                .await?;
            Ok(json!(null))
        },
        retry,
    )
    .await;
    assert_eq!(result.unwrap_err().to_string(), "undo a failed");
    let result = run_one(
        &mut client,
        "saga-reverse",
        async |job, steps| {
            let outcome = steps
                .saga(
                    async |_| panic!("forward handler ran during compensation"),
                    async |name, receipt, tx| {
                        assert_eq!(name, "undo-a", "already checkpointed undo-b ran again");
                        assert_eq!(*receipt, json!("a"));
                        tx.execute(
                            "insert into saga_effects (job, label) values ($1, $2)",
                            &[&job.id, &name],
                        )
                        .await?;
                        Ok(json!(null))
                    },
                )
                .await?;
            assert_eq!(outcome, SagaOutcome::Compensated(json!("abandoned")));
            // Also exercise a crash/error after compensation completed but before job completion.
            Err("after compensation".into())
        },
        retry,
    )
    .await;
    assert!(result.is_err());
    assert!(
        run_one(
            &mut client,
            "saga-reverse",
            async |_, steps| {
                assert_eq!(
                    steps
                        .saga(
                            async |_| panic!("forward after compensation"),
                            async |_, _, _| panic!("repeated finished compensation")
                        )
                        .await?,
                    SagaOutcome::Compensated(json!("abandoned"))
                );
                Ok(json!(null))
            },
            retry
        )
        .await?
    );
    let labels: Vec<String> = client
        .query_one(
            "select array_agg(label order by seq) from saga_effects where job = $1",
            &[&id],
        )
        .await?
        .get(0);
    assert_eq!(labels, ["a", "b", "undo-b", "undo-a"]);
    assert!(
        client
            .query_one(
                "select completed and saga_phase = 'compensated' from resume.jobs where id = $1",
                &[&id]
            )
            .await?
            .get::<_, bool>(0)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn unknown_forward_effect_blocks_compensation_until_reconciled() -> Result<()> {
    let mut client = connect().await?;
    let id = submit(&client, "saga-unknown", "key", &json!(null)).await?;
    let forward = async |saga: &mut resume::Saga<'_, '_>| {
        saga.step("reserve", "release", async |_| Ok(json!(1)))
            .await?;
        let _ = saga
            .step_once("charge", "refund", async || Err("response lost".into()))
            .await;
        saga.compensate(json!("shipment abandoned")).await?;
        Ok(json!(null))
    };
    let error = run_one(
        &mut client,
        "saga-unknown",
        async |_, steps| {
            steps
                .saga(&forward, async |_, _, _| {
                    panic!("compensated an unknown effect")
                })
                .await?;
            Ok(json!(null))
        },
        |_, _| Retry::Stop,
    )
    .await
    .unwrap_err();
    assert!(message(&error).contains("unknown"));
    assert!(
        client
            .query_one(
                "select paused and saga_phase = 'forward' from resume.jobs where id = $1",
                &[&id]
            )
            .await?
            .get::<_, bool>(0)
    );
    assert_eq!(
        client
            .query_one(
                "select compensation from resume.steps where job_id = $1 and key = 'charge'",
                &[&id]
            )
            .await?
            .get::<_, &str>(0),
        "refund"
    );
    client
        .execute("select resume.resolve_step($1, 'charge', 'null')", &[&id])
        .await?;
    client.execute("select resume.requeue($1)", &[&id]).await?;
    let calls = std::cell::RefCell::new(Vec::new());
    assert!(
        run_one(
            &mut client,
            "saga-unknown",
            async |_, steps| {
                let outcome = steps
                    .saga(&forward, async |name, receipt, _| {
                        if name == "refund" {
                            assert_eq!(*receipt, json!(null));
                        }
                        calls.borrow_mut().push(name.to_owned());
                        Ok(json!(null))
                    })
                    .await?;
                assert_eq!(
                    outcome,
                    SagaOutcome::Compensated(json!("shipment abandoned"))
                );
                Ok(json!(null))
            },
            retry
        )
        .await?
    );
    assert_eq!(*calls.borrow(), ["refund", "release"]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn saga_registration_and_omitted_history_are_checked_before_effects() -> Result<()> {
    let mut client = connect().await?;
    let id = submit(&client, "saga-history", "key", &json!(null)).await?;
    assert!(
        run_one(
            &mut client,
            "saga-history",
            async |_, steps| {
                steps
                    .saga(
                        async |saga| {
                            saga.step("reserve", "release", async |_| Ok(json!(1)))
                                .await?;
                            Err("retry forward".into())
                        },
                        async |_, _, _| panic!("ordinary failure must not compensate"),
                    )
                    .await?;
                Ok(json!(null))
            },
            retry
        )
        .await
        .is_err()
    );
    for omit in [false, true] {
        let error = run_one(
            &mut client,
            "saga-history",
            async |_, steps| {
                steps
                    .saga(
                        async |saga| {
                            if !omit {
                                saga.step("reserve", "changed-release", async |_| {
                                    panic!("changed history ran")
                                })
                                .await?;
                            }
                            saga.compensate(json!("stop")).await?;
                            Ok(json!(null))
                        },
                        async |_, _, _| panic!("invalid history compensated"),
                    )
                    .await?;
                Ok(json!(null))
            },
            retry,
        )
        .await
        .unwrap_err();
        assert!(message(&error).contains(if omit { "omitted" } else { "history differs" }));
    }
    assert!(
        client
            .query_one(
                "select saga_phase = 'forward' from resume.jobs where id = $1",
                &[&id]
            )
            .await?
            .get::<_, bool>(0)
    );
    assert!(
        run_one(
            &mut client,
            "saga-history",
            async |_, steps| {
                steps
                    .saga(
                        async |saga| {
                            saga.step("reserve", "release", async |_| {
                                panic!("repeated saved action")
                            })
                            .await?;
                            // Failure before a regular step commits must not register its undo.
                            assert!(
                                saga.step("failed", "undo-failed", async |_| Err(
                                    "rolled back".into()
                                ))
                                .await
                                .is_err()
                            );
                            saga.compensate(json!("stop")).await?;
                            Ok(json!(null))
                        },
                        async |name, _, _| {
                            assert_eq!(name, "release");
                            Ok(json!(null))
                        },
                    )
                    .await?;
                Ok(json!(null))
            },
            retry
        )
        .await?
    );
    Ok(())
}
