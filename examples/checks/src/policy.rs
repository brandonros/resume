//! Executor transitions and the submission/retry policies layered on them. Run with `just check`.

use std::time::Duration;

use resume::{Producer, RetryPolicy};
use serde_json::{Value, json};
use tokio_postgres::{Client, Error};

use crate::common::{claim, connect, make_due, run_is, workflow};

/// Ends the attempt through the executor with the given retry delay. Returns whether it retries.
async fn end(
    client: &Client,
    run: i64,
    attempt: i64,
    retry_after_seconds: Option<f64>,
) -> Result<bool, Error> {
    client
        .query_one(
            "select resume.end_attempt($1, $2, 'failed action', $3)",
            &[&run, &attempt, &retry_after_seconds],
        )
        .await
        .map(|row| row.get(0))
}

async fn snapshot(client: &Client, run: i64) -> Value {
    client
        .query_one(
            "select to_jsonb(r) from resume.runs r where id = $1",
            &[&run],
        )
        .await
        .unwrap()
        .get(0)
}

pub(super) async fn executor_finish_enforces_ownership_budget_and_deadline() {
    let client = connect().await;
    let name = workflow("executor-policy");
    let row = client
        .query_one(
            "select * from resume.submit_run($1, '1', 'key', '{}', 2,
                clock_timestamp() + interval '1 hour', clock_timestamp())",
            &[&name],
        )
        .await
        .unwrap();
    let run: i64 = row.get("run_id");
    assert!(row.get::<_, bool>("created"));
    let (_, first) = claim(&client, &name).await.unwrap();

    for invalid in [-1.0, f64::NEG_INFINITY, f64::INFINITY] {
        assert!(end(&client, run, first, Some(invalid)).await.is_err());
    }
    assert!(
        run_is(
            &client,
            run,
            "leased and attempts_used = 1 and last_error is null"
        )
        .await
    );
    assert!(end(&client, run, first, Some(7200.0)).await.unwrap());
    assert!(
        run_is(
            &client,
            run,
            "not leased and attempts_used = 1 and failed_at is null
             and available_at = deadline_at and last_error = 'failed action'"
        )
        .await
    );
    assert!(end(&client, run, first, None).await.is_err());

    make_due(&client, run).await;
    let (_, second) = claim(&client, &name).await.unwrap();
    assert!(end(&client, run, first, None).await.is_err());
    assert!(!end(&client, run, second, Some(0.0)).await.unwrap());
    assert!(run_is(&client, run, "failed_at is not null and attempts_used = 2").await);
    assert!(claim(&client, &name).await.is_none());

    let permanent: i64 = client
        .query_one(
            "select run_id from resume.submit_run($1, '1', 'permanent', '{}', 3)",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    let (_, attempt) = claim(&client, &name).await.unwrap();
    assert!(!end(&client, permanent, attempt, None).await.unwrap());
    assert!(
        run_is(
            &client,
            permanent,
            "failed_at is not null and attempts_used = 1"
        )
        .await
    );

    let scheduled: i64 = client
        .query_one(
            "select run_id from resume.submit_run($1, '1', 'scheduled', '{}', 3,
                clock_timestamp() + interval '1 hour', clock_timestamp() + interval '2 hours')",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        run_is(
            &client,
            scheduled,
            "available_at = deadline_at and attempt = 0"
        )
        .await
    );
}

pub(super) async fn retry_policy_uses_charged_attempts_and_caps_backoff() {
    let client = connect().await;
    let name = workflow("retry-policy");
    let run = Producer::new(&client, &name, "1")
        .retry(RetryPolicy {
            max_attempts: 4,
            delay: Duration::from_secs(8),
            max_delay: Duration::from_secs(12),
        })
        .submit("key", &json!({}))
        .await
        .unwrap()
        .id;
    let (_, attempt) = claim(&client, &name).await.unwrap();
    client
        .execute("select resume.release_run($1, $2, 0)", &[&run, &attempt])
        .await
        .unwrap();

    // Releasing refunded the first claim. The attempt token is now ahead of charged attempts.
    for (used, lower, upper) in [(1, 4.0, 8.0), (2, 6.0, 12.0), (3, 6.0, 12.0)] {
        let (_, attempt) = claim(&client, &name).await.unwrap();
        assert_eq!(attempt, used + 1);
        let delay: Option<f64> = client
            .query_one(
                "select resume.fail_attempt($1, $2, 'retry me', false)",
                &[&run, &attempt],
            )
            .await
            .unwrap()
            .get(0);
        assert!((lower..=upper).contains(&delay.unwrap()));
        assert!(
            run_is(
                &client,
                run,
                &format!(
                    "not leased and failed_at is null and attempts_used = {used}
                          and available_at > clock_timestamp() and last_error = 'retry me'"
                )
            )
            .await
        );
        make_due(&client, run).await;
    }
    let (_, attempt) = claim(&client, &name).await.unwrap();
    let terminal: Option<f64> = client
        .query_one(
            "select resume.fail_attempt($1, $2, 'budget spent', false)",
            &[&run, &attempt],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(terminal, None);
    assert!(
        run_is(
            &client,
            run,
            "attempt = 5 and attempts_used = 4 and failed_at is not null"
        )
        .await
    );
}

pub(super) async fn submission_is_atomic_and_duplicate_keeps_winning_policy() {
    let mut client = connect().await;
    let competitor = connect().await;
    let name = workflow("submission-policy");

    let tx = client.transaction().await.unwrap();
    let rolled_back = Producer::new(&tx, &name, "1")
        .on_failure("cleanup", "2")
        .submit_for("customer:42", "key", &json!({}))
        .await
        .unwrap();
    assert!(
        competitor
            .query_opt(
                "select id from resume.runs where id = $1",
                &[&rolled_back.id]
            )
            .await
            .unwrap()
            .is_none()
    );
    tx.rollback().await.unwrap();
    assert!(
        competitor
            .query_opt(
                "select id from resume.runs where id = $1",
                &[&rolled_back.id]
            )
            .await
            .unwrap()
            .is_none()
    );

    let tx = client.transaction().await.unwrap();
    let winner = Producer::new(&tx, &name, "1")
        .retry(RetryPolicy {
            max_attempts: 7,
            delay: Duration::from_secs(3),
            max_delay: Duration::from_secs(7),
        })
        .delay(Duration::from_secs(50))
        .deadline(Duration::from_secs(100))
        .on_failure("cleanup", "2")
        .submit_for("customer:42", "key", &json!({}))
        .await
        .unwrap();
    assert!(winner.created);
    let original: Value = tx
        .query_one(
            "select to_jsonb(r) from resume.runs r where id = $1",
            &[&winner.id],
        )
        .await
        .unwrap()
        .get(0);
    let other = Producer::new(&competitor, &name, "1")
        .deadline(Duration::from_secs(1000))
        .on_failure("different-cleanup", "3");
    let input = json!({});
    let duplicate = other.submit_for("customer:42", "key", &input);
    tokio::pin!(duplicate);
    tokio::select! {
        _ = &mut duplicate => panic!("duplicate returned before the winner committed"),
        () = tokio::time::sleep(Duration::from_millis(20)) => {}
    }
    tx.commit().await.unwrap();
    let duplicate = duplicate.await.unwrap();
    assert!(!duplicate.created);
    assert_eq!(duplicate.id, winner.id);
    assert_eq!(snapshot(&competitor, winner.id).await, original);
    assert!(
        run_is(
            &competitor,
            winner.id,
            "subject = 'customer:42' and max_attempts = 7
             and retry_delay = interval '3 seconds' and retry_max_delay = interval '7 seconds'
             and on_failure_workflow = 'cleanup' and on_failure_version = '2'
             and deadline_at - available_at = interval '50 seconds'"
        )
        .await
    );
}

pub(super) async fn new_policy_constraints_and_duplicate_identity() {
    let client = connect().await;
    let name = workflow("duplicate-policy");
    for invalid in [
        "p_max_attempts => 0",
        "p_retry_delay_seconds => 0",
        "p_retry_delay_seconds => null",
        "p_retry_max_delay_seconds => 0",
        "p_delay_seconds => -1",
        "p_subject => ''",
        "p_on_failure_workflow => 'cleanup'",
        "p_on_failure_workflow => '', p_on_failure_version => '1'",
    ] {
        let sql =
            format!("select * from resume.submit_workflow($1, '1', 'invalid', '{{}}', {invalid})");
        assert!(
            client.query_one(&sql, &[&name]).await.is_err(),
            "accepted {invalid}"
        );
        let runs: i64 = client
            .query_one(
                "select count(*) from resume.runs where workflow = $1",
                &[&name],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(runs, 0, "invalid policy left the executor's insert behind");
    }

    let run = Producer::new(&client, &name, "1")
        .on_failure("cleanup", "2")
        .submit_for("customer:42", "key", &json!({}))
        .await
        .unwrap()
        .id;
    let original = snapshot(&client, run).await;
    // Settings are applied only on creation; duplicates ignore even unusable retry/handler values.
    for ignored in [
        "p_retry_delay_seconds => 0",
        "p_retry_delay_seconds => null",
        "p_retry_max_delay_seconds => 0",
        "p_on_failure_workflow => 'cleanup'",
        "p_on_failure_workflow => '', p_on_failure_version => '1'",
    ] {
        let sql = format!(
            "select * from resume.submit_workflow($1, '1', 'key', '{{}}',
             p_subject => 'customer:42', {ignored})"
        );
        let duplicate = client.query_one(&sql, &[&name]).await.unwrap();
        assert_eq!(duplicate.get::<_, i64>("run_id"), run);
        assert!(!duplicate.get::<_, bool>("created"));
        assert_eq!(snapshot(&client, run).await, original);
    }

    for sql in [
        "select * from resume.submit_workflow($1, '1', 'key', '{}', p_subject => 'customer:43')",
        "select * from resume.submit_workflow($1, '2', 'key', '{}', p_subject => 'customer:42')",
        "select * from resume.submit_workflow($1, '1', 'key', '42', p_subject => 'customer:42')",
        "select * from resume.submit_run($1, '2', 'key', '{}')",
        "select * from resume.submit_run($1, '1', 'key', '42')",
    ] {
        let error = client.query_one(sql, &[&name]).await.unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "23505");
    }
    let duplicate = client
        .query_one(
            "select * from resume.submit_run($1, '1', 'key', '{}', 9,
             clock_timestamp() + interval '1 hour', clock_timestamp() + interval '2 hours')",
            &[&name],
        )
        .await
        .unwrap();
    assert_eq!(duplicate.get::<_, i64>("run_id"), run);
    assert!(!duplicate.get::<_, bool>("created"));
    assert_eq!(snapshot(&client, run).await, original);
}
