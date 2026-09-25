use std::time::Duration;

use resume::{JobHandle, JobOutcome, Producer};
use serde_json::json;

use crate::common::*;

pub async fn timeout_leaves_job_available_and_wait_observes_completion() {
    let client = connect().await;
    let processor = connect().await;
    let workflow = workflow("wait-completion");
    let submitted = Producer::new(&client, &workflow, "1")
        .submit("key", &json!({}))
        .await
        .unwrap();

    let error = submitted
        .wait(&client, Duration::from_millis(20))
        .await
        .unwrap_err();
    assert!(error.is::<tokio::time::error::Elapsed>());
    assert!(run_is(&client, submitted.id, "not leased and failed_at is null").await);

    let (id, attempt) = claim(&processor, &workflow).await.unwrap();
    // A retry is not a terminal failure, even though last_error is set.
    fail_attempt(&processor, id, attempt, "temporary", false)
        .await
        .unwrap();
    let (outcome, ()) = tokio::join!(submitted.wait(&client, Duration::from_secs(5)), async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        make_due(&processor, id).await;
        let (_, attempt) = claim(&processor, &workflow).await.unwrap();
        complete(&processor, id, attempt, 0).await.unwrap();
    });
    assert_eq!(outcome.unwrap(), JobOutcome::Completed);

    let duplicate = Producer::new(&client, &workflow, "1")
        .submit("key", &json!({}))
        .await
        .unwrap();
    assert!(!duplicate.created);
    assert_eq!(
        duplicate
            .wait(&client, Duration::from_secs(1))
            .await
            .unwrap(),
        JobOutcome::Completed
    );
}

pub async fn wait_reports_failure_cancellation_missing_jobs_and_query_timeout() {
    let client = connect().await;
    let mut locker = connect().await;
    let workflow = workflow("wait-outcomes");
    let producer = Producer::new(&client, &workflow, "1");
    let failed = producer.submit("failed", &json!({})).await.unwrap();
    let (id, attempt) = claim(&client, &workflow).await.unwrap();
    fail_attempt(&client, id, attempt, "invalid input", true)
        .await
        .unwrap();
    assert_eq!(
        failed.wait(&client, Duration::from_secs(1)).await.unwrap(),
        JobOutcome::Failed {
            error: Some("invalid input".into())
        }
    );

    let cancelled = producer.submit("cancelled", &json!({})).await.unwrap();
    client
        .execute("select resume.cancel_run($1)", &[&cancelled.id])
        .await
        .unwrap();
    assert!(matches!(
        cancelled
            .wait(&client, Duration::from_secs(1))
            .await
            .unwrap(),
        JobOutcome::Cancelled { .. }
    ));

    let missing = JobHandle {
        id: -1,
        created: false,
    };
    let error = missing
        .wait(&client, Duration::from_secs(1))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("does not exist"));

    // Bound the query itself, not just the sleep between queries.
    let tx = locker.transaction().await.unwrap();
    tx.batch_execute("lock table resume.runs in access exclusive mode")
        .await
        .unwrap();
    let error = failed
        .wait(&client, Duration::from_millis(20))
        .await
        .unwrap_err();
    assert!(error.is::<tokio::time::error::Elapsed>());
    tx.rollback().await.unwrap();
    assert!(matches!(
        failed.wait(&client, Duration::from_secs(1)).await.unwrap(),
        JobOutcome::Failed { .. }
    ));
}
