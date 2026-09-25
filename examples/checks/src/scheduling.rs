//! Relative and absolute scheduling through Producer and the SQL claim query. Run with `just check`.

use std::time::{Duration, SystemTime};

use chrono::{DateTime, FixedOffset, Utc};
use resume::{Producer, Submitted};
use serde_json::json;
use tokio_postgres::Client;

use crate::common::{claim, complete, connect, make_due, pass_deadline, run_is, workflow};

/// Submits through `submit`, or through `submit_for` when there is a subject.
async fn submit_as(producer: &Producer<'_, Client>, subject: Option<&str>, key: &str) -> Submitted {
    match subject {
        None => producer.submit(key, &json!({})).await,
        Some(subject) => producer.submit_for(subject, key, &json!({})).await,
    }
    .unwrap()
}

async fn available_at(client: &Client, run: i64) -> SystemTime {
    client
        .query_one(
            "select available_at from resume.runs where id = $1",
            &[&run],
        )
        .await
        .unwrap()
        .get(0)
}

/// Two hours from now by the database's clock.
async fn in_two_hours(client: &Client) -> SystemTime {
    client
        .query_one("select clock_timestamp() + interval '2 hours'", &[])
        .await
        .unwrap()
        .get(0)
}

pub(super) async fn delayed_runs_are_stored_but_only_claimed_when_due() {
    let client = connect().await;
    let competitor = connect().await;
    let name = workflow("delay");
    let delayed = |seconds| Producer::new(&client, &name, "1").delay(Duration::from_secs(seconds));

    // Exercise both submission APIs, including a duplicate with a different delay.
    for subject in [None, Some("customer:42")] {
        let key = subject.unwrap_or("plain");
        let submitted = submit_as(&delayed(300), subject, key).await;
        assert!(submitted.created);
        assert!(
            run_is(
                &client,
                submitted.id,
                "attempt = 0 and not leased
                 and available_at > clock_timestamp() + interval '4 minutes'"
            )
            .await
        );
        let schedule = available_at(&client, submitted.id).await;

        let duplicate = submit_as(&delayed(600), subject, key).await;
        assert_eq!(duplicate.id, submitted.id);
        assert!(!duplicate.created);
        assert_eq!(available_at(&client, submitted.id).await, schedule);
        assert!(claim(&competitor, &name).await.is_none());

        // A future run does not prevent a later, immediately eligible run from being claimed.
        let immediate = Producer::new(&client, &name, "1")
            .submit(&format!("immediate-{key}"), &json!({}))
            .await
            .unwrap();
        let (id, attempt) = claim(&competitor, &name).await.unwrap();
        assert_eq!(id, immediate.id);
        complete(&competitor, id, attempt).await.unwrap();

        // Advance eligibility instead of sleeping five minutes, then race two claimers.
        make_due(&client, submitted.id).await;
        let (a, b) = tokio::join!(claim(&client, &name), claim(&competitor, &name));
        let claims: Vec<_> = [a, b].into_iter().flatten().collect();
        assert_eq!(
            claims,
            [(submitted.id, 1)],
            "two claimers won, or waiting used up an attempt"
        );
        complete(&client, submitted.id, 1).await.unwrap();
    }
}

pub(super) async fn earlier_deadline_fails_a_scheduled_run_without_claiming_it() {
    let client = connect().await;
    let name = workflow("deadline");
    let run = Producer::new(&client, &name, "1")
        .delay(Duration::from_secs(7200))
        .deadline(Duration::from_secs(3600))
        .submit("key", &json!({}))
        .await
        .unwrap()
        .id;
    assert!(run_is(&client, run, "available_at = deadline_at").await);
    pass_deadline(&client, run).await;
    assert!(claim(&client, &name).await.is_none());
    assert!(
        run_is(
            &client,
            run,
            "attempt = 0 and failed_at is not null
             and last_error = 'the run passed its deadline'"
        )
        .await
    );
}

pub(super) async fn submission_rejects_invalid_schedules_and_zero_is_immediately_eligible() {
    let client = connect().await;
    let name = workflow("validation");
    for delay in [
        None,
        Some(-1.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
    ] {
        let error = client
            .query_one(
                "select * from resume.submit_run($1, '1', 'key', '{}', 1, 1, 60, null, null, $2, null, null, null)",
                &[&name, &delay],
            )
            .await
            .unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "22023");
    }
    for (delay, at) in [
        (0.0, "infinity"),
        (0.0, "-infinity"),
        (1.0, "2099-01-01T00:00:00Z"),
    ] {
        let error = client
            .query_one(
                "select * from resume.submit_run($1, '1', 'key', '{}', 1, 1, 60, null, null,
                                            $2, $3::text::timestamptz, null, null)",
                &[&name, &delay, &at],
            )
            .await
            .unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "22023");
    }
    let runs: i64 = client
        .query_one(
            "select count(*) from resume.runs where workflow = $1",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(runs, 0, "an invalid submission created a run");

    let run: i64 = client
        .query_one(
            "select run_id from resume.submit_run($1, '1', 'key', '{}', 1, 1, 60, null, null, 0, null, null, null)",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(claim(&client, &name).await.unwrap().0, run);
}

pub(super) async fn absolute_times_preserve_offsets_and_duplicate_submissions_keep_the_schedule() {
    let client = connect().await;
    let name = workflow("absolute");
    let future = in_two_hours(&client).await;
    let utc: DateTime<Utc> = future.into();
    let timestamps = [
        utc.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        utc.with_timezone(&FixedOffset::west_opt(4 * 3600).unwrap())
            .to_rfc3339(),
        utc.with_timezone(&FixedOffset::east_opt(5 * 3600 + 1800).unwrap())
            .to_rfc3339(),
    ];
    // The database session's timezone must not reinterpret an absolute instant.
    client
        .batch_execute("set time zone 'Asia/Tokyo'")
        .await
        .unwrap();
    for (index, timestamp) in timestamps.iter().enumerate() {
        let key = format!("offset-{index}");
        let subject = (index == 1).then_some("customer:42");
        let producer = Producer::new(&client, &name, "1")
            .delay(Duration::from_secs(86400))
            .at(timestamp.parse::<DateTime<Utc>>().unwrap());
        let submitted = submit_as(&producer, subject, &key).await;
        assert_eq!(available_at(&client, submitted.id).await, future);
        assert!(run_is(&client, submitted.id, "attempt = 0 and not leased").await);
        assert!(claim(&client, &name).await.is_none());

        let later = Producer::new(&client, &name, "1").at(future + Duration::from_secs(3600));
        let duplicate = submit_as(&later, subject, &key).await;
        assert_eq!(duplicate.id, submitted.id);
        assert!(!duplicate.created);
        assert_eq!(available_at(&client, submitted.id).await, future);
    }
}

pub(super) async fn past_times_are_eligible_and_the_last_schedule_setter_wins() {
    let client = connect().await;
    let name = workflow("past");
    let producer = || Producer::new(&client, &name, "1");
    let past = "2000-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let immediate = producer()
        .delay(Duration::from_secs(300))
        .at(past)
        .submit("past", &json!({}))
        .await
        .unwrap();
    let cleared = producer()
        .at("2099-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap())
        .delay(Duration::ZERO)
        .submit("zero", &json!({}))
        .await
        .unwrap();
    let future = producer()
        .at(past)
        .delay(Duration::from_secs(300))
        .submit("future", &json!({}))
        .await
        .unwrap();

    for run in [immediate.id, cleared.id] {
        assert_eq!(claim(&client, &name).await, Some((run, 1)));
        complete(&client, run, 1).await.unwrap();
    }
    assert!(claim(&client, &name).await.is_none());
    assert!(
        run_is(
            &client,
            future.id,
            "attempt = 0 and available_at > clock_timestamp()"
        )
        .await
    );
}

pub(super) async fn an_absolute_schedule_cannot_postpone_the_deadline() {
    let client = connect().await;
    let name = workflow("absolute-deadline");
    let run = Producer::new(&client, &name, "1")
        .at(in_two_hours(&client).await)
        .deadline(Duration::from_secs(3600))
        .submit("key", &json!({}))
        .await
        .unwrap()
        .id;
    assert!(run_is(&client, run, "available_at = deadline_at").await);
    pass_deadline(&client, run).await;
    assert!(claim(&client, &name).await.is_none());
    assert!(run_is(&client, run, "attempt = 0 and failed_at is not null").await);
}
