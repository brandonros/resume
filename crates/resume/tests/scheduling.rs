//! Relative and absolute scheduling through Producer and the SQL claim query. Run with `just test`.

use std::time::{Duration, SystemTime};

use chrono::{DateTime, FixedOffset, Utc};
use resume::Producer;
use serde_json::json;
use tokio_postgres::{Client, NoTls};

async fn connect() -> Client {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL; run the tests with `just test`");
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

fn workflow(name: &str) -> String {
    format!("schedule-{name}-{}", std::process::id())
}

#[tokio::test]
async fn delayed_runs_are_stored_but_only_claimed_when_due() {
    let client = connect().await;
    let competitor = connect().await;
    let name = workflow("delay");

    // Exercise both submission APIs, including a duplicate with a different delay.
    for subject in [None, Some("customer:42")] {
        let key = subject.unwrap_or("plain");
        let producer = Producer::new(&client, &name, "1").delay(Duration::from_secs(300));
        let submitted = match subject {
            None => producer.submit(key, &json!({})).await,
            Some(subject) => producer.submit_for(subject, key, &json!({})).await,
        }
        .unwrap();
        assert!(submitted.created);
        let row = client
            .query_one(
                "select available_at::text, attempt, leased,
                    available_at > clock_timestamp() + interval '4 minutes' as future
             from resume.runs where id = $1",
                &[&submitted.id],
            )
            .await
            .unwrap();
        let schedule: String = row.get(0);
        assert_eq!(row.get::<_, i64>(1), 0);
        assert!(!row.get::<_, bool>(2));
        assert!(row.get::<_, bool>(3));

        let producer = Producer::new(&client, &name, "1").delay(Duration::from_secs(600));
        let duplicate = match subject {
            None => producer.submit(key, &json!({})).await,
            Some(subject) => producer.submit_for(subject, key, &json!({})).await,
        }
        .unwrap();
        assert_eq!(duplicate.id, submitted.id);
        assert!(!duplicate.created);
        assert_eq!(
            client
                .query_one(
                    "select available_at::text from resume.runs where id = $1",
                    &[&submitted.id],
                )
                .await
                .unwrap()
                .get::<_, String>(0),
            schedule
        );
        assert!(
            competitor
                .query_opt("select id from resume.claim_run($1, '1', 60)", &[&name],)
                .await
                .unwrap()
                .is_none()
        );

        // A future run does not prevent a later, immediately eligible run from being claimed.
        let immediate = Producer::new(&client, &name, "1")
            .submit(&format!("immediate-{key}"), &json!({}))
            .await
            .unwrap();
        let claimed = competitor
            .query_one(
                "select id, attempt from resume.claim_run($1, '1', 60)",
                &[&name],
            )
            .await
            .unwrap();
        assert_eq!(claimed.get::<_, i64>(0), immediate.id);
        competitor
            .execute(
                "select resume.complete_run($1, $2)",
                &[&immediate.id, &claimed.get::<_, i64>(1)],
            )
            .await
            .unwrap();

        // Advance eligibility instead of sleeping five minutes, then race two claimers.
        client
            .execute(
                "update resume.runs set available_at = clock_timestamp() - interval '1 second'
             where id = $1",
                &[&submitted.id],
            )
            .await
            .unwrap();
        let (a, b) = tokio::join!(
            async {
                client
                    .query_opt(
                        "select id, attempt from resume.claim_run($1, '1', 60)",
                        &[&name],
                    )
                    .await
            },
            async {
                competitor
                    .query_opt(
                        "select id, attempt from resume.claim_run($1, '1', 60)",
                        &[&name],
                    )
                    .await
            },
        );
        let claims: Vec<_> = [a.unwrap(), b.unwrap()].into_iter().flatten().collect();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].get::<_, i64>(0), submitted.id);
        assert_eq!(claims[0].get::<_, i64>(1), 1, "waiting used up an attempt");
        client
            .execute("select resume.complete_run($1, 1)", &[&submitted.id])
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn earlier_deadline_fails_a_scheduled_run_without_claiming_it() {
    let client = connect().await;
    let name = workflow("deadline");
    let run = Producer::new(&client, &name, "1")
        .delay(Duration::from_secs(7200))
        .deadline(Duration::from_secs(3600))
        .submit("key", &json!({}))
        .await
        .unwrap()
        .id;
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
            "select attempt, failed_at is not null, last_error from resume.runs where id = $1",
            &[&run],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 0);
    assert!(row.get::<_, bool>(1));
    assert_eq!(row.get::<_, String>(2), "the run passed its deadline");
}

#[tokio::test]
async fn submission_rejects_invalid_schedules_and_zero_is_immediately_eligible() {
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
                "select * from resume.submit_run($1, '1', 'key', '{}', 1, 1, 60, null, null, $2, null)",
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
                                            $2, $3::text::timestamptz)",
                &[&name, &delay, &at],
            )
            .await
            .unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "22023");
    }
    assert_eq!(
        client
            .query_one(
                "select count(*) from resume.runs where workflow = $1",
                &[&name],
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );

    let run: i64 = client
        .query_one(
            "select run_id from resume.submit_run($1, '1', 'key', '{}', 1, 1, 60, null, null, 0, null)",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        client
            .query_one("select id from resume.claim_run($1, '1', 60)", &[&name],)
            .await
            .unwrap()
            .get::<_, i64>(0),
        run
    );
}

#[tokio::test]
async fn absolute_times_preserve_offsets_and_duplicate_submissions_keep_the_schedule() {
    let client = connect().await;
    let name = workflow("absolute");
    let future: SystemTime = client
        .query_one("select clock_timestamp() + interval '2 hours'", &[])
        .await
        .unwrap()
        .get(0);
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
            .at(timestamp.parse().unwrap());
        let submitted = match subject {
            Some(subject) => producer.submit_for(subject, &key, &json!({})).await,
            None => producer.submit(&key, &json!({})).await,
        }
        .unwrap();
        let row = client
            .query_one(
                "select available_at, attempt, leased from resume.runs where id = $1",
                &[&submitted.id],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, SystemTime>(0), future);
        assert_eq!(row.get::<_, i64>(1), 0);
        assert!(!row.get::<_, bool>(2));
        assert!(
            client
                .query_opt("select id from resume.claim_run($1, '1', 60)", &[&name],)
                .await
                .unwrap()
                .is_none()
        );

        let producer =
            Producer::new(&client, &name, "1").at((future + Duration::from_secs(3600)).into());
        let duplicate = match subject {
            Some(subject) => producer.submit_for(subject, &key, &json!({})).await,
            None => producer.submit(&key, &json!({})).await,
        }
        .unwrap();
        assert_eq!(duplicate.id, submitted.id);
        assert!(!duplicate.created);
        assert_eq!(
            client
                .query_one(
                    "select available_at from resume.runs where id = $1",
                    &[&submitted.id],
                )
                .await
                .unwrap()
                .get::<_, SystemTime>(0),
            future
        );
    }
}

#[tokio::test]
async fn past_times_are_eligible_and_the_last_schedule_setter_wins() {
    let client = connect().await;
    let name = workflow("past");
    let past = "2000-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let immediate = Producer::new(&client, &name, "1")
        .delay(Duration::from_secs(300))
        .at(past)
        .submit("past", &json!({}))
        .await
        .unwrap();
    let cleared = Producer::new(&client, &name, "1")
        .at("2099-01-01T00:00:00Z".parse().unwrap())
        .delay(Duration::ZERO)
        .submit("zero", &json!({}))
        .await
        .unwrap();
    let future = Producer::new(&client, &name, "1")
        .at(past)
        .delay(Duration::from_secs(300))
        .submit("future", &json!({}))
        .await
        .unwrap();

    for run in [immediate.id, cleared.id] {
        let claimed = client
            .query_one(
                "select id, attempt from resume.claim_run($1, '1', 60)",
                &[&name],
            )
            .await
            .unwrap();
        assert_eq!(claimed.get::<_, i64>(0), run);
        assert_eq!(claimed.get::<_, i64>(1), 1);
        client
            .execute("select resume.complete_run($1, 1)", &[&run])
            .await
            .unwrap();
    }
    assert!(
        client
            .query_opt("select id from resume.claim_run($1, '1', 60)", &[&name],)
            .await
            .unwrap()
            .is_none()
    );
    assert!(client.query_one(
        "select attempt = 0 and available_at > clock_timestamp() from resume.runs where id = $1",
        &[&future.id],
    ).await.unwrap().get::<_, bool>(0));
}

#[tokio::test]
async fn an_absolute_schedule_cannot_postpone_the_deadline() {
    let client = connect().await;
    let name = workflow("absolute-deadline");
    let future: SystemTime = client
        .query_one("select clock_timestamp() + interval '2 hours'", &[])
        .await
        .unwrap()
        .get(0);
    let run = Producer::new(&client, &name, "1")
        .at(future.into())
        .deadline(Duration::from_secs(3600))
        .submit("key", &json!({}))
        .await
        .unwrap()
        .id;
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
    assert!(
        client
            .query_one(
                "select attempt = 0 and failed_at is not null from resume.runs where id = $1",
                &[&run],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
}
