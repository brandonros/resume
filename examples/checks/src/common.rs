//! Shared scenario helpers. Unique workflow names isolate each scenario's runs.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use resume::Producer;
use serde_json::json;
use tokio_postgres::{Client, Error};

pub async fn connect() -> Client {
    let url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL; run the checks with `just check`");
    harness::connect(&url).await.unwrap()
}

/// A workflow name no other scenario uses.
pub fn workflow(name: &str) -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    format!("{name}-{}-{n}", std::process::id())
}

/// Submits a run of version 1 with the key "key" and the default settings. Returns its ID.
pub async fn submit(client: &Client, workflow: &str) -> i64 {
    Producer::new(client, workflow, "1")
        .submit("key", &json!({}))
        .await
        .unwrap()
        .id
}

/// Sweeps expired runs, then claims a run of version 1 with a 60 second lease. Returns its ID and attempt.
pub async fn claim(client: &Client, workflow: &str) -> Option<(i64, i64)> {
    claim_with(client, workflow, "1", 60.0).await
}

pub async fn claim_with(
    client: &Client,
    workflow: &str,
    version: &str,
    lease_seconds: f64,
) -> Option<(i64, i64)> {
    client
        .execute("select resume.expire_runs($1)", &[&workflow])
        .await
        .unwrap();
    client
        .query_opt(
            "select id, attempt from resume.claim_run($1, $2, $3)",
            &[&workflow, &version, &lease_seconds],
        )
        .await
        .unwrap()
        .map(|row| (row.get(0), row.get(1)))
}

pub async fn complete(client: &Client, run: i64, attempt: i64) -> Result<u64, Error> {
    client
        .execute("select resume.complete_run($1, $2)", &[&run, &attempt])
        .await
}

pub async fn end_attempt(
    client: &Client,
    run: i64,
    attempt: i64,
    error: &str,
    permanent: bool,
) -> Result<u64, Error> {
    client
        .execute(
            "select resume.end_attempt($1, $2, $3, $4)",
            &[&run, &attempt, &error, &permanent],
        )
        .await
}

/// Evaluates `condition` on the run's row in resume.runs.
pub async fn run_is(client: &Client, run: i64, condition: &str) -> bool {
    client
        .query_one(
            &format!("select ({condition}) from resume.runs where id = $1"),
            &[&run],
        )
        .await
        .unwrap()
        .get(0)
}

/// Waits up to 10 seconds for `condition` to hold on the run's row.
pub async fn wait_for(client: &Client, run: i64, condition: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !run_is(client, run, condition).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("run {run} never reached {condition}"));
}

/// Makes the run eligible now, instead of sleeping until it is.
pub async fn make_due(client: &Client, run: i64) {
    client
        .execute(
            "update resume.runs set available_at = clock_timestamp() - interval '1 second'
             where id = $1",
            &[&run],
        )
        .await
        .unwrap();
}

/// Moves the run's deadline into the past, and makes it eligible so expiry cleanup can fail it.
pub async fn pass_deadline(client: &Client, run: i64) {
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
}
