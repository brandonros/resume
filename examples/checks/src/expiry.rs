//! Claim eligibility does not depend on cleanup having caught up.

use crate::common::{connect, run_is, workflow};

pub(super) async fn cleanup_is_bounded_skips_locks_and_claims_ignore_unswept_runs() {
    let mut client = connect().await;
    let competitor = connect().await;
    let name = workflow("expiry-backlog");
    // Include old versions: cleanup belongs to the workflow, not a worker version.
    client
        .execute(
            "insert into resume.runs (workflow, version, idempotency_key, input, deadline_at)
             select $1, case when n % 2 = 0 then 'old' else '1' end, n::text, '{}',
                    clock_timestamp() - interval '1 second'
             from generate_series(1, 102) n",
            &[&name],
        )
        .await
        .unwrap();
    let tx = client.transaction().await.unwrap();
    let locked: i64 = tx
        .query_one(
            "select id from resume.runs where workflow = $1 order by id limit 1 for update",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    competitor
        .execute("select resume.expire_runs($1)", &[&name])
        .await
        .unwrap();
    let failed: i64 = competitor
        .query_one(
            "select count(*) from resume.runs where workflow = $1 and failed_at is not null",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(failed, 100);
    assert!(run_is(&competitor, locked, "failed_at is null").await);
    tx.rollback().await.unwrap();

    // Direct claims must skip the unswept deadlines and an exhausted final lease.
    competitor
        .execute(
            "insert into resume.runs
             (workflow, version, idempotency_key, input, attempt, attempts_used, leased)
             values ($1, '1', 'exhausted', '{}', 1, 1, true)",
            &[&name],
        )
        .await
        .unwrap();
    assert!(
        competitor
            .query_opt("select id from resume.claim_run($1, '1', 60)", &[&name])
            .await
            .unwrap()
            .is_none()
    );
    competitor
        .execute("select resume.expire_runs($1)", &[&name])
        .await
        .unwrap();
    let remaining: i64 = competitor
        .query_one(
            "select count(*) from resume.runs where workflow = $1 and failed_at is null",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0);
}
