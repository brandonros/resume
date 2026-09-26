use tokio_postgres::Transaction;

use crate::Result;

/// Locks `resource` (for example "customer:42") until the step's transaction ends, so steps
/// naming the same resource run one at a time, even across runs and workflows. A crashed
/// worker's lock is released when its connection closes. Take it before checking state, so
/// the check and the action it guards happen under the same lock. Names are hashed to 64 bits,
/// so two names sharing a lock is possible but very unlikely.
pub async fn lock_resource(tx: &Transaction<'_>, resource: &str) -> Result<()> {
    tx.execute(
        "select pg_advisory_xact_lock(hashtextextended($1, 0))",
        &[&resource],
    )
    .await?;
    Ok(())
}
