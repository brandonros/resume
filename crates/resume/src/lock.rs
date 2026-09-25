use tokio_postgres::Transaction;

use crate::Result;

/// Locks `resource` (for example "customer:42") until the step's transaction ends, so steps
/// naming the same resource run one at a time, even across runs and workflows. A crashed
/// worker's lock is released when its connection closes. Take it before checking state, so
/// the check and the action it guards happen under the same lock.
pub async fn lock_resource(tx: &Transaction<'_>, resource: &str) -> Result<()> {
    tx.execute("select resume.lock_resource($1)", &[&resource])
        .await?;
    Ok(())
}
