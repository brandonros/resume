use tokio_postgres::Transaction;

use crate::Result;
use crate::executor::Job;

impl Job {
    /// Whether no newer run of this workflow has the same subject (see `Producer::submit_for`),
    /// for use in a step. Locks the subject until the step's transaction ends, so a newer run's
    /// step waits for this one to commit and applies after it.
    pub async fn is_latest(&self, tx: &Transaction<'_>) -> Result<bool> {
        Ok(tx
            .query_one("select resume.lock_subject($1)", &[&self.id])
            .await?
            .try_get(0)?)
    }
}
