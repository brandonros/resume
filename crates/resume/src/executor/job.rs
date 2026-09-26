use serde_json::Value;
use tokio_postgres::Row;

use crate::Result;

/// Immutable job data supplied to a handler, separate from its mutable execution state.
pub struct Job {
    pub id: i64,
    pub idempotency_key: String,
    pub input: Value,
    attempt: i64,
    attempts_used: i64,
    max_attempts: i32,
}

impl Job {
    pub(crate) fn from_claim(row: &Row) -> Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            idempotency_key: row.try_get("idempotency_key")?,
            input: row.try_get("input")?,
            attempt: row.try_get("attempt")?,
            attempts_used: row.try_get("attempts_used")?,
            max_attempts: row.try_get("max_attempts")?,
        })
    }
    pub(crate) fn attempt(&self) -> i64 {
        self.attempt
    }
    pub(crate) fn attempts_used(&self) -> i64 {
        self.attempts_used
    }
    pub(crate) fn max_attempts(&self) -> i32 {
        self.max_attempts
    }
}
