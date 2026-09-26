use serde_json::Value;
use tokio_postgres::Transaction;

use crate::{Result, Steps};

/// A successful forward result, or the persisted reason for completed compensation.
#[derive(Debug, PartialEq)]
pub enum SagaOutcome {
    Completed(Value),
    Compensated(Value),
}

/// Control flow returned after the compensation decision commits. Propagate with `?`.
#[derive(Debug)]
pub struct Compensating;
impl std::fmt::Display for Compensating {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("saga is compensating")
    }
}
impl std::error::Error for Compensating {}

/// Forward execution of one sequential saga. Compensation names are durable handler IDs.
/// Keep them compatible for existing jobs. Keys beginning with `$undo:` are reserved.
pub struct Saga<'a, 'db> {
    steps: &'a mut Steps<'db>,
    compensating: bool,
    reason: Value,
}

impl Saga<'_, '_> {
    /// Atomically save a database action, its receipt, and its compensation name.
    pub async fn step(
        &mut self,
        key: &str,
        compensation: &str,
        action: impl AsyncFnOnce(&Transaction<'_>) -> Result<Value>,
    ) -> Result<Value> {
        self.check_forward()?;
        self.steps
            .step_registered(key, Some(compensation), false, action)
            .await
    }

    /// Commit an external action's start and compensation name before invoking it once.
    /// An unknown outcome blocks compensation until `resolve_step` records a verified receipt.
    pub async fn step_once(
        &mut self,
        key: &str,
        compensation: &str,
        action: impl AsyncFnOnce() -> Result<Value>,
    ) -> Result<Value> {
        self.check_forward()?;
        self.steps
            .once_registered(key, Some(compensation), action)
            .await
    }

    /// Save a result that requires no compensation, such as a confirmed rejection.
    pub async fn checkpoint(
        &mut self,
        key: &str,
        action: impl AsyncFnOnce(&Transaction<'_>) -> Result<Value>,
    ) -> Result<Value> {
        self.check_forward()?;
        self.steps.step(key, action).await
    }

    /// Persist the decision before rollback starts, then return `Compensating`.
    /// Unknown or omitted saved steps reject the decision. Ordinary errors do not initiate it.
    pub async fn compensate(&mut self, reason: Value) -> Result<()> {
        self.check_forward()?;
        self.steps
            .client
            .execute(
                "select resume.compensate($1, $2, $3, $4)",
                &[
                    &self.steps.id,
                    &self.steps.attempt,
                    &self.steps.position,
                    &reason,
                ],
            )
            .await?;
        self.compensating = true;
        self.reason = reason;
        Err(Compensating.into())
    }

    fn check_forward(&self) -> Result<()> {
        if self.compensating {
            Err(Compensating.into())
        } else {
            Ok(())
        }
    }
}

impl Steps<'_> {
    /// Run one sequential saga per job, before any other steps.
    ///
    /// `forward` registers compensation names alongside saved receipts. It explicitly calls
    /// `Saga::compensate` to abandon fulfillment. Errors otherwise follow the usual retry policy.
    /// Once compensating, retries skip `forward` and call `undo(name, receipt, transaction)` in
    /// reverse order. Each compensation is checkpointed with its database effects; external
    /// compensations must be idempotent because they can repeat after an uncertain commit.
    /// Unknown forward `step_once` outcomes require operator reconciliation first.
    ///
    /// The undo handler must recognize every durable compensation name; return an error for
    /// unknown names. No automatic compensation is provided for child workflows or nested sagas.
    pub async fn saga(
        &mut self,
        forward: impl AsyncFnOnce(&mut Saga<'_, '_>) -> Result<Value>,
        undo: impl AsyncFn(&str, &Value, &Transaction<'_>) -> Result<Value>,
    ) -> Result<SagaOutcome> {
        if self.position != 0 || self.saga_started || self.suspended {
            return Err("one saga is allowed per job, before other steps".into());
        }
        self.saga_started = true;
        let row = self
            .client
            .query_one(
                "select * from resume.begin_saga($1, $2)",
                &[&self.id, &self.attempt],
            )
            .await?;
        let phase: &str = row.try_get("saga_phase")?;
        let mut reason: Value = row
            .try_get::<_, Option<Value>>("saga_result")?
            .unwrap_or(Value::Null);
        if phase != "forward" {
            self.position = row.try_get("saga_position")?;
        }
        match phase {
            "completed" => return Ok(SagaOutcome::Completed(reason)),
            "compensated" => return Ok(SagaOutcome::Compensated(reason)),
            "forward" => {
                let mut saga = Saga {
                    steps: self,
                    compensating: false,
                    reason: Value::Null,
                };
                let result = forward(&mut saga).await;
                if !saga.compensating {
                    let output = result?;
                    self.end_saga(&output).await?;
                    return Ok(SagaOutcome::Completed(output));
                }
                reason = saga.reason;
            }
            "compensating" => {}
            _ => return Err("invalid saga phase".into()),
        }
        let actions = self
            .client
            .query(
                "select position, compensation, output from resume.steps
            where job_id = $1 and compensation is not null order by position desc",
                &[&self.id],
            )
            .await?;
        for action in actions {
            let position: i32 = action.try_get(0)?;
            let name: &str = action.try_get(1)?;
            let receipt: Value = action.try_get(2)?;
            let key = format!("$undo:{position}:{name}");
            self.step_registered(&key, None, true, async |tx| undo(name, &receipt, tx).await)
                .await?;
        }
        self.end_saga(&reason).await?;
        Ok(SagaOutcome::Compensated(reason))
    }

    async fn end_saga(&mut self, output: &Value) -> Result<()> {
        self.client
            .execute(
                "select resume.end_saga($1, $2, $3, $4)",
                &[&self.id, &self.attempt, &self.position, output],
            )
            .await?;
        Ok(())
    }
}
