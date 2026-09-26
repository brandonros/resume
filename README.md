# resume

A minimal durable workflow engine on PostgreSQL.

## Contract

- Submit a job with input and an idempotency key. Within a workflow, submitting the
  same key and input returns the existing job; different input is rejected.
- A worker claims a job with a lease and an attempt token. An abandoned job becomes
  eligible again when its lease expires. Stale attempts cannot record progress.
- A handler executes sequential steps with unique, stable names. On retry, it runs
  again from the beginning and follows the same steps using saved results.
- A completed step returns its saved output without running its action again.
- A new step runs in a database transaction. Changes made through that transaction
  and the step's output commit together, or neither commits.
- A handler that returns successfully completes the job. Completed jobs are not
  claimed again, and their idempotency keys remain reserved.
- A failed attempt makes the job eligible for another attempt after a fixed delay.
- External effects may repeat. They must tolerate repetition, typically through
  vendor idempotency keys. The engine does not guarantee exactly-once external effects.
