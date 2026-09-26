# resume

A durable workflow engine on PostgreSQL. A producer submits a run with an idempotency key
and JSON input. A worker claims it and executes a Rust handler whose progress is saved in
steps. If the worker crashes, another claims the run once its lease expires and replays the
saved steps, so each step's effect happens once even though the handler runs many times.

```rust
Producer::new(&client, "onboard", "1")
    .submit("customer:42", &json!({"email": "ada@example.com"}))
    .await?;

Worker::new(client, "onboard", "1")
    .run(shutdown_signal(), async |job| {
        let customer = job.step("create", async |tx| create_customer(tx, &job.input).await).await?;
        job.step_once("charge", async || charge_card(&customer).await).await?;
        job.step("welcome", async |tx| queue_email(tx, &customer).await).await?;
        Ok(())
    })
    .await
```

## Layout

The schema has two layers, and the Rust crate mirrors them.

- **Executor** (`sql/executor`, `crates/resume/src/executor.rs`): durable execution. Runs and
  steps, claims and leases, `begin_step` and `save_step`, `complete_run`, `end_attempt`,
  `release_run`, `expire_runs`. It enforces instants and counts (`available_at`, `deadline_at`,
  `max_attempts`) and knows nothing about why they were chosen.
- **Workflow** (`sql/workflow`, `crates/resume/src/workflow/`): policy on top. `submit_workflow`
  turns a retry policy, delay, deadline and failure handler into a run. `fail_attempt` applies
  exponential backoff with jitter. Subjects, cancellation, reopening, failure handlers and the
  `run_status` view live here.

Everything a worker does is a function call on the database, so a handler in another language
could use the same schema.

## Contracts

**Steps commit with their effects.** `job.step(key, action)` runs the action inside a transaction
that also saves the result. Either both commit or neither does. Database writes made through the
step's transaction cannot outlive a lost result. Effects outside the database can repeat, so make
them idempotent: look for the effect first and create it only if missing.

**`step_once` runs its action at most once per run.** The step's start commits before the action
is called. If the attempt dies, or the action times out or returns any error, the run fails at
once: the outcome is unknown, and no later attempt will call the action again. `run_status` and
`step_status` show `needs_resolution`. An operator checks the effect and records the result with
`resume.reopen_run(run, key, output)`, and the next attempt replays it. Do readiness checks and
anything that may legitimately fail in a regular step before the `step_once`.

**Handlers must be deterministic.** Given the same input and the same saved step outputs, a
handler must call the same step keys in the same order. Every claim replays the handler from the
start, and the engine checks history against code: a key at a different position, a different key
at a recorded position, a recorded step the attempt never reached, or a `step_once` row without a
result all fail the run permanently with SQLSTATE `RS001`. Branch on `job.input` and on step
outputs, never on the clock, randomness, or reads outside a step. To change a workflow's steps,
bump the version: producers submit for a version and only workers of that version claim it.

**Failure is graded.** An ordinary error ends the attempt and the run retries after a backoff,
up to `max_attempts`. `Permanent` fails the run at once. `Snooze` releases the run for a while
without charging an attempt, for waiting on something external. A crashed worker's run is
reclaimed once its lease expires, and that counts as an attempt. When the budget is spent, or
the error is permanent, the run fails and its failure handler, if any, is queued in the same
transaction.

**Deadlines and cancellation stop new work, not running work.** A run past its deadline fails
the next time a worker polls if it is waiting, or when it starts its next step if it is in
progress. A step already running finishes and saves, and a run whose last step had started can
still complete. Cancellation behaves the same way: the run stops at its next step, and a
`step_once` action in flight still records the result it received, so there is nothing to
resolve.

## Operating

`resume.run_status` and `resume.step_status` are the views to watch. The `justfile` wraps the
operator calls:

```sh
just cancel-run 7                       # stop a run that has not finished
just reopen-run 7                       # requeue a failed run with fresh attempts
just resolve-step 7 charge '{"id": 9}'  # record an interrupted step_once result, then requeue
```

Reopening a run that has a `step_once` with an unknown outcome is refused until the outcome
is supplied. A run that dispatched a failure handler cannot be reopened; reopen the handler.

## Running it

The Postgres tools must be on `PATH`. `RESUME_SERVER` names the server (default
`postgresql://localhost:5432`).

```sh
just reset   # drop and recreate the resume database with every schema
just check   # run the regression scenarios in examples/checks on a temporary database
```

The examples each show one thing: `onboard` (crash and chaos testing with invariants),
`checkout` (failure handlers), `provision` (resource locks under quota), `shipping` (check and
act in one step), `subscription` (newest request wins with `is_latest`), `tickets`
(`step_once`), and `waiting` (submit and wait for the outcome). Each has a README or a
`just` recipe.
