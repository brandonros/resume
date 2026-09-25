# Submit a job and wait for its outcome

This example follows `Producer -> JobHandle -> JobOutcome`. The producer submits a
job and waits with a timeout; a worker in another terminal processes it using a
durable step. Only the core resume schema is needed.

Use a disposable local database. From the repository root, `just reset` drops and
recreates the example `resume` database and installs its schemas.

Start the worker in one terminal:

```sh
just waiting-process
```

In another terminal, try completion, permanent failure, and a short wait timeout:

```sh
just waiting-submit demo-success
just waiting-submit demo-failure fail
just waiting-submit demo-slow slow 100
```

The slow job takes two seconds to execute. Its 100 ms wait timeout does not cancel
it. Wait again with the same key and input:

```sh
just waiting-submit demo-slow slow 5000
```

The handle reports `created: false`, refers to the same job, and observes its
completion. Completed jobs can be submitted again with the same input to observe
their existing outcome. Use a new key to execute a fresh job.

The central API calls are:

```rust
let handle: JobHandle = Producer::new(&client, "waiting", "1")
    .submit(&key, &json!({"mode": mode}))
    .await?;
let outcome: JobOutcome = handle.wait(&client, Duration::from_secs(5)).await?;
```

`src/main.rs` handles each outcome and distinguishes a wait timeout from other
errors. `Completed` reports completion, not the JSON output of the step.

To observe cancellation, stop the worker, submit a fresh key with a long timeout,
and run `just cancel-run <id>` in another terminal using the printed job ID.

Without `just`, set `DATABASE_URL` and use `cargo run -p waiting -- work` or
`cargo run -p waiting -- submit <key> [success|fail|slow] [timeout-ms]`.
