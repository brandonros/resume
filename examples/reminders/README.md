# Scheduling a reminder with Producer::delay or Producer::at

`Producer::delay(Duration::from_secs(10)).submit(...)` inserts a run immediately, with an
eligibility time ten seconds ahead. Workers exclude it from claims until then. Once claimed,
a regular step delivers the reminder to a local database inbox.

After `just reset`, which installs every schema, run from the repository root:

```sh
just reminders-submit later 10 "Time to stretch"
just reminders-submit now 0 "This one is ready immediately"
just reminders-process
```

`now` is delivered first. In another terminal, `just reminders-show` shows `later` waiting
with zero claims until its scheduled time. No step is running during that wait.

Stop and restart the worker to see that the schedule survives. Submitting the same key and
message again keeps the original run and time, even if a different delay is supplied. Each
reminder creates one inbox row, committed together with the step's saved result.

For a specific time, use an ISO 8601 timestamp with `Z` or an explicit UTC offset:

```sh
just reminders-submit-at midnight "2026-09-26T00:00:00-04:00" "Midnight reminder"
just reminders-process
```

The corresponding Rust API is:

```rust
Producer::new(&client, "reminders", VERSION)
    .at("2026-09-26T00:00:00-04:00".parse()?)
    .submit("midnight", &json!({"message": "Midnight reminder"}))
    .await?;
```

The timestamp makes the run eligible at that instant; a worker may pick it up later. A past
timestamp is immediately eligible. The last call to `at` or `delay` wins, and any deadline
still counts from submission. This schedules one run, without recurring or cron behavior.
