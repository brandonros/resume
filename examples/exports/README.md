# Waiting for an export with Snooze

A mock vendor takes ten seconds to prepare an export. `start_export` saves its ID, then
`wait_for_export` checks readiness once and returns `Snooze(Duration::from_secs(2))` if needed.
The worker handles other runs until the next check is due. When ready, the workflow records
the export's mock download URL.

With the current core schema installed, run from the repository root:

```sh
just exports-schema
just exports-submit slow 10
just exports-submit fast 0
just exports-process
```

`just reset` includes this example's schema, so omit `exports-schema` if you used it.

`fast` finishes while `slow` snoozes. In another terminal, `just exports-show` shows the runs,
claim counts, attempts used, and saved results. Even after several snoozes, each completed
run uses only one attempt; the producer deliberately sets `max_attempts: 1`.

Stop the worker with Ctrl-C after a snooze, then run `just exports-process` again. The waiting
time and export ID survive: `start_export` reuses its saved result. The mock vendor stores its
state through a separate database connection and accepts an idempotency key, so a crash after
starting the export but before saving the ID also avoids creating a second export.
