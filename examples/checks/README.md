# Runnable regression checks

From the repository root:

```sh
just check
```

This creates a temporary PostgreSQL database, installs the framework schema, runs all
21 scenarios, and drops the database on exit. An assertion failure or timeout makes
the command fail. It does not touch your application database.

The scenarios cover:

- Ownership: expired leases, stale attempts, rollback, and saved-step replay.
- Scheduling: delays, absolute timestamps, duplicate submissions, and deadlines.
- Snooze: released locks, saved progress, retry counts, and deadlines.
- Failure handlers: atomic dispatch, terminal failure paths, saved cleanup progress, and lost results.

The runner calls ordinary async Rust functions; there is no separate test harness.
To use your own **fresh, disposable database** with the core schema installed, run
`DATABASE_URL=... cargo run -p checks`. These checks write data and expect an empty
database each time.
