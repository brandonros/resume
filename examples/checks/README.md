# Runnable regression checks

From the repository root:

```sh
just check
```

This creates a temporary PostgreSQL database, installs the framework schema, runs all
26 scenarios, and drops the database on exit. An assertion failure or timeout makes
the command fail. It does not touch your application database.

The scenarios cover:

- Ownership: expired leases, stale attempts, rollback, and saved-step replay.
- Scheduling: delays, absolute timestamps, duplicate submissions, and deadlines.
- Snooze: released locks, saved progress, retry counts, and deadlines.
- Failure handlers: atomic dispatch, terminal failure paths, saved cleanup progress, and lost results.

To run against an existing **disposable database** with the core schema installed:

```sh
DATABASE_URL=... cargo run -p checks
```

Each scenario uses unique workflow names and scratch tables in the `checks` schema,
so checks can be rerun on the same database.
