# Runnable regression checks

From the repository root:

```sh
just check
```

This creates a temporary PostgreSQL database, installs the framework schema, runs every
scenario, and drops the database on exit. An assertion failure or timeout makes
the command fail. It does not touch your application database.

The scenarios cover:

- Waiting: completion across retries, terminal outcomes, missing jobs, and timeouts during polling and queries.
- Expiry: bounded cleanup, locked rows, old versions, and claim safety before cleanup.
- Ownership: expired leases, stale attempts, rollback, saved-step replay, and shutdown during a step.
- Errors: contextual wrappers preserve permanent failure and snooze policy; configuration errors stop claiming, while lock timeouts retry.
- Executor and policy: retry fencing and budgets, charged-attempt backoff, atomic submissions, and concurrent duplicate validation.
- Scheduling: delays, absolute timestamps, duplicate submissions, and deadlines.
- Snooze: released locks, saved progress, retry counts, and deadlines.
- Failure handlers: atomic dispatch, terminal failure paths, saved cleanup progress, and lost results.
- Completion and unknown outcomes: rejected completion after a swallowed or unreached step, immediate failure after a step_once error, results kept when cancelled mid-call, and operator resolution.

To run against an existing **disposable database** with both SQL layers installed:

```sh
DATABASE_URL=... cargo run -p checks
```

Each scenario uses unique workflow names and scratch tables in the `checks` schema,
so checks can be rerun on the same database.
