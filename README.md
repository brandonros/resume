# resume

A small durable workflow and saga engine for C# and SQL Server 2022+. Targets net8.0 and net10.0.

## Install

```csharp
await Schema.InstallAsync(connection);
```

This creates or upgrades the `resume` schema. You can run it again safely, and several hosts can run it at
the same time. `resume.schema_version` records the installed version. Upgrades only go forward. The installer
fails with error 50006 when the database is newer than the library. The same script ships as
`content/schema.sql` in the package, for DBA-run deployments.

## Running jobs

```csharp
var id = await Workflow.SubmitAsync(db, "dormant-init", requestKey, Json.Value(new { accountRef }));

// Run that job now, in the caller. If the caller dies, a WorkAsync worker on "dormant-init"
// claims it after its lease expires.
await Workflow.RunJobAsync(connection, "dormant-init", id, handler, retry);
var job = await Workflow.GetJobAsync(db, id); // Status, Output, LastError, Failures, UnresolvedSteps
```

`RunJobAsync` returns false when the job is not ready. That includes a job that is leased, scheduled,
waiting, paused, completed or in a different workflow. A background worker can claim the job between submit
and run. In that case `RunJobAsync` returns false, and you can check `GetJobAsync` later.

`RunOneAsync` and `WorkAsync` claim any ready job of a workflow. Use one dedicated connection per worker.

## The handler must be replayable

A handler can run many times, on any worker. Each time it must be able to rebuild all of its context from
`job.Input` and from earlier step outputs. Do not capture request objects in the closure. A worker that
resumes the job does not have them. Load that data again from the references in the input on every run.
Put a value in a step output only when a replay must get the same value again, such as a message id or a
timestamp:

```csharp
var account = await accounts.LoadAsync(job.Input.GetProperty("accountRef").GetString()!, ct); // not stored
var messageId = await steps.StepAsync("message-id", (db, ct) => Task.FromResult(Json.Value(Guid.NewGuid())));
```

Step keys and their order are durable history. If a replay calls a different step at a given position, the
job pauses with "Step history differs".

## Step kinds

| Call | Guarantee |
| --- | --- |
| `StepAsync(key, (db, ct) => ...)` | The saved output commits in one transaction with every write made through `db`. On failure, both roll back. |
| `StepOnceAsync(key, ct => ...)` | Runs at most once. An exception leaves the outcome unknown, and the job pauses until an operator calls `ResolveStepAsync` and then `RequeueAsync`. |

**`StepAsync` is transactional only for writes on resume's own connection.** For those writes to be atomic,
they must go to the same database and use `db.Connection` and `db.Transaction` (for ADO.NET or Dapper, pass
both to each command). A repository that opens its own `SqlConnection` is outside the transaction. Its
effects are external, like an HTTP call. Put such calls in `StepOnceAsync`, or make them idempotent.

### Definite failures

An action in `StepOnceAsync` can throw `DefiniteFailureException(message, detail)` when the remote system
confirms that it did nothing (for example, "rejected"). Resume then discards the marker, and the exception
goes to the retry policy like any other failure. The job does not pause. `Detail` holds any JSON that you
attach. Throw it only when the remote system confirms that no effect occurred. Any doubt means an unknown
outcome.

### Timeouts and blocking code

```csharp
var options = new RunOptions { Lease = TimeSpan.FromSeconds(15), StepTimeout = TimeSpan.FromSeconds(10) };
await steps.StepOnceAsync("post-ledger", PostAsync, timeout: TimeSpan.FromSeconds(12));
```

- `Lease` (default 60 s) is how long a claim survives without progress. Each step renews it. After it
  expires, another worker can take over the job.
- `StepTimeout` (default 30 s, per step through `timeout:`) must be shorter than the lease.
- Actions run on the thread pool, so the timeout also fires for synchronous, blocking code. **An action that
  times out is not stopped.** It keeps running in the background, and resume cannot see its effects. After a
  timeout, the attempt fails. A `StepOnceAsync` step then has an unknown outcome.

## Sensitive data

Job input, step outputs, saga results and `last_error` are stored as plain text.

- The job input must hold references only, such as a request id or an account reference. It must not hold
  PII, card data or account numbers. Step outputs follow the same rule. Keep only what a replay needs.
- `RunOptions.ErrorText` controls the text stored in `last_error`. The default is `exception.Message`. In a
  regulated environment, replace it, for example with `e => e.GetType().Name`. If your function throws or
  returns an empty string, resume stores the exception type name. The exception that your code receives
  does not change.

## Isolation

All procedures run at READ COMMITTED. When they need to see the latest committed rows, they either take a
lock first or use `READCOMMITTEDLOCK`, so READ_COMMITTED_SNAPSHOT (the Azure SQL Database default) is
expected to work. To confirm this on your server, run the contract suite with `--rcsi`. Transactions that
you pass to `SubmitAsync`, `ResolveStepAsync` or `RequeueAsync` must be READ COMMITTED, not SNAPSHOT.

## Tests

```sh
dotnet run --project tests/Resume.Tests -- --database [--kill] [--rcsi]
```

`RESUME_CONNECTION_STRING` must point to a disposable empty database.
