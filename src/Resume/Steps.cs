using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Resume;

public sealed class SuspendedException() : Exception("Job suspended");

/// <summary>Thrown by a step_once action when the remote system confirms that it applied nothing.
/// The step marker is discarded and the exception goes to the retry policy.
/// Never throw it when the outcome is uncertain.</summary>
public sealed class DefiniteFailureException(string message, JsonElement? detail = null, Exception? inner = null)
    : Exception(message, inner)
{
    public JsonElement? Detail { get; } = detail?.Clone();
}

/// <summary>Step state for one attempt. Steps run one at a time. Actions must honor cancellation.</summary>
public sealed partial class Steps
{
    private readonly SqlConnection connection;
    private readonly long id;
    private readonly long attempt;
    private readonly RunOptions options;
    private readonly CancellationToken token;
    private readonly HashSet<string> keys = new(StringComparer.Ordinal);
    private int busy;
    private volatile bool closed;
    private volatile bool poisoned;
    private bool sagaStarted;

    internal bool Suspended { get; private set; }
    internal int Position { get; private set; }

    internal Steps(SqlConnection connection, long id, long attempt, RunOptions options, CancellationToken token)
    {
        this.connection = connection;
        this.id = id;
        this.attempt = attempt;
        this.options = options;
        this.token = token;
    }

    /// <summary>Runs in resume's transaction. Only writes made through the given Db
    /// (or its Connection and Transaction) commit atomically with the saved output.</summary>
    public Task<JsonElement> StepAsync(string key, Func<Db, CancellationToken, Task<JsonElement>> action, TimeSpan? timeout = null) =>
        StepRegistered(key, compensation: null, isCompensation: false, action, timeout);

    /// <summary>Runs at most once. Any exception except DefiniteFailureException leaves the outcome unknown
    /// and pauses the job for an operator.</summary>
    public Task<JsonElement> StepOnceAsync(string key, Func<CancellationToken, Task<JsonElement>> action, TimeSpan? timeout = null) =>
        OnceRegistered(key, compensation: null, action, timeout);

    public async Task SleepAsync(string key, TimeSpan duration)
    {
        if (duration < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(duration));

        // Save the wake time so a replay sleeps until the same moment.
        var wake = await StepAsync(key, async (db, ct) =>
        {
            var row = (await db.QueryAsync("SELECT resume.after_delay(@seconds) AS wake", ct, ("seconds", duration.TotalSeconds))).Single();
            if (row["wake"] is not DateTime time) throw new ArgumentOutOfRangeException(nameof(duration));
            return Json.Value(new DateTimeOffset(DateTime.SpecifyKind(time, DateTimeKind.Utc)));
        });

        var suspended = await Exclusive(async () =>
            (await Call(new Db(connection), "suspend", ("until", wake.GetDateTimeOffset())))!.Bool("suspended"));
        if (suspended) Suspend();
    }

    public async Task<long> SpawnAsync(string key, string workflow, JsonElement input)
    {
        var child = await StepAsync(key, async (db, ct) =>
            Json.Value(await Workflow.SubmitAsync(db, workflow, $"{id}/{key}", input, parentId: id, ct: ct)));
        return child.GetInt64();
    }

    public async Task<JsonElement> WaitForAsync(long child)
    {
        var row = (await Exclusive(() => Call(new Db(connection), "wait_for", ("child", child))))!;
        if (row["output"] is null) Suspend();
        return row.Json("output");
    }

    internal void Close() => closed = true;

    internal void EnsureSettled()
    {
        if (Volatile.Read(ref busy) != 0 || poisoned)
            throw new InvalidOperationException("An action is still running or was interrupted");
    }

    internal Task<JsonElement> StepRegistered(string key, string? compensation, bool isCompensation,
        Func<Db, CancellationToken, Task<JsonElement>> action, TimeSpan? timeout = null)
    {
        var limit = Limit(timeout);
        return Exclusive(async () =>
        {
            await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, token);
            var db = new Db(connection, tx);

            var output = await Start(db, key, once: false, compensation, isCompensation)
                ?? await Save(db, key, await RunAction(ct => action(db, ct), limit));

            await tx.CommitAsync(token);
            return output;
        });
    }

    internal Task<JsonElement> OnceRegistered(string key, string? compensation,
        Func<CancellationToken, Task<JsonElement>> action, TimeSpan? timeout = null)
    {
        var limit = Limit(timeout);
        return Exclusive(async () =>
        {
            var position = Position;

            // Commit the marker before the action runs, so a crash leaves a visible unknown outcome.
            JsonElement? saved;
            await using (var tx = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, token))
            {
                saved = await Start(new Db(connection, tx), key, once: true, compensation, isCompensation: false);
                await tx.CommitAsync(token);
            }
            if (saved is JsonElement replayed) return replayed;

            JsonElement output;
            try
            {
                output = await RunAction(action, limit);
            }
            catch (DefiniteFailureException)
            {
                // Nothing happened remotely, so continue as if the step never started.
                await Call(new Db(connection), "discard_step", ("key", key));
                Position = position;
                keys.Remove(key);
                throw;
            }
            return await Save(new Db(connection), key, output);
        });
    }

    /// <summary>Runs body as the only step in progress, and rejects concurrent or late calls.</summary>
    private async Task<T> Exclusive<T>(Func<Task<T>> body)
    {
        token.ThrowIfCancellationRequested();
        if (closed || poisoned) throw new InvalidOperationException("Attempt is no longer usable");
        if (Suspended) throw new SuspendedException();
        if (Interlocked.CompareExchange(ref busy, 1, 0) != 0) throw new InvalidOperationException("Steps must execute sequentially");
        try
        {
            return await body();
        }
        finally
        {
            Volatile.Write(ref busy, 0);
        }
    }

    /// <summary>Calls a procedure that takes this attempt's id and attempt number.</summary>
    private async Task<Row?> Call(Db db, string procedure, params (string, object?)[] args)
    {
        var rows = await db.ProcAsync(procedure, token, [("id", id), ("attempt", attempt), .. args]);
        return rows.SingleOrDefault();
    }

    private void Suspend()
    {
        Suspended = true;
        throw new SuspendedException();
    }

    private TimeSpan Limit(TimeSpan? timeout)
    {
        var limit = timeout ?? options.StepTimeout;
        options.CheckTimeout(limit);
        return limit;
    }

    /// <summary>Registers the step, or returns its saved output on replay.</summary>
    private async Task<JsonElement?> Start(Db db, string key, bool once, string? compensation, bool isCompensation)
    {
        if (string.IsNullOrEmpty(key) || !keys.Add(key))
            throw new ArgumentException("Step keys must be nonempty and unique within an attempt");

        var next = checked(Position + 1);
        var row = (await Call(db, "start_step",
            ("key", key), ("position", Position), ("once", once), ("compensation", compensation), ("is_compensation", isCompensation)))!;
        Position = next;
        return row["output"] is null ? null : row.Json("output");
    }

    private async Task<JsonElement> Save(Db db, string key, JsonElement output)
    {
        if (closed) throw new InvalidOperationException("Handler abandoned the attempt");
        var row = (await Call(db, "save_step", ("key", key), ("output", Json.Canonical(output))))!;
        return row.Json("output");
    }

    private async Task<JsonElement> RunAction(Func<CancellationToken, Task<JsonElement>> action, TimeSpan limit)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(limit);
        try
        {
            // Task.Run lets the timeout fire even when the action blocks synchronously.
            return await Task.Run(() => action(timeout.Token)).WaitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested && !token.IsCancellationRequested)
        {
            poisoned = true;
            throw new TimeoutException($"Step action exceeded {limit}; it may still be running and its effects are unknown");
        }
    }
}
