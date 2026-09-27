using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Resume;

public sealed class SuspendedException() : Exception("Job suspended");

/// <summary>Sequential state for one attempt. Concurrent calls are rejected. Actions must honor cancellation.</summary>
public sealed partial class Steps
{
    private readonly SqlConnection connection;
    private readonly long id, attempt;
    private readonly CancellationToken token;
    private readonly HashSet<string> keys = new(StringComparer.Ordinal);
    private int busy;
    private volatile bool closed, poisoned;
    private bool sagaStarted;
    internal bool Suspended { get; private set; }
    internal int Position { get; private set; }
    internal Steps(SqlConnection connection, long id, long attempt, CancellationToken token) =>
        (this.connection, this.id, this.attempt, this.token) = (connection, id, attempt, token);

    public Task<JsonElement> StepAsync(string key, Func<Db, CancellationToken, Task<JsonElement>> action) => StepRegistered(key, null, false, action);
    public Task<JsonElement> StepOnceAsync(string key, Func<CancellationToken, Task<JsonElement>> action) => OnceRegistered(key, null, action);
    internal void Close() => closed = true;
    internal void EnsureSettled()
    {
        if (Volatile.Read(ref busy) != 0 || poisoned) throw new InvalidOperationException("An action is still running or was interrupted");
    }
    private void Enter()
    {
        token.ThrowIfCancellationRequested();
        if (closed || poisoned) throw new InvalidOperationException("Attempt is no longer usable");
        if (Suspended) throw new SuspendedException();
        if (Interlocked.CompareExchange(ref busy, 1, 0) != 0) throw new InvalidOperationException("Steps must execute sequentially");
    }
    private async Task<JsonElement?> Start(Db db, string key, bool once, string? undo, bool isUndo)
    {
        if (string.IsNullOrEmpty(key) || !keys.Add(key)) throw new ArgumentException("Step keys must be nonempty and unique within an attempt");
        var next = checked(Position + 1);
        var row = (await db.ProcAsync("start_step", token, ("id", id), ("attempt", attempt), ("key", key),
            ("position", Position), ("once", once), ("compensation", undo), ("is_compensation", isUndo))).Single();
        Position = next;
        return row["output"] is null ? null : row.Json("output");
    }
    private async Task<JsonElement> Save(Db db, string key, JsonElement output)
    {
        if (closed) throw new InvalidOperationException("Handler abandoned the attempt");
        var row = (await db.ProcAsync("save_step", token, ("id", id), ("attempt", attempt),
            ("key", key), ("output", Json.Canonical(output)))).Single();
        return row.Json("output");
    }
    private async Task<JsonElement> Action(Func<CancellationToken, Task<JsonElement>> action)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        try { return await action(timeout.Token).WaitAsync(timeout.Token); }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested && !token.IsCancellationRequested)
        {
            poisoned = true;
            throw new TimeoutException("Step action exceeded 30 seconds; external effects may still be running");
        }
    }
    internal async Task<JsonElement> StepRegistered(string key, string? undo, bool isUndo, Func<Db, CancellationToken, Task<JsonElement>> action)
    {
        Enter();
        try
        {
            await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, token);
            var db = new Db(connection, tx);
            var saved = await Start(db, key, false, undo, isUndo);
            var output = saved ?? await Save(db, key, await Action(ct => action(db, ct)));
            await tx.CommitAsync(token);
            return output;
        }
        finally { Volatile.Write(ref busy, 0); }
    }
    internal async Task<JsonElement> OnceRegistered(string key, string? undo, Func<CancellationToken, Task<JsonElement>> action)
    {
        Enter();
        try
        {
            JsonElement? saved;
            await using (var tx = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, token))
            {
                saved = await Start(new Db(connection, tx), key, true, undo, false);
                await tx.CommitAsync(token);
            }
            return saved ?? await Save(new Db(connection), key, await Action(action));
        }
        finally { Volatile.Write(ref busy, 0); }
    }
    public async Task SleepAsync(string key, TimeSpan duration)
    {
        if (duration < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(duration));
        var wake = await StepAsync(key, async (db, ct) =>
        {
            var row = (await db.QueryAsync("SELECT resume.after_delay(@seconds) AS wake", ct, ("seconds", duration.TotalSeconds))).Single();
            if (row["wake"] is not DateTime dt) throw new ArgumentOutOfRangeException(nameof(duration));
            return Json.Value(new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)));
        });
        Enter();
        try
        {
            var row = (await new Db(connection).ProcAsync("suspend", token, ("id", id), ("attempt", attempt),
                ("until", wake.GetDateTimeOffset()))).Single();
            if (row.Bool("suspended")) { Suspended = true; throw new SuspendedException(); }
        }
        finally { Volatile.Write(ref busy, 0); }
    }
    public async Task<long> SpawnAsync(string key, string workflow, JsonElement input) =>
        (await StepAsync(key, async (db, ct) => Json.Value(await Workflow.SubmitAsync(db, workflow, $"{id}/{key}", input, parentId: id, ct: ct)))).GetInt64();
    public async Task<JsonElement> WaitForAsync(long child)
    {
        Enter();
        try
        {
            var row = (await new Db(connection).ProcAsync("wait_for", token, ("id", id), ("attempt", attempt), ("child", child))).Single();
            if (row["output"] is null) { Suspended = true; throw new SuspendedException(); }
            return row.Json("output");
        }
        finally { Volatile.Write(ref busy, 0); }
    }
}
