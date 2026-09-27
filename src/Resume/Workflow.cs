using System.Data;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Resume;

public sealed record Job(long Id, long Attempt, long Failures, string Key, JsonElement Input);
public readonly record struct Retry(TimeSpan? Delay)
{
    public static Retry Stop => new(null);
    public static Retry After(TimeSpan delay) => delay < TimeSpan.Zero ? throw new ArgumentOutOfRangeException(nameof(delay)) : new(delay);
}
public delegate Task<JsonElement> Handler(Job job, Steps steps, CancellationToken cancellationToken);

public static class Workflow
{
    public static async Task<long> SubmitAsync(Db db, string workflow, string key, JsonElement input,
        DateTimeOffset? at = null, long? parentId = null, CancellationToken ct = default)
    {
        var row = (await db.ProcAsync("submit", ct, ("workflow", workflow), ("key", key),
            ("input", Json.Canonical(input)), ("available_at", at), ("parent_id", parentId))).Single();
        return row.Long("id");
    }
    /// <summary>One dedicated connection per worker. Infrastructure/settlement errors return to the caller.</summary>
    public static async Task WorkAsync(SqlConnection connection, string workflow, Handler handler,
        Func<Job, Exception, Retry> retry, CancellationToken ct = default)
    {
        while (true)
        {
            var result = await RunAttempt(connection, workflow, handler, retry, ct);
            if (!result.Processed) await Task.Delay(250, ct);
        }
    }
    /// <summary>True on completion or suspension; false when idle. Reported failures are persisted then rethrown.</summary>
    public static async Task<bool> RunOneAsync(SqlConnection connection, string workflow, Handler handler,
        Func<Job, Exception, Retry> retry, CancellationToken ct = default)
    {
        var result = await RunAttempt(connection, workflow, handler, retry, ct);
        if (result.Error is not null) ExceptionDispatchInfo.Capture(result.Error).Throw();
        return result.Processed;
    }
    private sealed record Attempt(bool Processed, Exception? Error = null);
    private static async Task<Attempt> RunAttempt(SqlConnection connection, string workflow, Handler handler,
        Func<Job, Exception, Retry> retry, CancellationToken ct)
    {
        var db = new Db(connection);
        await db.ExecAsync("SET XACT_ABORT ON; SET LOCK_TIMEOUT 60000; SET TRANSACTION ISOLATION LEVEL READ COMMITTED;", ct);
        var rows = await db.ProcAsync("claim", ct, ("workflow", workflow));
        if (rows.Count == 0) return new(false);
        var row = rows.Single();
        var job = new Job(row.Long("id"), row.Long("attempt"), row.Long("failures"), row.Text("key"), row.Json("input"));
        var steps = new Steps(connection, job.Id, job.Attempt, ct);
        Exception? failure = null;
        try
        {
            JsonElement output;
            try { output = await handler(job, steps, ct).WaitAsync(ct); }
            catch when (steps.Suspended && !ct.IsCancellationRequested) { return new(true); }
            if (steps.Suspended) return new(true);
            steps.EnsureSettled();
            await db.ProcAsync("finish", ct, ("id", job.Id), ("attempt", job.Attempt), ("position", steps.Position), ("output", Json.Canonical(output)));
            return new(true);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch (Exception error) { failure = error; }
        finally { steps.Close(); }
        var decision = retry(job, failure);
        await db.ProcAsync("finish", ct, ("id", job.Id), ("attempt", job.Attempt), ("error", failure.Message),
            ("retry_after_seconds", decision.Delay?.TotalSeconds));
        return new(true, failure);
    }
    public static async Task ResolveStepAsync(Db db, long id, string key, JsonElement output, CancellationToken ct = default) =>
        _ = await db.ProcAsync("resolve_step", ct, ("id", id), ("key", key), ("output", Json.Canonical(output)));
    public static async Task RequeueAsync(Db db, long id, TimeSpan delay = default, CancellationToken ct = default) =>
        _ = await db.ProcAsync("requeue", ct, ("id", id), ("delay_seconds", delay.TotalSeconds));
}
