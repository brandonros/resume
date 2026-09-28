using System.Runtime.ExceptionServices;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Resume;

public sealed record Job(long Id, long Attempt, long Failures, string Key, JsonElement Input);

public readonly record struct Retry(TimeSpan? Delay)
{
    public static Retry Stop => new(null);

    public static Retry After(TimeSpan delay) =>
        delay < TimeSpan.Zero ? throw new ArgumentOutOfRangeException(nameof(delay)) : new(delay);
}

public delegate Task<JsonElement> Handler(Job job, Steps steps, CancellationToken cancellationToken);

public delegate Retry RetryPolicy(Job job, Exception error);

/// <summary>Worker settings. StepTimeout must be shorter than Lease so an action never outlives its claim.</summary>
public sealed record RunOptions
{
    public static RunOptions Default { get; } = new();

    /// <summary>How long a claim survives without progress. Each step renews it.</summary>
    public TimeSpan Lease { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>Default limit for one step action. Steps can override it.</summary>
    public TimeSpan StepTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>The text stored as last_error, in plain text. Use this to redact identifiers.</summary>
    public Func<Exception, string> ErrorText { get; init; } = error => error.Message;

    internal int LeaseMilliseconds()
    {
        if (Lease < TimeSpan.FromSeconds(1) || Lease > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(Lease), "Lease must be between 1 second and 1 day");
        CheckTimeout(StepTimeout);
        return (int)Math.Ceiling(Lease.TotalMilliseconds);
    }

    internal void CheckTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero || timeout >= Lease)
            throw new ArgumentOutOfRangeException(nameof(StepTimeout), "Step timeout must be positive and shorter than the lease");
    }

    internal string DescribeError(Exception error)
    {
        try
        {
            var text = ErrorText(error);
            if (!string.IsNullOrEmpty(text)) return text;
        }
        catch (Exception) { }
        return error.GetType().FullName ?? "Exception";
    }
}

public enum JobStatus { Ready, Scheduled, Running, LeaseExpired, Waiting, Paused, Completed }

/// <summary>A snapshot of one job. While Running, an UnresolvedSteps entry may still be in flight.</summary>
public sealed record JobInfo(
    long Id,
    string Workflow,
    string Key,
    JobStatus Status,
    long Attempt,
    long Failures,
    DateTimeOffset? AvailableAt,
    long? ParentId,
    JsonElement? Output,
    string? LastError,
    IReadOnlyList<string> UnresolvedSteps);

public static class Workflow
{
    public static async Task<long> SubmitAsync(Db db, string workflow, string key, JsonElement input,
        DateTimeOffset? at = null, long? parentId = null, CancellationToken ct = default)
    {
        var rows = await db.ProcAsync("submit", ct,
            ("workflow", workflow), ("key", key), ("input", Json.Canonical(input)), ("available_at", at), ("parent_id", parentId));
        return rows.Single().Long("id");
    }

    /// <summary>Runs jobs until cancelled. Use one dedicated connection per worker.</summary>
    public static async Task WorkAsync(SqlConnection connection, string workflow, Handler handler, RetryPolicy retry,
        RunOptions? options = null, CancellationToken ct = default)
    {
        while (true)
        {
            var result = await RunAttempt(connection, ClaimNext(workflow), handler, retry, options, ct);
            if (!result.Processed) await Task.Delay(250, ct);
        }
    }

    /// <summary>Runs the next ready job. True when a job ran; false when none was ready.
    /// A handler failure is saved first and then rethrown.</summary>
    public static async Task<bool> RunOneAsync(SqlConnection connection, string workflow, Handler handler, RetryPolicy retry,
        RunOptions? options = null, CancellationToken ct = default)
    {
        var result = await RunAttempt(connection, ClaimNext(workflow), handler, retry, options, ct);
        return Unwrap(result);
    }

    /// <summary>Runs one specific job now. False when it is not ready: leased, scheduled, waiting, paused,
    /// completed, missing, or in another workflow. If this caller dies, a worker for the workflow resumes it.</summary>
    public static async Task<bool> RunJobAsync(SqlConnection connection, string workflow, long id, Handler handler, RetryPolicy retry,
        RunOptions? options = null, CancellationToken ct = default)
    {
        Claim claim = (db, lease, token) => db.ProcAsync("claim_job", token, ("workflow", workflow), ("id", id), ("lease_ms", lease));
        var result = await RunAttempt(connection, claim, handler, retry, options, ct);
        return Unwrap(result);
    }

    public static async Task<JobInfo?> GetJobAsync(Db db, long id, CancellationToken ct = default)
    {
        const string sql = """
            SELECT j.*,
                (SELECT s.[key] AS k FROM resume.steps s
                 WHERE s.job_id = j.id AND s.once = 1 AND s.output IS NULL
                 ORDER BY s.position FOR JSON PATH) AS unresolved
            FROM resume.job_status j
            WHERE j.id = @id
            """;
        var row = (await db.QueryAsync(sql, ct, ("id", id))).SingleOrDefault();
        if (row is null) return null;

        var unresolved = row["unresolved"] is null
            ? new List<string>()
            : row.Json("unresolved").EnumerateArray().Select(step => step.GetProperty("k").GetString()!).ToList();

        return new JobInfo(
            Id: row.Long("id"),
            Workflow: row.Text("workflow"),
            Key: row.Text("key"),
            Status: ParseStatus(row.Text("status")),
            Attempt: row.Long("attempt"),
            Failures: row.Long("failures"),
            AvailableAt: row["available_at"] is DateTime at ? new DateTimeOffset(DateTime.SpecifyKind(at, DateTimeKind.Utc)) : null,
            ParentId: row["parent_id"] is null ? null : row.Long("parent_id"),
            Output: row["output"] is null ? null : row.Json("output"),
            LastError: (string?)row["last_error"],
            UnresolvedSteps: unresolved);
    }

    public static async Task ResolveStepAsync(Db db, long id, string key, JsonElement output, CancellationToken ct = default) =>
        await db.ProcAsync("resolve_step", ct, ("id", id), ("key", key), ("output", Json.Canonical(output)));

    public static async Task RequeueAsync(Db db, long id, TimeSpan delay = default, CancellationToken ct = default) =>
        await db.ProcAsync("requeue", ct, ("id", id), ("delay_seconds", delay.TotalSeconds));

    private delegate Task<List<Row>> Claim(Db db, int leaseMilliseconds, CancellationToken ct);

    private sealed record Attempt(bool Processed, Exception? Error = null);

    private static Claim ClaimNext(string workflow) =>
        (db, lease, ct) => db.ProcAsync("claim", ct, ("workflow", workflow), ("lease_ms", lease));

    private static bool Unwrap(Attempt result)
    {
        if (result.Error is not null) ExceptionDispatchInfo.Capture(result.Error).Throw();
        return result.Processed;
    }

    private static async Task<Attempt> RunAttempt(SqlConnection connection, Claim claim, Handler handler, RetryPolicy retry,
        RunOptions? options, CancellationToken ct)
    {
        options ??= RunOptions.Default;
        var lease = options.LeaseMilliseconds();
        var db = new Db(connection);
        await db.ExecAsync("SET XACT_ABORT ON; SET LOCK_TIMEOUT 60000; SET TRANSACTION ISOLATION LEVEL READ COMMITTED;", ct);

        var row = (await claim(db, lease, ct)).SingleOrDefault();
        if (row is null) return new(Processed: false);

        var job = new Job(row.Long("id"), row.Long("attempt"), row.Long("failures"), row.Text("key"), row.Json("input"));
        var steps = new Steps(connection, job.Id, job.Attempt, options, ct);
        Exception failure;
        try
        {
            JsonElement output;
            try
            {
                output = await handler(job, steps, ct).WaitAsync(ct);
            }
            catch when (steps.Suspended && !ct.IsCancellationRequested)
            {
                // Sleeping or waiting for a child; the handler may have surfaced the suspension as an exception.
                return new(Processed: true);
            }
            if (steps.Suspended) return new(Processed: true);

            steps.EnsureSettled();
            await db.ProcAsync("finish", ct,
                ("id", job.Id), ("attempt", job.Attempt), ("position", steps.Position), ("output", Json.Canonical(output)));
            return new(Processed: true);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error)
        {
            failure = error;
        }
        finally
        {
            steps.Close();
        }

        // No retry delay pauses the job for an operator.
        var decision = retry(job, failure);
        await db.ProcAsync("finish", ct,
            ("id", job.Id), ("attempt", job.Attempt), ("error", options.DescribeError(failure)),
            ("retry_after_seconds", decision.Delay?.TotalSeconds));
        return new(Processed: true, failure);
    }

    private static JobStatus ParseStatus(string status) => status switch
    {
        "ready" => JobStatus.Ready,
        "scheduled" => JobStatus.Scheduled,
        "running" => JobStatus.Running,
        "lease_expired" => JobStatus.LeaseExpired,
        "waiting" => JobStatus.Waiting,
        "paused" => JobStatus.Paused,
        "completed" => JobStatus.Completed,
        _ => throw new InvalidOperationException($"Unknown job status {status}")
    };
}
