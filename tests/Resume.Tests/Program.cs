using Microsoft.SqlServer.TransactSql.ScriptDom;
using Resume;
using Resume.Tests;

var failures = 0;
async Task Test(string name, Func<Task> test)
{
    try { await test().WaitAsync(TimeSpan.FromSeconds(90)); Console.WriteLine($"PASS {name}"); }
    catch (Exception error) { failures++; Console.Error.WriteLine($"FAIL {name}: {error}"); }
}
await Test("canonical JSON: order, duplicates, numbers, null", () =>
{
    Check.Equal(Json.Canonical(Json.Parse("{\"b\":1.00,\"a\":[null,100,0.001]}")), "{\"a\":[null,100,0.001],\"b\":1}");
    Check.Equal(Json.Canonical(Json.Parse("{\"x\":0,\"x\":2e2}")), "{\"x\":200}");
    Check.Equal(Json.Canonical(Json.Parse("-0.00")), "0");
    Check.Equal(Json.Parse(Json.Canonical(Json.Value(500L))).GetInt64(), 500L);
    return Task.CompletedTask;
});
await Test("all SQL scripts parse as T-SQL", () =>
{
    var scripts = Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "sql"), "*.sql", SearchOption.AllDirectories)
        .Append(Path.Combine(AppContext.BaseDirectory, "schema.sql"));
    foreach (var path in scripts)
    {
        using var source = File.OpenText(path);
        new TSql170Parser(true).Parse(source, out var errors);
        Check.That(errors.Count == 0, $"{path}: {string.Join("; ", errors.Select(e => $"line {e.Line}: {e.Message}"))}");
    }
    return Task.CompletedTask;
});
await Test("negative retry delay is rejected", async () =>
    await Check.Throws<ArgumentOutOfRangeException>(() => Task.FromResult(Retry.After(TimeSpan.FromSeconds(-1)))));

if (args.Contains("--database"))
{
    if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("RESUME_CONNECTION_STRING")))
        throw new InvalidOperationException("--database requires RESUME_CONNECTION_STRING pointing to a disposable empty SQL Server database");
    // --rcsi turns on READ_COMMITTED_SNAPSHOT first (the Azure SQL Database default); it needs ALTER DATABASE permission.
    await Contract.Install(args.Contains("--rcsi"));
    await Test("schema install is idempotent, concurrent and versioned", Contract.Reinstall);
    await Test("submission identity and caller rollback", Contract.Submission);
    await Test("transactional effects and durable replay", Contract.Replay);
    await Test("claim contention and stale fencing", Contract.Claims);
    await Test("lease expiry cannot steal an open step transaction", Contract.LockedStep);
    await Test("unknown external outcome and operator recovery", Contract.Recovery);
    await Test("once null replay and atomic operator recovery", Contract.OnceAndRecovery);
    await Test("history order, mode, omitted and duplicate steps", Contract.History);
    await Test("retry policy, delay and error visibility", Contract.RetryPolicy);
    await Test("durable sleep and scheduling", Contract.Sleep);
    await Test("child join and concurrent wakeup", Contract.Children);
    await Test("cancellation rolls back a regular step", Contract.Cancellation);
    await Test("cancellation preserves an at-most-once marker", Contract.OnceCancellation);
    await Test("saga success and reverse compensation replay", Contract.Sagas);
    await Test("saga unknown outcome blocks compensation", Contract.SagaUnknown);
    await Test("run a specific job and read its state", Contract.RunJob);
    await Test("definite step_once failure discards the marker", Contract.DefiniteFailure);
    await Test("lease, step timeout and error text options", Contract.Options);
    await Test("parallel workers drain each job once", Contract.Stress);
    await Test("application examples: orders and import", Contract.Applications);
    await Test("saga example: uncertain charge and refund retry", Contract.CompensationExample);
    if (args.Contains("--kill")) await Test("connection termination rolls back effects and fences worker", Contract.ConnectionLoss);
    else Console.WriteLine("SKIP connection termination (add --kill; requires permission to KILL sessions)");
}
else Console.WriteLine("SKIP SQL Server integration tests (run with --database against a disposable empty database)");
Environment.ExitCode = failures == 0 ? 0 : 1;

namespace Resume.Tests
{
    public static class Check
    {
        public static void That(bool condition, string message = "Assertion failed") { if (!condition) throw new InvalidOperationException(message); }
        public static void Equal<T>(T actual, T expected) => That(EqualityComparer<T>.Default.Equals(actual, expected), $"Expected {expected}, got {actual}");
        public static async Task<T> Throws<T>(Func<Task> action) where T : Exception
        {
            try { await action(); } catch (T error) { return error; }
            throw new InvalidOperationException($"Expected {typeof(T).Name}");
        }
    }
}
