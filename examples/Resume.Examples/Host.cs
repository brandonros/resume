using System.Text.Json;
using Microsoft.Data.SqlClient;
using Resume;

namespace Resume.Examples;

public static class Host
{
    public static async Task<SqlConnection> ConnectAsync(bool provider = false, CancellationToken ct = default)
    {
        var connectionString = provider ? Environment.GetEnvironmentVariable("RESUME_PAYMENT_CONNECTION_STRING") : null;
        connectionString ??= Environment.GetEnvironmentVariable("RESUME_CONNECTION_STRING")
            ?? throw new InvalidOperationException("Set RESUME_CONNECTION_STRING to a SQL Server database");
        var connection = new SqlConnection(connectionString);
        try { await connection.OpenAsync(ct); return connection; }
        catch { await connection.DisposeAsync(); throw; }
    }
    public static Task InstallAsync(Db db, string name) => SqlScript.ApplyAsync(db, File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "sql", name, "schema.sql")));
    public static Retry Retry(Job job, Exception error)
    {
        Console.Error.WriteLine($"job {job.Id}: {error.Message}");
        return error is SqlException { Number: 1205 } && job.Failures < 3
            ? Resume.Retry.After(TimeSpan.FromSeconds(1)) : Resume.Retry.Stop;
    }
    public static async Task WorkersAsync(CancellationToken ct, params Func<CancellationToken, Task>[] workers)
    {
        using var group = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var tasks = workers.Select(worker => worker(group.Token)).ToArray();
        var first = await Task.WhenAny(tasks);
        await group.CancelAsync();
        try { await Task.WhenAll(tasks); }
        catch { await first; throw; }
    }
    public static void Print(JsonElement value) => Console.WriteLine(JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true }));
    public static string Text(JsonElement value, string name) => value.GetProperty(name).GetString() ?? throw new ArgumentException($"Missing {name}");
    public static long Long(JsonElement value, string name) => value.GetProperty(name).GetInt64();
    public static void Check(bool condition, string message) { if (!condition) throw new InvalidOperationException(message); }
}
