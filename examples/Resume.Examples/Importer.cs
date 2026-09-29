using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Resume;

namespace Resume.Examples;

public static class Importer
{
    public const string ImportWorkflow = "import-app:import:v1", BatchWorkflow = "import-app:batch:v1";
    private const int MaxBytes = 2 * 1024 * 1024;
    public static Task<long> EnqueueAsync(Db db, string key, string source)
    {
        if (Encoding.UTF8.GetByteCount(source) > MaxBytes) throw new ArgumentException("Import exceeds 2 MiB; split it");
        return Workflow.SubmitAsync(db, ImportWorkflow, key, Json.Value(new { source }));
    }
    public static async Task<JsonElement> Import(Job job, Steps steps, CancellationToken ct)
    {
        var lines = new List<string>(); using var reader = new StringReader(Host.Text(job.Input, "source"));
        while (reader.ReadLine() is { } line) lines.Add(line);
        var children = new List<long>();
        for (var i = 0; i < lines.Count; i += 100)
            children.Add(await steps.SpawnAsync($"batch-{i / 100}", BatchWorkflow, Json.Value(new { start = i + 1, lines = lines.Skip(i).Take(100).ToArray() })));
        long accepted = 0, rejected = 0;
        foreach (var child in children) { var result = await steps.WaitForAsync(child); accepted += Host.Long(result, "accepted"); rejected += Host.Long(result, "rejected"); }
        return Json.Value(new { accepted, rejected });
    }
    public static Task<JsonElement> Batch(Job job, Steps steps, CancellationToken ct) => steps.StepAsync("import-rows", async (db, token) =>
    {
        long accepted = 0, rejected = 0, line = Host.Long(job.Input, "start");
        foreach (var item in job.Input.GetProperty("lines").EnumerateArray())
        {
            var raw = item.GetString()!; string? reason = null; JsonElement value = Json.Null;
            try
            {
                value = Json.Parse(raw);
                if (string.IsNullOrEmpty(Host.Text(value, "id")) || Host.Text(value, "id").Length > 512 || Host.Text(value, "id") != Host.Text(value, "id").TrimEnd() || string.IsNullOrEmpty(Host.Text(value, "account"))) throw new ArgumentException("Invalid id/account");
                _ = Host.Long(value, "amount_cents");
            }
            catch (Exception e) when (e is JsonException or KeyNotFoundException or InvalidOperationException or ArgumentException or FormatException) { reason = e.Message; }
            if (reason is null)
            {
                var id = Host.Text(value, "id"); var payload = Json.Canonical(value);
                var saved = await db.QueryAsync("SELECT payload FROM import_app.events WITH (UPDLOCK,HOLDLOCK) WHERE id=@id", token, ("id", id));
                if (saved.Count == 0) await db.ExecAsync("INSERT import_app.events VALUES(@id,@payload)", token, ("id", id), ("payload", payload));
                else if (saved.Single().Text("payload") != payload) reason = "event id already has different content";
            }
            if (reason is null) accepted++;
            else { await db.ExecAsync("INSERT import_app.rejections VALUES(@job,@line,@raw,@reason)", token, ("job", job.Id), ("line", line), ("raw", raw), ("reason", reason)); rejected++; }
            line++;
        }
        return Json.Value(new { accepted, rejected });
    });
    public static async Task<JsonElement> StatusAsync(Db db, string key)
    {
        var row = (await db.QueryAsync("SELECT j.id,s.status,s.last_error,j.output FROM resume.jobs j JOIN resume.job_status s ON s.id=j.id WHERE j.workflow=@workflow AND j.[key]=@key", ("workflow", ImportWorkflow), ("key", key))).Single();
        row["result"] = row["output"] is null ? null : row.Json("output"); row.Remove("output");
        row["children"] = await db.QueryAsync("SELECT id,status,failures,last_error FROM resume.job_status WHERE parent_id=@id ORDER BY id", ("id", row.Long("id")));
        row["rejections"] = await db.QueryAsync("SELECT r.line,r.reason FROM import_app.rejections r JOIN resume.jobs c ON c.id=r.job_id WHERE c.parent_id=@id ORDER BY r.line", ("id", row.Long("id")));
        return Json.Value(row);
    }
    public static async Task DrainAsync(SqlConnection c, CancellationToken ct = default)
    {
        while (true)
        {
            var parent = await Workflow.RunOneAsync(c, ImportWorkflow, Import, Host.Retry, ct: ct);
            var child = await Workflow.RunOneAsync(c, BatchWorkflow, Batch, Host.Retry, ct: ct);
            if (!parent && !child) return;
        }
    }
    public static async Task RunAsync(string[] args, CancellationToken ct)
    {
        await using var c = await Host.ConnectAsync(ct: ct); var db = new Db(c);
        switch (args)
        {
            case ["init"]: await Host.InstallAsync(db, "importer"); break;
            case ["load", var key, var path]:
                await using (var file = File.OpenRead(path))
                {
                    if (file.Length > MaxBytes) throw new ArgumentException("Import exceeds 2 MiB");
                    using var reader = new StreamReader(file, new UTF8Encoding(false, true));
                    Console.WriteLine(await EnqueueAsync(db, key, await reader.ReadToEndAsync(ct)));
                }
                break;
            case ["status", var key]: Host.Print(await StatusAsync(db, key)); break;
            case ["drain"]: await DrainAsync(c, ct); break;
            case ["work"]:
                await using (var batches = await Host.ConnectAsync(ct: ct))
                    await Host.WorkersAsync(ct, token => Workflow.WorkAsync(c, ImportWorkflow, Import, Host.Retry, ct: token), token => Workflow.WorkAsync(batches, BatchWorkflow, Batch, Host.Retry, ct: token));
                break;
            default: throw new ArgumentException("importer init | load KEY FILE | status KEY | drain | work");
        }
    }
}
