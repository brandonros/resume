using Resume;

namespace Resume.Examples;

public static class Demos
{
    public static async Task Basic(CancellationToken ct)
    {
        await using var connection = await Host.ConnectAsync(ct: ct);
        var db = new Db(connection);
        await db.ExecAsync("IF OBJECT_ID('dbo.greetings') IS NULL CREATE TABLE dbo.greetings(job_id bigint PRIMARY KEY,name nvarchar(200));", ct);
        await Workflow.SubmitAsync(db, "greet", "ada", Json.Value(new { name = "Ada" }), ct: ct);
        await Workflow.WorkAsync(connection, "greet", async (job, steps, _) =>
            await steps.StepAsync("greet", async (tx, token) =>
            {
                var name = Host.Text(job.Input, "name");
                await tx.ExecAsync("INSERT INTO dbo.greetings VALUES(@id,@name)", token, ("id", job.Id), ("name", name));
                return Json.Value(new { greeted = name });
            }), Host.Retry, ct);
    }
    public static async Task Flow(CancellationToken ct)
    {
        await using var parent = await Host.ConnectAsync(ct: ct);
        await using var child = await Host.ConnectAsync(ct: ct);
        await using var ticks = await Host.ConnectAsync(ct: ct);
        await Workflow.SubmitAsync(new Db(parent), "flow-order", "order-1", Json.Value(new[] { "book", "lamp" }), ct: ct);
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        await Workflow.SubmitAsync(new Db(ticks), "tick", now.ToString(), Json.Value(now), ct: ct);
        await Host.WorkersAsync(ct,
            token => Workflow.WorkAsync(parent, "flow-order", async (job, steps, _) =>
            {
                var children = new List<long>();
                foreach (var item in job.Input.EnumerateArray()) children.Add(await steps.SpawnAsync("ship-" + item.GetString(), "flow-ship", item));
                await steps.SleepAsync("cool-off", TimeSpan.FromSeconds(2));
                var receipts = new List<System.Text.Json.JsonElement>();
                foreach (var id in children) receipts.Add(await steps.WaitForAsync(id));
                return Json.Value(receipts);
            }, Host.Retry, token),
            token => Workflow.WorkAsync(child, "flow-ship", (job, steps, _) => steps.StepAsync("ship", (_, _) => Task.FromResult(Json.Value(new { shipped = job.Input }))), Host.Retry, token),
            token => Workflow.WorkAsync(ticks, "tick", (job, steps, _) => steps.StepAsync("schedule-next", async (tx, cancellation) =>
            {
                var next = job.Input.GetInt64() + 5;
                await Workflow.SubmitAsync(tx, "tick", next.ToString(), Json.Value(next), DateTimeOffset.FromUnixTimeSeconds(next), ct: cancellation);
                Console.WriteLine($"tick {job.Input}");
                return Json.Value(next);
            }), Host.Retry, token));
    }
    public static async Task Recovery(CancellationToken ct)
    {
        await using var worker = await Host.ConnectAsync(ct: ct);
        await using var service = await Host.ConnectAsync(ct: ct);
        var db = new Db(worker);
        var external = new Db(service);
        await external.ExecAsync("CREATE TABLE #charges(id bigint IDENTITY PRIMARY KEY,job_id bigint)", ct);
        var workflow = "recovery-" + Guid.NewGuid().ToString("N");
        var id = await Workflow.SubmitAsync(db, workflow, "order", Json.Null, ct: ct);
        Handler checkout = (_, steps, _) => steps.StepOnceAsync("charge", async token =>
        {
            await external.ExecAsync("INSERT INTO #charges(job_id) VALUES(@id)", token, ("id", id));
            throw new IOException("Charge committed, response lost");
        });
        try { await Workflow.RunOneAsync(worker, workflow, checkout, (_, _) => Retry.Stop, ct); throw new InvalidOperationException("Expected lost response"); }
        catch (IOException) { }
        try { await Workflow.RequeueAsync(db, id, ct: ct); throw new InvalidOperationException("Unresolved action must block requeue"); }
        catch (Microsoft.Data.SqlClient.SqlException e) when (e.Number == 50004) { }
        var verified = (await external.QueryAsync("SELECT id FROM #charges WHERE job_id=@id", ct, ("id", id))).Single();
        await Workflow.ResolveStepAsync(db, id, "charge", Json.Value(new { charge_id = verified.Long("id") }), ct);
        await Workflow.RequeueAsync(db, id, ct: ct);
        await Workflow.RunOneAsync(worker, workflow, checkout, (_, _) => Retry.Stop, ct);
        Host.Check((await external.QueryAsync("SELECT id FROM #charges", ct)).Count == 1, "Charge repeated");
        Console.WriteLine($"Job {id}: reconciled and completed with one charge invocation.");
    }
}
