using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Resume;

namespace Resume.Examples;

public static class Compensation
{
    public const string Name = "compensation-app:order:v1";
    public sealed class ResponseLost(bool refund) : IOException(refund ? "Refund response lost" : "Charge response lost")
    { public bool Refund { get; } = refund; }
    public static string ChargeKey(string key) => $"resume-compensation:v1:{key}:charge";
    public static string RefundKey(string key) => $"resume-compensation:v1:{key}:refund";
    public static Retry Policy(Job job, Exception error) => error is ResponseLost { Refund: true } && job.Failures < 3
        ? Retry.After(TimeSpan.Zero) : Retry.Stop;

    public static async Task Initialize(SqlConnection connection, SqlConnection provider)
    {
        await Host.InstallAsync(new Db(connection), "compensation");
        await new Db(connection).ExecAsync("IF NOT EXISTS(SELECT 1 FROM compensation_app.inventory WHERE sku='book') INSERT INTO compensation_app.inventory VALUES('book',10)");
        await SqlScript.ApplyAsync(new Db(provider), File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "sql", "compensation", "provider.sql")));
    }
    public static async Task<JsonElement> Payment(SqlConnection provider, string key, long amount, string? charge, CancellationToken ct)
    {
        if (key.Length > 512 || amount <= 0) throw new ArgumentException("Invalid payment parameters");
        // Deduplication belongs to the provider, in a transaction independent of resume.
        await using var tx = (SqlTransaction)await provider.BeginTransactionAsync(IsolationLevel.Serializable, ct);
        var db = new Db(provider, tx);
        var table = charge is null ? "charges" : "refunds";
        if (charge is not null)
        {
            var receipt = (await db.QueryAsync("SELECT amount FROM payment_provider.charges WHERE [key]=@key", ct, ("key", charge))).Single();
            Host.Check(receipt.Long("amount") == amount, "Refund does not match charge");
        }
        var rows = await db.QueryAsync($"SELECT * FROM payment_provider.{table} WITH(UPDLOCK,HOLDLOCK) WHERE [key]=@key", ct, ("key", key));
        var first = rows.Count == 0;
        if (first)
        {
            if (charge is null) await db.ExecAsync("INSERT INTO payment_provider.charges([key],amount) VALUES(@key,@amount)", ct, ("key", key), ("amount", amount));
            else await db.ExecAsync("INSERT INTO payment_provider.refunds([key],charge_key,amount) VALUES(@key,@charge,@amount)", ct, ("key", key), ("charge", charge), ("amount", amount));
        }
        else
        {
            Host.Check(rows[0].Long("amount") == amount && (charge is null || rows[0].Text("charge_key") == charge), "Payment key reused with different parameters");
            await db.ExecAsync($"UPDATE payment_provider.{table} SET calls=calls+1 WHERE [key]=@key", ct, ("key", key));
        }
        await tx.CommitAsync(ct);
        if (first) throw new ResponseLost(charge is not null);
        return charge is null ? Json.Value(new { charge_key = key, amount }) : Json.Value(new { refund_key = key, charge_key = charge, amount });
    }
    public static async Task<JsonElement> Fulfill(Job job, Steps steps, SqlConnection provider)
    {
        var quantity = Host.Long(job.Input, "quantity");
        var amount = Host.Long(job.Input, "amount");
        Host.Check(quantity > 0 && amount > 0, "Quantity and amount must be positive");
        var outcome = await steps.SagaAsync(async (saga, _) =>
        {
            await saga.StepAsync("reserve", "release-inventory", async (db, ct) =>
            {
                var reserved = await db.QueryAsync("UPDATE compensation_app.inventory SET available=available-@quantity OUTPUT inserted.available WHERE sku='book' AND available>=@quantity", ct, ("quantity", quantity));
                Host.Check(reserved.Count == 1, "Insufficient stock");
                await db.ExecAsync("INSERT INTO compensation_app.orders(job_id,quantity,state) VALUES(@id,@quantity,'reserved')", ct, ("id", job.Id), ("quantity", quantity));
                return Json.Value(new { reservation_id = job.Id, quantity });
            });
            await saga.StepOnceAsync("charge", "refund", ct => Payment(provider, ChargeKey(job.Key), amount, null, ct));
            var shipping = await saga.CheckpointAsync("shipment", (_, _) => Task.FromResult(Json.Value(new { accepted = false, reason = "destination unsupported" })));
            if (!shipping.GetProperty("accepted").GetBoolean()) await saga.CompensateAsync(Json.Value(new { reason = "destination unsupported" }));
            return shipping;
        }, async (name, receipt, db, ct) =>
        {
            if (name == "refund") return await Payment(provider, RefundKey(job.Key), Host.Long(receipt, "amount"), Host.Text(receipt, "charge_key"), ct);
            if (name != "release-inventory") throw new InvalidOperationException("Unknown compensation: " + name);
            var id = Host.Long(receipt, "reservation_id");
            var row = (await db.QueryAsync("UPDATE compensation_app.orders SET released=1,state='compensated' OUTPUT inserted.quantity WHERE job_id=@id AND released=0", ct, ("id", id))).Single();
            await db.ExecAsync("UPDATE compensation_app.inventory SET available=available+@quantity WHERE sku='book'", ct, ("quantity", row.Long("quantity")));
            return Json.Value(new { released = id });
        });
        return Json.Value(new { state = outcome.IsCompensated ? "compensated" : "shipped", result = outcome.Value });
    }
    public static async Task Reconcile(Db db, SqlConnection provider, string key, CancellationToken ct)
    {
        var job = (await db.QueryAsync("SELECT id,input FROM resume.jobs WHERE workflow=@workflow AND [key]=@key", ct, ("workflow", Name), ("key", key))).Single();
        var paymentKey = ChargeKey(key);
        var rows = await new Db(provider).QueryAsync("SELECT amount FROM payment_provider.charges WHERE [key]=@key", ct, ("key", paymentKey));
        Host.Check(rows.Count == 1, "Outcome still unknown; absence is not proof of failure");
        var amount = rows[0].Long("amount");
        Host.Check(amount == Host.Long(job.Json("input"), "amount"), "Receipt does not match order");
        await Workflow.ResolveStepAsync(db, job.Long("id"), "charge", Json.Value(new { charge_key = paymentKey, amount }), ct);
    }
    public static async Task RunAsync(string[] args, CancellationToken ct)
    {
        await using var connection = await Host.ConnectAsync(ct: ct);
        await using var provider = await Host.ConnectAsync(true, ct);
        var db = new Db(connection);
        switch (args)
        {
            case ["init"]: await Initialize(connection, provider); break;
            case ["submit", var key, var quantity, var amount]:
                Console.WriteLine(await Workflow.SubmitAsync(db, Name, key, Json.Value(new { quantity = long.Parse(quantity), amount = long.Parse(amount) }), ct: ct)); break;
            case ["run"]: Console.WriteLine(await Workflow.RunOneAsync(connection, Name, (job, steps, _) => Fulfill(job, steps, provider), Policy, ct)); break;
            case ["reconcile", var key]: await Reconcile(db, provider, key, ct); Console.WriteLine("Verified; still paused"); break;
            case ["resume", var key]:
                var job = (await db.QueryAsync("SELECT id FROM resume.jobs WHERE workflow=@workflow AND [key]=@key", ct, ("workflow", Name), ("key", key))).Single();
                await Workflow.RequeueAsync(db, job.Long("id"), ct: ct); break;
            case ["status", var key]:
                var rows = await db.QueryAsync("SELECT j.id,j.status,j.saga_phase,j.last_error,j.failures,o.state,o.released FROM resume.job_status j LEFT JOIN compensation_app.orders o ON o.job_id=j.id WHERE j.workflow=@workflow AND j.[key]=@key", ct, ("workflow", Name), ("key", key));
                foreach (var row in rows) Console.WriteLine($"{row.Long("id")}: {row.Text("status")}, saga={row["saga_phase"]}, error={row["last_error"]}"); break;
            default: Console.WriteLine("compensation init | submit KEY QUANTITY CENTS | run | reconcile KEY | resume KEY | status KEY"); break;
        }
    }
}
