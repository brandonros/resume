using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Resume;

namespace Resume.Examples;

public static class Orders
{
    public const string OrderWorkflow = "orders-app:order:v1", ShipWorkflow = "orders-app:ship:v1";
    public static async Task StockAsync(SqlConnection connection, string sku, long quantity)
    {
        if (string.IsNullOrWhiteSpace(sku) || sku.Length > 256 || sku != sku.TrimEnd() || quantity <= 0) throw new ArgumentException("Stock requires a SKU and positive quantity");
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.Serializable);
        await new Db(connection, tx).ExecAsync("UPDATE orders_app.inventory WITH (UPDLOCK,HOLDLOCK) SET available=available+@q WHERE sku=@sku; IF @@ROWCOUNT=0 INSERT orders_app.inventory VALUES(@sku,@q);", ("q", quantity), ("sku", sku));
        await tx.CommitAsync();
    }
    public static async Task<long> PlaceAsync(SqlConnection connection, string key, string sku, long quantity)
    {
        if (string.IsNullOrWhiteSpace(key) || key.Length > 512 || key != key.TrimEnd() || string.IsNullOrWhiteSpace(sku) || sku.Length > 256 || sku != sku.TrimEnd() || quantity <= 0) throw new ArgumentException("Order requires a key, SKU and positive quantity");
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync();
        var db = new Db(connection, tx);
        var id = await Workflow.SubmitAsync(db, OrderWorkflow, key, Json.Value(new { sku, quantity }));
        await db.ExecAsync("IF NOT EXISTS(SELECT 1 FROM orders_app.orders WHERE [key]=@key) INSERT orders_app.orders([key],sku,quantity,job_id) VALUES(@key,@sku,@q,@id)",
            ("key", key), ("sku", sku), ("q", quantity), ("id", id));
        await tx.CommitAsync(); return id;
    }
    public static async Task<JsonElement> Fulfill(Job job, Steps steps, CancellationToken ct)
    {
        var sku = Host.Text(job.Input, "sku"); var quantity = Host.Long(job.Input, "quantity");
        var reserved = await steps.StepAsync("reserve", async (db, token) =>
        {
            var changed = (await db.QueryAsync("UPDATE orders_app.inventory SET available=available-@q OUTPUT inserted.available WHERE sku=@sku AND available>=@q", token, ("q", quantity), ("sku", sku))).Count;
            var state = changed == 1 ? "reserved" : "rejected";
            await db.ExecAsync("UPDATE orders_app.orders SET status=@state WHERE [key]=@key", token, ("state", state), ("key", job.Key));
            return Json.Value(changed == 1);
        });
        if (!reserved.GetBoolean()) return Json.Value(new { status = "rejected", reason = "insufficient stock" });
        var child = await steps.SpawnAsync("dispatch", ShipWorkflow, Json.Value(new { order = job.Key }));
        var receipt = await steps.WaitForAsync(child);
        await steps.StepAsync("mark-shipped", async (db, token) =>
        {
            await db.ExecAsync("UPDATE orders_app.orders SET status='shipped' WHERE [key]=@key", token, ("key", job.Key)); return Json.Null;
        });
        return Json.Value(new { status = "shipped", receipt });
    }
    public static Task<JsonElement> Dispatch(Job job, Steps steps, CancellationToken ct) => steps.StepAsync("record-dispatch", async (db, token) =>
    {
        var receipt = $"dispatch-{job.Id}";
        await db.ExecAsync("INSERT orders_app.shipments VALUES(@key,@receipt)", token, ("key", Host.Text(job.Input, "order")), ("receipt", receipt));
        return Json.Value(receipt);
    });
    public static async Task<JsonElement> StatusAsync(Db db, string key)
    {
        var row = (await db.QueryAsync("SELECT o.[key],o.sku,o.quantity,o.status,j.id,j.status AS job_status,j.attempt,j.last_error,s.receipt FROM orders_app.orders o JOIN resume.job_status j ON j.id=o.job_id LEFT JOIN orders_app.shipments s ON s.order_key=o.[key] WHERE o.[key]=@key", ("key", key))).Single();
        row["children"] = await db.QueryAsync("SELECT id,status,failures,last_error FROM resume.job_status WHERE parent_id=@id ORDER BY id", ("id", row.Long("id")));
        return Json.Value(row);
    }
    public static async Task DrainAsync(SqlConnection connection, CancellationToken ct = default)
    {
        while (true)
        {
            var order = await Workflow.RunOneAsync(connection, OrderWorkflow, Fulfill, Host.Retry, ct: ct);
            var child = await Workflow.RunOneAsync(connection, ShipWorkflow, Dispatch, Host.Retry, ct: ct);
            if (!order && !child) return;
        }
    }
    public static async Task RunAsync(string[] args, CancellationToken ct)
    {
        await using var connection = await Host.ConnectAsync(ct: ct); var db = new Db(connection);
        switch (args)
        {
            case ["init"]: await Host.InstallAsync(db, "orders"); break;
            case ["stock", var sku, var count]: await StockAsync(connection, sku, long.Parse(count)); break;
            case ["submit", var key, var sku, var count]: Console.WriteLine(await PlaceAsync(connection, key, sku, long.Parse(count))); break;
            case ["status", var key]: Host.Print(await StatusAsync(db, key)); break;
            case ["drain"]: await DrainAsync(connection, ct); break;
            case ["work"]:
                await using (var ships = await Host.ConnectAsync(ct: ct))
                    await Host.WorkersAsync(ct, token => Workflow.WorkAsync(connection, OrderWorkflow, Fulfill, Host.Retry, ct: token), token => Workflow.WorkAsync(ships, ShipWorkflow, Dispatch, Host.Retry, ct: token));
                break;
            default: throw new ArgumentException("orders init | stock SKU QUANTITY | submit KEY SKU QUANTITY | status KEY | drain | work");
        }
    }
}
