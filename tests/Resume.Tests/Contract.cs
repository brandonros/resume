using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Resume;
using Resume.Examples;

namespace Resume.Tests;

public static class Contract
{
    static string Name() => "test-" + Guid.NewGuid().ToString("N");
    static Retry Stop(Job _, Exception __) => Retry.Stop;
    static Task<JsonElement> Null(Db _, CancellationToken __) => Task.FromResult(Json.Null);
    static Task Expire(Db db, long id) => db.ExecAsync("UPDATE resume.jobs SET available_at=DATEADD(SECOND,-1,SYSUTCDATETIME()) WHERE id=@id", ("id", id));
    static async Task<Row> State(Db db, long id) => (await db.QueryAsync("SELECT * FROM resume.jobs WHERE id=@id", ("id", id))).Single();
    static async Task SqlError(Func<Task> action, int number) => Check.Equal((await Check.Throws<SqlException>(action)).Number, number);
    public static async Task Install()
    {
        await using var c = await Host.ConnectAsync();
        // Never drop an existing schema: the caller must provide an empty disposable DB.
        await SqlScript.ApplyAsync(new Db(c), File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "schema.sql")));
        await new Db(c).ExecAsync("CREATE TABLE dbo.test_effects(job_id bigint NOT NULL,label nvarchar(100) NOT NULL,PRIMARY KEY(job_id,label))");
    }
    public static async Task Submission()
    {
        await using var c = await Host.ConnectAsync(); var db = new Db(c); var name = Name();
        var id = await Workflow.SubmitAsync(db, name, "key", Json.Parse("{\"b\":2,\"a\":1.0}"));
        Check.Equal(await Workflow.SubmitAsync(db, name, "key", Json.Parse("{\"a\":1,\"b\":2}")), id);
        await SqlError(() => Workflow.SubmitAsync(db, name, "key", Json.Null), 50005);
        Check.That(await Workflow.SubmitAsync(db, name, "key ", Json.Null) != id, "Trailing spaces must remain distinct");
        Check.That(await Workflow.SubmitAsync(db, name, "KEY", Json.Null) != id, "Case must remain distinct");
        long rolledBack;
        await using (var tx = (SqlTransaction)await c.BeginTransactionAsync())
        { rolledBack = await Workflow.SubmitAsync(new Db(c, tx), name, "rollback", Json.Null); await tx.RollbackAsync(); }
        Check.Equal((await db.QueryAsync("SELECT id FROM resume.jobs WHERE id=@id", ("id", rolledBack))).Count, 0);
    }
    public static async Task Replay()
    {
        await using var c = await Host.ConnectAsync(); var db = new Db(c); var name = Name();
        var id = await Workflow.SubmitAsync(db, name, "key", Json.Null); var calls = 0; var fail = true;
        Handler handler = async (job, steps, _) =>
        {
            await steps.StepAsync("saved", async (tx, ct) => { calls++; await tx.ExecAsync("INSERT INTO dbo.test_effects VALUES(@id,'saved')", ct, ("id", job.Id)); return Json.Null; });
            return await steps.StepAsync("atomic", async (tx, ct) =>
            {
                await tx.ExecAsync("INSERT INTO dbo.test_effects VALUES(@id,'atomic')", ct, ("id", job.Id));
                if (fail) throw new IOException("retry me");
                return Json.Value(42);
            });
        };
        await Check.Throws<IOException>(() => Workflow.RunOneAsync(c, name, handler, (_, _) => Retry.After(TimeSpan.Zero)));
        Check.Equal((await db.QueryAsync("SELECT * FROM dbo.test_effects WHERE job_id=@id", ("id", id))).Count, 1);
        fail = false;
        Check.That(await Workflow.RunOneAsync(c, name, handler, Stop));
        Check.Equal(calls, 1); Check.Equal((await State(db, id)).Json("output").GetInt32(), 42);
        Check.That(!await Workflow.RunOneAsync(c, name, handler, Stop));
    }
    public static async Task Claims()
    {
        await using var a = await Host.ConnectAsync(); await using var b = await Host.ConnectAsync();
        var db = new Db(a); var other = new Db(b); var name = Name(); var id = await Workflow.SubmitAsync(db, name, "key", Json.Null);
        var claims = await Task.WhenAll(db.ProcAsync("claim", default, ("workflow", name)), other.ProcAsync("claim", default, ("workflow", name)));
        Check.Equal(claims.Sum(c => c.Count), 1);
        await Expire(db, id);
        var current = (await other.ProcAsync("claim", default, ("workflow", name))).Single(); Check.Equal(current.Long("attempt"), 2L);
        await SqlError(() => db.ProcAsync("begin_step", default, ("id", id), ("attempt", 1L)), 50001);
        await SqlError(() => Workflow.RequeueAsync(db, id), 50004);
        await other.ProcAsync("finish", default, ("id", id), ("attempt", 2L));
    }
    public static async Task LockedStep()
    {
        await using var a = await Host.ConnectAsync(); await using var b = await Host.ConnectAsync();
        var db = new Db(a); var name = Name(); var id = await Workflow.SubmitAsync(db, name, "key", Json.Null);
        await db.ProcAsync("claim", default, ("workflow", name));
        await using var tx = (SqlTransaction)await a.BeginTransactionAsync(); var locked = new Db(a, tx);
        await locked.ProcAsync("start_step", default, ("id", id), ("attempt", 1L), ("key", "step"), ("position", 0), ("once", false));
        await Expire(locked, id);
        Check.Equal((await new Db(b).ProcAsync("claim", default, ("workflow", name))).Count, 0);
        await locked.ProcAsync("save_step", default, ("id", id), ("attempt", 1L), ("key", "step"), ("output", "null"));
        await tx.CommitAsync();
        Check.Equal((await new Db(b).ProcAsync("claim", default, ("workflow", name))).Single().Long("attempt"), 2L);
        await SqlError(() => db.ProcAsync("save_step", default, ("id", id), ("attempt", 1L), ("key", "step"), ("output", "42")), 50001);
    }
    public static Task Recovery() => Demos.Recovery(default);
    public static async Task OnceAndRecovery()
    {
        await using var c = await Host.ConnectAsync(); var db = new Db(c); var name = Name(); var calls = 0;
        var id = await Workflow.SubmitAsync(db, name, "key", Json.Null);
        Handler handler = async (_, steps, _) =>
        {
            var output = await steps.StepOnceAsync("null", _ => { calls++; return Task.FromResult(Json.Null); });
            Check.Equal(output.ValueKind, JsonValueKind.Null);
            if (calls == 1) throw new IOException();
            return output;
        };
        await Check.Throws<IOException>(() => Workflow.RunOneAsync(c, name, handler, (_, _) => Retry.After(TimeSpan.Zero)));
        await Workflow.RunOneAsync(c, name, (_, steps, _) => steps.StepOnceAsync("null", _ => { calls++; return Task.FromResult(Json.Null); }), Stop);
        Check.Equal(calls, 1);
        name = Name(); id = await Workflow.SubmitAsync(db, name, "unknown", Json.Null);
        await SqlError(() => Workflow.RunOneAsync(c, name, async (_, steps, _) =>
        {
            try { await steps.StepOnceAsync("external", _ => throw new IOException()); } catch (IOException) { }
            return Json.Null;
        }, Stop), 50002);
        await using (var tx = (SqlTransaction)await c.BeginTransactionAsync())
        {
            var atomic = new Db(c, tx);
            await Workflow.ResolveStepAsync(atomic, id, "external", Json.Null);
            await Workflow.RequeueAsync(atomic, id);
            await tx.RollbackAsync();
        }
        Check.That((await State(db, id)).Bool("paused"));
        await SqlError(() => Workflow.RequeueAsync(db, id), 50004);
        await Workflow.ResolveStepAsync(db, id, "external", Json.Null);
        await Workflow.RequeueAsync(db, id);
        await Workflow.RunOneAsync(c, name, (_, steps, _) => steps.StepOnceAsync("external", _ => throw new InvalidOperationException("Must replay")), Stop);
    }
    public static async Task History()
    {
        foreach (var variant in new[] { "rename", "omit", "mode", "duplicate" })
        {
            await using var c = await Host.ConnectAsync(); var db = new Db(c); var name = Name(); var id = await Workflow.SubmitAsync(db, name, "key", Json.Null);
            await Check.Throws<IOException>(() => Workflow.RunOneAsync(c, name, async (_, steps, _) =>
            { await steps.StepAsync("first", Null); throw new IOException(); }, Stop));
            await Workflow.RequeueAsync(db, id);
            var invoked = false;
            Handler changed = async (_, steps, _) =>
            {
                if (variant == "omit") return Json.Null;
                if (variant == "mode") return await steps.StepOnceAsync("first", _ => { invoked = true; return Task.FromResult(Json.Null); });
                await steps.StepAsync(variant == "rename" ? "renamed" : "first", (_, _) => { invoked = true; return Task.FromResult(Json.Null); });
                if (variant == "duplicate") await steps.StepAsync("first", Null);
                return Json.Null;
            };
            await Check.Throws<Exception>(() => Workflow.RunOneAsync(c, name, changed, Stop));
            Check.That(!invoked); Check.That((await State(db, id)).Bool("paused"));
        }
    }
    public static async Task RetryPolicy()
    {
        await using var c = await Host.ConnectAsync(); var db = new Db(c); var name = Name(); var id = await Workflow.SubmitAsync(db, name, "key", Json.Null);
        Handler fail = (_, _, _) => throw new ArgumentException("invalid input");
        await Check.Throws<ArgumentException>(() => Workflow.RunOneAsync(c, name, fail, (job, error) =>
        { Check.Equal(job.Failures, 0L); Check.That(error is ArgumentException); return Retry.After(TimeSpan.FromHours(1)); }));
        Check.That(!await Workflow.RunOneAsync(c, name, fail, Stop));
        await Expire(db, id);
        await Check.Throws<ArgumentException>(() => Workflow.RunOneAsync(c, name, fail, (job, _) => { Check.Equal(job.Failures, 1L); return Retry.Stop; }));
        Check.That((await State(db, id)).Bool("paused"));
        await SqlError(() => db.ProcAsync("requeue", default, ("id", id), ("delay_seconds", -1d)), 50003);
        await Workflow.RequeueAsync(db, id);
        await Check.Throws<IOException>(() => Workflow.WorkAsync(c, name, fail, (_, _) => throw new IOException("policy failure")));
    }
    public static async Task Sleep()
    {
        await using var c = await Host.ConnectAsync(); var db = new Db(c); var name = Name(); var id = await Workflow.SubmitAsync(db, name, "key", Json.Null);
        Handler handler = async (_, steps, _) => { await steps.SleepAsync("sleep", TimeSpan.FromMilliseconds(150)); return Json.Value("awake"); };
        await Workflow.RunOneAsync(c, name, handler, Stop);
        Check.Equal((await State(db, id)).Long("failures"), 0L);
        await Task.Delay(200);
        await Workflow.RunOneAsync(c, name, handler, Stop);
        Check.That((await State(db, id)).Bool("completed"));
        var future = Name(); await Workflow.SubmitAsync(db, future, "key", Json.Null, DateTimeOffset.UtcNow.AddHours(1));
        Check.That(!await Workflow.RunOneAsync(c, future, handler, Stop));
    }
    public static async Task Children()
    {
        for (var i = 0; i < 12; i++)
        {
            await using var a = await Host.ConnectAsync(); await using var b = await Host.ConnectAsync();
            var db = new Db(a); var parentName = Name(); var childName = Name();
            var parent = await Workflow.SubmitAsync(db, parentName, "parent", Json.Null);
            var child = await Workflow.SubmitAsync(db, childName, "child", Json.Null, parentId: parent);
            await db.ProcAsync("claim", default, ("workflow", parentName));
            await new Db(b).ProcAsync("claim", default, ("workflow", childName));
            var wait = db.ProcAsync("wait_for", default, ("id", parent), ("attempt", 1L), ("child", child));
            var finish = new Db(b).ProcAsync("finish", default, ("id", child), ("attempt", 1L), ("output", "42"));
            await Task.WhenAll(wait, finish);
            var state = await State(db, parent);
            Check.That(state["available_at"] is not null, "Lost parent wakeup");
            if (!state.Bool("leased")) await db.ProcAsync("claim", default, ("workflow", parentName));
            var attempt = (await State(db, parent)).Long("attempt");
            var receipt = (await db.ProcAsync("wait_for", default, ("id", parent), ("attempt", attempt), ("child", child))).Single();
            Check.Equal(receipt.Json("output").GetInt32(), 42);
        }
    }
    public static Task Cancellation() => CancelStep(false);
    public static Task OnceCancellation() => CancelStep(true);
    static async Task CancelStep(bool once)
    {
        await using var c = await Host.ConnectAsync(); var db = new Db(c); var name = Name(); var id = await Workflow.SubmitAsync(db, name, "key", Json.Null);
        using var stop = new CancellationTokenSource(); var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Handler handler = (_, steps, _) => once ? steps.StepOnceAsync("work", async token =>
        { entered.SetResult(); await Task.Delay(Timeout.Infinite, token); return Json.Null; }) : steps.StepAsync("work", async (tx, token) =>
        { await tx.ExecAsync("INSERT INTO dbo.test_effects VALUES(@id,'cancel')", token, ("id", id)); entered.SetResult(); await Task.Delay(Timeout.Infinite, token); return Json.Null; });
        var run = Workflow.RunOneAsync(c, name, handler, Stop, stop.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10)); await stop.CancelAsync();
        await Check.Throws<OperationCanceledException>(() => run);
        // Dispose completes rollback before observing from a replacement connection.
        await c.DisposeAsync();
        await using var observer = await Host.ConnectAsync(); var view = new Db(observer);
        Check.Equal((await view.QueryAsync("SELECT * FROM dbo.test_effects WHERE job_id=@id", ("id", id))).Count, 0);
        Check.Equal((await view.QueryAsync("SELECT * FROM resume.steps WHERE job_id=@id", ("id", id))).Count, once ? 1 : 0);
        Check.Equal((await State(view, id)).Long("failures"), 0L);
    }
    public static async Task Sagas()
    {
        await using var c = await Host.ConnectAsync(); var db = new Db(c); var name = Name(); var id = await Workflow.SubmitAsync(db, name, "key", Json.Null);
        var forwardCalls = 0; var undo = new List<string>(); var fail = true;
        Handler handler = async (_, steps, _) =>
        {
            var outcome = await steps.SagaAsync(async (saga, _) =>
            {
                forwardCalls++;
                await saga.StepAsync("a", "undo-a", Null); await saga.StepAsync("b", "undo-b", Null);
                await saga.CompensateAsync(Json.Value("abandon")); return Json.Null;
            }, (name, receipt, tx, ct) =>
            {
                if (name == "undo-a" && fail) throw new IOException("undo interrupted");
                undo.Add(name); return Task.FromResult(Json.Null);
            });
            Check.That(outcome.IsCompensated); return outcome.Value;
        };
        await Check.Throws<IOException>(() => Workflow.RunOneAsync(c, name, handler, (_, _) => Retry.After(TimeSpan.Zero)));
        fail = false; await Workflow.RunOneAsync(c, name, handler, Stop);
        Check.Equal(forwardCalls, 1); Check.Equal(string.Join(",", undo), "undo-b,undo-a");
        Check.Equal((await State(db, id)).Text("saga_phase"), "compensated");
        var success = Name(); await Workflow.SubmitAsync(db, success, "key", Json.Null);
        await Workflow.RunOneAsync(c, success, async (_, steps, _) => (await steps.SagaAsync((s, _) => s.StepAsync("a", "undo-a", Null), (_, _, _, _) => throw new InvalidOperationException())).Value, Stop);
    }
    public static async Task SagaUnknown()
    {
        await using var c = await Host.ConnectAsync(); var db = new Db(c); var name = Name(); var id = await Workflow.SubmitAsync(db, name, "key", Json.Null); var undone = false;
        await Check.Throws<SqlException>(() => Workflow.RunOneAsync(c, name, async (_, steps, _) =>
            (await steps.SagaAsync(async (saga, _) =>
            {
                try { await saga.StepOnceAsync("external", "undo", _ => throw new IOException()); } catch (IOException) { }
                await saga.CompensateAsync(Json.Null); return Json.Null;
            }, (_, _, _, _) => { undone = true; return Task.FromResult(Json.Null); })).Value, Stop));
        Check.That(!undone); Check.Equal((await State(db, id)).Text("saga_phase"), "forward");
    }
    public static async Task Stress()
    {
        await using var submitter = await Host.ConnectAsync(); var db = new Db(submitter); var name = Name();
        for (var i = 0; i < 40; i++) await Workflow.SubmitAsync(db, name, i.ToString(), Json.Value(i));
        await Task.WhenAll(Enumerable.Range(0, 4).Select(async _ =>
        {
            await using var c = await Host.ConnectAsync();
            while (await Workflow.RunOneAsync(c, name, (job, steps, _) => steps.StepAsync("effect", async (tx, ct) =>
            { await tx.ExecAsync("INSERT INTO dbo.test_effects VALUES(@id,'stress')", ct, ("id", job.Id)); return job.Input; }), Stop)) { }
        }));
        var rows = await db.QueryAsync("SELECT j.completed,j.attempt FROM resume.jobs j WHERE workflow=@name", ("name", name));
        Check.Equal(rows.Count, 40); Check.That(rows.All(r => r.Bool("completed") && r.Long("attempt") == 1));
    }
    public static async Task Applications()
    {
        await using var c = await Host.ConnectAsync(); var db = new Db(c);
        await Host.InstallAsync(db, "orders"); await Host.InstallAsync(db, "importer");
        await Orders.StockAsync(c, "book", 5);
        await Orders.PlaceAsync(c, "test-order", "book", 2);
        await Orders.DrainAsync(c, default);
        var order = (await db.QueryAsync("SELECT status FROM orders_app.orders WHERE [key]='test-order'")).Single();
        Check.Equal(order.Text("status"), "shipped");
        var importId = await Importer.EnqueueAsync(db, "test-import", "{\"id\":\"event-1\",\"account\":\"a\",\"amount_cents\":100}\ninvalid\n");
        await Importer.DrainAsync(c);
        var result = (await State(db, importId)).Json("output");
        Check.Equal(result.GetProperty("accepted").GetInt64(), 1L);
        Check.Equal(result.GetProperty("rejected").GetInt64(), 1L);
    }
    public static async Task CompensationExample()
    {
        await using var c = await Host.ConnectAsync(); await using var provider = await Host.ConnectAsync(true); var db = new Db(c);
        await Compensation.Initialize(c, provider); var key = Name();
        var id = await Workflow.SubmitAsync(db, Compensation.Name, key, Json.Value(new { quantity = 2, amount = 500 }));
        Handler handler = (job, steps, _) => Compensation.Fulfill(job, steps, provider);
        await Check.Throws<Compensation.ResponseLost>(() => Workflow.RunOneAsync(c, Compensation.Name, handler, Compensation.Policy));
        await Compensation.Reconcile(db, provider, key, default);
        Check.That((await State(db, id)).Bool("paused")); await Workflow.RequeueAsync(db, id);
        await Check.Throws<Compensation.ResponseLost>(() => Workflow.RunOneAsync(c, Compensation.Name, handler, Compensation.Policy));
        await Workflow.RunOneAsync(c, Compensation.Name, handler, Compensation.Policy);
        Check.That((await State(db, id)).Bool("completed"));
        Check.Equal((await new Db(provider).QueryAsync("SELECT calls FROM payment_provider.charges WHERE [key]=@key", ("key", Compensation.ChargeKey(key)))).Single().Long("calls"), 1L);
        Check.Equal((await new Db(provider).QueryAsync("SELECT calls FROM payment_provider.refunds WHERE [key]=@key", ("key", Compensation.RefundKey(key)))).Single().Long("calls"), 2L);
    }
    public static async Task ConnectionLoss()
    {
        await using var worker = await Host.ConnectAsync(); await using var control = await Host.ConnectAsync(); var db = new Db(control);
        var spid = (await new Db(worker).QueryAsync("SELECT @@SPID AS id")).Single().Int("id");
        var name = Name(); var id = await Workflow.SubmitAsync(db, name, "key", Json.Null);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var killed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var run = Workflow.RunOneAsync(worker, name, (_, steps, _) => steps.StepAsync("effect", async (tx, ct) =>
        {
            await tx.ExecAsync("INSERT INTO dbo.test_effects VALUES(@id,'lost')", ct, ("id", id)); entered.SetResult();
            await killed.Task.WaitAsync(ct); return Json.Null;
        }), Stop);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        try { await db.ExecAsync($"KILL {spid}"); } finally { killed.TrySetResult(); }
        await Check.Throws<Exception>(() => run);
        Check.Equal((await db.QueryAsync("SELECT * FROM dbo.test_effects WHERE job_id=@id", ("id", id))).Count, 0);
        await Expire(db, id);
        await using var replacement = await Host.ConnectAsync();
        await Workflow.RunOneAsync(replacement, name, (_, steps, _) => steps.StepAsync("effect", Null), Stop);
        Check.That((await State(db, id)).Bool("completed"));
    }
}
