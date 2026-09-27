using System.Text.Json;

namespace Resume;

public sealed class CompensatingException() : Exception("Saga is compensating");
public sealed record SagaOutcome(bool IsCompensated, JsonElement Value);

/// <summary>One sequential saga per job. Compensation names are stable durable handler identifiers.</summary>
public sealed class Saga
{
    private readonly Steps steps;
    private readonly Func<JsonElement, Task> decide;
    internal bool Compensating { get; private set; }
    internal JsonElement Reason { get; private set; } = Json.Null;
    internal Saga(Steps steps, Func<JsonElement, Task> decide) => (this.steps, this.decide) = (steps, decide);
    private void Check() { if (Compensating) throw new CompensatingException(); }
    public Task<JsonElement> StepAsync(string key, string compensation, Func<Db, CancellationToken, Task<JsonElement>> action)
    { Check(); return steps.StepRegistered(key, compensation, false, action); }
    public Task<JsonElement> StepOnceAsync(string key, string compensation, Func<CancellationToken, Task<JsonElement>> action)
    { Check(); return steps.OnceRegistered(key, compensation, action); }
    public Task<JsonElement> CheckpointAsync(string key, Func<Db, CancellationToken, Task<JsonElement>> action)
    { Check(); return steps.StepAsync(key, action); }
    public async Task CompensateAsync(JsonElement reason)
    {
        Check(); await decide(reason); Compensating = true; Reason = reason.Clone(); throw new CompensatingException();
    }
}

public sealed partial class Steps
{
    /// <summary>Explicit compensation; ordinary failures use retry policy. Undo effects must be idempotent externally.</summary>
    public async Task<SagaOutcome> SagaAsync(Func<Saga, CancellationToken, Task<JsonElement>> forward,
        Func<string, JsonElement, Db, CancellationToken, Task<JsonElement>> undo)
    {
        if (Position != 0 || sagaStarted || Suspended) throw new InvalidOperationException("One saga is allowed per job, before other steps");
        sagaStarted = true;
        Enter();
        Row row;
        try { row = (await new Db(connection).ProcAsync("begin_saga", token, ("id", id), ("attempt", attempt))).Single(); }
        finally { Volatile.Write(ref busy, 0); }
        var phase = row.Text("saga_phase");
        var reason = row["saga_result"] is null ? Json.Null : row.Json("saga_result");
        if (phase != "forward") Position = row.Int("saga_position");
        if (phase is "completed" or "compensated") return new(phase == "compensated", reason);
        if (phase == "forward")
        {
            var saga = new Saga(this, async decision =>
            {
                Enter();
                try { await new Db(connection).ProcAsync("compensate", token, ("id", id), ("attempt", attempt), ("position", Position), ("reason", Json.Canonical(decision))); }
                finally { Volatile.Write(ref busy, 0); }
            });
            JsonElement output = Json.Null;
            try { output = await forward(saga, token); }
            catch when (saga.Compensating && !token.IsCancellationRequested) { }
            if (!saga.Compensating) { await EndSaga(output); return new(false, output); }
            reason = saga.Reason;
        }
        var actions = await new Db(connection).QueryAsync("SELECT position,compensation,output FROM resume.steps WHERE job_id=@id AND compensation IS NOT NULL ORDER BY position DESC", token, ("id", id));
        foreach (var action in actions)
        {
            var name = action.Text("compensation");
            await StepRegistered($"$undo:{action.Int("position")}:{name}", null, true,
                (db, ct) => undo(name, action.Json("output"), db, ct));
        }
        await EndSaga(reason);
        return new(true, reason);
    }
    private async Task EndSaga(JsonElement output)
    {
        Enter();
        try { await new Db(connection).ProcAsync("end_saga", token, ("id", id), ("attempt", attempt), ("position", Position), ("output", Json.Canonical(output))); }
        finally { Volatile.Write(ref busy, 0); }
    }
}
