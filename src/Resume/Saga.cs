using System.Text.Json;

namespace Resume;

public sealed class CompensatingException() : Exception("Saga is compensating");

public sealed record SagaOutcome(bool IsCompensated, JsonElement Value);

/// <summary>One sequential saga per job. Compensation names are durable handler identifiers; keep them stable.</summary>
public sealed class Saga
{
    private readonly Steps steps;
    private readonly Func<JsonElement, Task> decide;

    internal bool Compensating { get; private set; }
    internal JsonElement Reason { get; private set; } = Json.Null;

    internal Saga(Steps steps, Func<JsonElement, Task> decide)
    {
        this.steps = steps;
        this.decide = decide;
    }

    public Task<JsonElement> StepAsync(string key, string compensation,
        Func<Db, CancellationToken, Task<JsonElement>> action, TimeSpan? timeout = null)
    {
        ThrowIfCompensating();
        return steps.StepRegistered(key, compensation, isCompensation: false, action, timeout);
    }

    public Task<JsonElement> StepOnceAsync(string key, string compensation,
        Func<CancellationToken, Task<JsonElement>> action, TimeSpan? timeout = null)
    {
        ThrowIfCompensating();
        return steps.OnceRegistered(key, compensation, action, timeout);
    }

    /// <summary>A forward step with nothing to undo.</summary>
    public Task<JsonElement> CheckpointAsync(string key,
        Func<Db, CancellationToken, Task<JsonElement>> action, TimeSpan? timeout = null)
    {
        ThrowIfCompensating();
        return steps.StepAsync(key, action, timeout);
    }

    /// <summary>Records the decision to compensate, then unwinds the forward function.</summary>
    public async Task CompensateAsync(JsonElement reason)
    {
        ThrowIfCompensating();
        await decide(reason);
        Compensating = true;
        Reason = reason.Clone();
        throw new CompensatingException();
    }

    private void ThrowIfCompensating()
    {
        if (Compensating) throw new CompensatingException();
    }
}

public sealed partial class Steps
{
    /// <summary>Compensation is explicit; ordinary failures use the retry policy.
    /// Undo effects must be idempotent in the external system.</summary>
    public async Task<SagaOutcome> SagaAsync(Func<Saga, CancellationToken, Task<JsonElement>> forward,
        Func<string, JsonElement, Db, CancellationToken, Task<JsonElement>> undo)
    {
        if (Position != 0 || sagaStarted || Suspended)
            throw new InvalidOperationException("One saga is allowed per job, before other steps");
        sagaStarted = true;

        var row = (await Exclusive(() => Call(new Db(connection), "begin_saga")))!;
        var phase = row.Text("saga_phase");
        var reason = row["saga_result"] is null ? Json.Null : row.Json("saga_result");
        if (phase != "forward") Position = row.Int("saga_position");

        // A replay of a finished saga returns its recorded outcome.
        if (phase is "completed" or "compensated") return new(phase == "compensated", reason);

        if (phase == "forward")
        {
            var saga = new Saga(this, decision => Exclusive(() =>
                Call(new Db(connection), "compensate", ("position", Position), ("reason", Json.Canonical(decision)))));

            var output = Json.Null;
            try
            {
                output = await forward(saga, token);
            }
            catch when (saga.Compensating && !token.IsCancellationRequested)
            {
                // CompensateAsync unwinds the forward function by throwing.
            }

            if (!saga.Compensating)
            {
                await EndSaga(output);
                return new(false, output);
            }
            reason = saga.Reason;
        }

        // Undo completed steps in reverse order. Each undo is itself a durable step.
        const string sql = "SELECT position, compensation, output FROM resume.steps WHERE job_id = @id AND compensation IS NOT NULL ORDER BY position DESC";
        foreach (var step in await new Db(connection).QueryAsync(sql, token, ("id", id)))
        {
            var name = step.Text("compensation");
            var receipt = step.Json("output");
            await StepRegistered($"$undo:{step.Int("position")}:{name}", compensation: null, isCompensation: true,
                (db, ct) => undo(name, receipt, db, ct));
        }

        await EndSaga(reason);
        return new(true, reason);
    }

    private Task EndSaga(JsonElement output) => Exclusive(() =>
        Call(new Db(connection), "end_saga", ("position", Position), ("output", Json.Canonical(output))));
}
