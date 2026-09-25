-- Starts a step: locks the run, checks this attempt still holds its claim, renews the lease,
-- and records the start. Returns the output if the step already completed, and whether an
-- earlier attempt started it without completing it. Call it first in the step's transaction,
-- so the lock covers the rest of the step.
create or replace function resume.begin_step(
    p_run_id bigint,
    p_attempt bigint,
    p_idempotency_key text,
    p_lease_seconds double precision
)
returns table (output jsonb, interrupted boolean)
language plpgsql
as $$
begin
    perform resume.lock_run(p_run_id, p_attempt);
    update resume.runs
    set available_at = clock_timestamp() + make_interval(secs => p_lease_seconds)
    where id = p_run_id;

    insert into resume.steps (run_id, idempotency_key, started_at)
    values (p_run_id, p_idempotency_key, now())
    on conflict do nothing;
    if found then
        return query select null::jsonb, false;
    else
        return query
        select s.output, s.completed_at is null
        from resume.steps s
        where s.run_id = p_run_id and s.idempotency_key = p_idempotency_key;
    end if;
end;
$$;
