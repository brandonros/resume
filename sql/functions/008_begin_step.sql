-- Starts a step: locks the run, checks this attempt still holds its claim, renews the lease,
-- and records the start. Returns the output if the step already completed, whether an earlier
-- attempt started it without completing it, whether the run is past its deadline, and why
-- step_if skipped it, if it did. Raises
-- with SQLSTATE RS001 if the step's position differs from when the run first reached it. Call
-- it first in the step's transaction, so the lock covers the rest of the step.
create or replace function resume.begin_step(
    p_run_id bigint,
    p_attempt bigint,
    p_idempotency_key text,
    p_position integer,
    p_lease_seconds double precision
)
returns table (output jsonb, interrupted boolean, past_deadline boolean, skipped text)
language plpgsql
as $$
declare
    v_step resume.steps;
    v_other text;
    v_past_deadline boolean;
begin
    perform resume.lock_run(p_run_id, p_attempt);
    -- An attempt whose lease expired must not start new work: another worker may claim the
    -- run as soon as this transaction ends.
    if (select r.available_at <= clock_timestamp() from resume.runs r where r.id = p_run_id) then
        raise exception 'run % lease expired', p_run_id using errcode = '55000';
    end if;
    update resume.runs r
    set available_at = clock_timestamp() + make_interval(secs => p_lease_seconds)
    where r.id = p_run_id
    returning coalesce(r.deadline_at <= clock_timestamp(), false) into v_past_deadline;

    select * into v_step from resume.steps s
    where s.run_id = p_run_id and s.idempotency_key = p_idempotency_key;
    if found then
        if v_step.position <> p_position then
            raise exception 'workflow changed: step % ran at position %, but is now at %',
                p_idempotency_key, v_step.position, p_position using errcode = 'RS001';
        end if;
        return query select v_step.output, v_step.completed_at is null, v_past_deadline,
            v_step.skipped;
        return;
    end if;

    select s.idempotency_key into v_other from resume.steps s
    where s.run_id = p_run_id and s.position = p_position;
    if found then
        raise exception 'workflow changed: position % ran step %, but is now step %',
            p_position, v_other, p_idempotency_key using errcode = 'RS001';
    end if;

    insert into resume.steps (run_id, idempotency_key, position, started_at)
    values (p_run_id, p_idempotency_key, p_position, now());
    return query select null::jsonb, false, v_past_deadline, null::text;
end;
$$;
