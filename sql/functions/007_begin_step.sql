-- Starts a step: locks the run, checks this attempt still holds its claim, renews the lease,
-- and records the start. Returns the output if the step already completed. Fails the run, and
-- returns the reason in `failed`, if it is past its deadline or an earlier attempt started this
-- step without completing it, so a step_once action's outcome is unknown; the caller commits
-- that. Raises with SQLSTATE RS001 if the step's position differs from when the run first
-- reached it. Call it first in the step's transaction, so the lock covers the rest of the step.
create or replace function resume.begin_step(
    p_run_id bigint,
    p_attempt bigint,
    p_key text,
    p_position integer,
    p_lease_seconds double precision
)
returns table (output jsonb, failed text)
language plpgsql
as $$
declare
    v_step resume.steps;
    v_other text;
    v_failed text;
begin
    perform resume.lock_run(p_run_id, p_attempt);
    -- An attempt whose lease expired must not start new work: another worker may claim the
    -- run as soon as this transaction ends.
    if (select r.available_at <= clock_timestamp() from resume.runs r where r.id = p_run_id) then
        raise exception 'run % lease expired', p_run_id using errcode = '55000';
    end if;
    update resume.runs r
    set available_at = clock_timestamp() + make_interval(secs => p_lease_seconds)
    where r.id = p_run_id;

    select * into v_step from resume.steps s where s.run_id = p_run_id and s.key = p_key;
    if found and v_step.position <> p_position then
        raise exception 'workflow changed: step % ran at position %, but is now at %',
            p_key, v_step.position, p_position using errcode = 'RS001';
    end if;
    if not found then
        select s.key into v_other from resume.steps s
        where s.run_id = p_run_id and s.position = p_position;
        if found then
            raise exception 'workflow changed: position % ran step %, but is now step %',
                p_position, v_other, p_key using errcode = 'RS001';
        end if;
    end if;

    if v_step.completed_at is not null then
        return query select v_step.output, null::text;
        return;
    end if;

    if (select r.deadline_at <= clock_timestamp() from resume.runs r where r.id = p_run_id) then
        v_failed := 'the run passed its deadline';
    elsif v_step.run_id is not null then
        v_failed := format('step %s started in an earlier attempt and its outcome is unknown', p_key);
    end if;
    if v_failed is not null then
        update resume.runs set failed_at = clock_timestamp(), last_error = v_failed
        where id = p_run_id;
        return query select null::jsonb, v_failed;
        return;
    end if;

    insert into resume.steps (run_id, key, position, started_at)
    values (p_run_id, p_key, p_position, now());
    return query select null::jsonb, null::text;
end;
$$;
