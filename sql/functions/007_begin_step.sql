-- Call first in the step transaction to lock the run, validate ownership, and renew its lease.
-- Returns saved output on replay. A missed deadline or an unresolved step_once start fails
-- the run and returns `failed`; the caller must commit that failure.
-- A changed step position raises RS001.
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
    v_run resume.runs;
    v_step resume.steps;
    v_other text;
    v_failed text;
begin
    v_run := resume.lock_run(p_run_id, p_attempt);
    -- An attempt whose lease expired must not start new work: another worker may claim the
    -- run as soon as this transaction ends.
    if v_run.available_at <= clock_timestamp() then
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

    if v_run.deadline_at <= clock_timestamp() then
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
