-- Returns the run for this key, creating it if needed. A key maps to one run for good,
-- even after that run fails, so a step_once action cannot repeat through a new run.
create or replace function resume.submit_run(
    p_workflow text,
    p_version text,
    p_idempotency_key text,
    p_input jsonb,
    p_max_attempts integer,
    p_retry_delay_seconds double precision,
    p_retry_max_delay_seconds double precision,
    p_subject text,
    p_deadline_seconds double precision,
    p_delay_seconds double precision,
    p_at timestamptz,
    p_on_failure_workflow text,
    p_on_failure_version text
)
returns table (run_id bigint, created boolean)
language plpgsql
as $$
declare
    v_run resume.runs;
    v_now timestamptz := clock_timestamp();
    v_deadline_at timestamptz := v_now + make_interval(secs => p_deadline_seconds);
begin
    if p_delay_seconds is null or p_delay_seconds < 0
       or p_delay_seconds >= 'Infinity'::double precision then
        raise exception 'submit delay seconds must be finite and nonnegative' using errcode = '22023';
    end if;
    if p_at is not null and (not isfinite(p_at) or p_delay_seconds <> 0) then
        raise exception 'submit at must be finite and cannot be combined with a delay' using errcode = '22023';
    end if;
    insert into resume.runs (
        workflow, version, idempotency_key, input, subject, max_attempts, retry_delay, retry_max_delay,
        deadline_at, available_at, on_failure_workflow, on_failure_version
    )
    values (
        p_workflow, p_version, p_idempotency_key, p_input, p_subject, p_max_attempts,
        make_interval(secs => p_retry_delay_seconds),
        make_interval(secs => p_retry_max_delay_seconds),
        v_deadline_at,
        -- Wake for an earlier deadline so the claim sweep can fail the run on time.
        least(coalesce(p_at, v_now + make_interval(secs => p_delay_seconds)), v_deadline_at),
        p_on_failure_workflow, p_on_failure_version
    )
    on conflict (workflow, idempotency_key) do nothing
    returning * into v_run;

    if not found then
        -- A separate statement sees the winning insert after a conflict.
        select * into v_run from resume.runs r
        where r.workflow = p_workflow and r.idempotency_key = p_idempotency_key;

        if v_run.input is distinct from p_input or v_run.subject is distinct from p_subject
           or v_run.version is distinct from p_version then
            raise exception 'idempotency key % already used with a different input, subject or version',
                p_idempotency_key
                using errcode = '23505';
        end if;
        return query select v_run.id, false;
        return;
    end if;

    return query select v_run.id, true;
end;
$$;
