-- Returns the existing run for this key or creates one. Keys remain bound to failed runs,
-- preventing step_once actions from repeating through resubmission. The 'resume:' prefix
-- is reserved for framework runs. Parameters after p_input are optional.
create or replace function resume.submit_run(
    p_workflow text,
    p_version text,
    p_idempotency_key text,
    p_input jsonb,
    p_max_attempts integer default 3,
    p_retry_delay_seconds double precision default 1,
    p_retry_max_delay_seconds double precision default 60,
    p_subject text default null,
    p_deadline_seconds double precision default null,
    p_delay_seconds double precision default 0,
    p_at timestamptz default null,
    p_on_failure_workflow text default null,
    p_on_failure_version text default null
)
returns table (run_id bigint, created boolean)
language plpgsql
as $$
declare
    v_run resume.runs;
    v_now timestamptz := clock_timestamp();
    v_deadline_at timestamptz := v_now + make_interval(secs => p_deadline_seconds);
begin
    if p_idempotency_key like 'resume:%' then
        raise exception 'idempotency key % is reserved: keys starting with resume: belong to resume',
            p_idempotency_key using errcode = '22023';
    end if;
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
