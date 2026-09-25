-- Workflow submission policy: resolves relative times and attaches subject, retry, and
-- failure-handler settings to a newly created run. Both writes share the caller's transaction.
-- Duplicate submissions keep the original settings. Parameters after p_input are optional.
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
    v_run_id bigint;
    v_created boolean;
    v_subject text;
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

    select c.run_id, c.created into v_run_id, v_created
    from resume.create_run(
        p_workflow, p_version, p_idempotency_key, p_input, p_max_attempts,
        v_deadline_at, coalesce(p_at, v_now + make_interval(secs => p_delay_seconds))
    ) c;

    if v_created then
        update resume.runs
        set subject = p_subject,
            retry_delay = make_interval(secs => p_retry_delay_seconds),
            retry_max_delay = make_interval(secs => p_retry_max_delay_seconds),
            on_failure_workflow = p_on_failure_workflow, on_failure_version = p_on_failure_version
        where id = v_run_id;
    else
        select r.subject into v_subject from resume.runs r where r.id = v_run_id;

        if v_subject is distinct from p_subject then
            raise exception 'idempotency key % already used with a different subject',
                p_idempotency_key
                using errcode = '23505';
        end if;
    end if;

    return query select v_run_id, v_created;
end;
$$;
