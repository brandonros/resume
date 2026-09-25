-- Returns zero or one run submitted for this workflow version. Commit this claim before
-- executing user code. `expired` says the previous attempt's lease expired without
-- it handing the run back, as after a crash.
-- The lease must outlast one step, since begin_step renews it at the start of each step.
create or replace function resume.claim_run(
    p_workflow text,
    p_version text,
    p_lease_seconds double precision
)
returns table (
    id bigint,
    idempotency_key text,
    input jsonb,
    attempt bigint,
    released integer,
    max_attempts integer,
    expired boolean
)
language plpgsql
as $$
#variable_conflict use_column
declare
    v_now timestamptz := clock_timestamp();
begin
    if p_lease_seconds is null or p_lease_seconds <= 0 then
        raise exception 'lease seconds must be positive' using errcode = '22023';
    end if;

    -- Fail waiting runs past their deadline, and runs whose final attempt's lease expired. An
    -- attempt that returns an error goes through end_attempt instead. No attempt
    -- holds these claims, so this cannot go through end_attempt. Skip locked runs so a busy
    -- worker cannot hold up claims.
    with ended as (
        select r.id, r.deadline_at <= v_now as past_deadline
        from resume.runs r
        where r.workflow = p_workflow
          and r.completed_at is null
          and r.failed_at is null
          and r.available_at <= v_now
          and (r.attempt - r.released >= r.max_attempts or r.deadline_at <= v_now)
        for update skip locked
    )
    update resume.runs r
    set failed_at = clock_timestamp(),
        last_error = case when e.past_deadline then 'the run passed its deadline'
            else format('attempt %s''s lease expired', r.attempt - r.released) end
    from ended e
    where r.id = e.id;

    return query
    with candidate as (
        select r.id, r.leased
        from resume.runs r
        where r.workflow = p_workflow
          and r.completed_at is null
          and r.failed_at is null
          and r.attempt - r.released < r.max_attempts
          and r.available_at <= v_now
          and r.version = p_version
        order by r.available_at, r.id
        limit 1
        for update skip locked
    )
    update resume.runs r
    set attempt = r.attempt + 1,
        available_at = clock_timestamp() + make_interval(secs => p_lease_seconds),
        leased = true,
        last_error = case when c.leased
            then format('attempt %s''s lease expired', r.attempt - r.released)
            else r.last_error end
    from candidate c
    where r.id = c.id
    returning r.id, r.idempotency_key, r.input, r.attempt, r.released, r.max_attempts,
              c.leased;
end;
$$;
