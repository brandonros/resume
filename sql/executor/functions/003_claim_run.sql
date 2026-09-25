-- Claims at most one run for this workflow version. Commit before executing user code.
-- Pollers call expire_runs separately to fail due runs that can no longer execute.
-- `expired` means the previous attempt's lease expired without release.
-- The lease must outlast one step; begin_step renews it before each action.
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
    attempts_used bigint,
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

    return query
    with candidate as (
        select r.id, r.leased
        from resume.runs r
        where r.workflow = p_workflow
          and r.completed_at is null
          and r.failed_at is null
          and r.attempts_used < r.max_attempts
          and (r.deadline_at is null or r.deadline_at > v_now)
          and r.available_at <= v_now
          and r.version = p_version
        order by r.available_at, r.id
        limit 1
        for update skip locked
    )
    update resume.runs r
    set attempt = r.attempt + 1,
        attempts_used = r.attempts_used + 1,
        available_at = clock_timestamp() + make_interval(secs => p_lease_seconds),
        leased = true,
        last_error = case when c.leased
            then format('attempt %s''s lease expired', r.attempts_used)
            else r.last_error end
    from candidate c
    where r.id = c.id
    returning r.id, r.idempotency_key, r.input, r.attempt, r.attempts_used, r.max_attempts,
              c.leased;
end;
$$;
