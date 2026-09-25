-- Claims at most one run for this workflow version. Commit before executing user code.
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

    -- Sweep missed deadlines and expired final attempts without an owning worker.
    -- Skip active locks and bound the batch, including the failure handlers it queues,
    -- so cleanup cannot hold up claims. Later calls sweep the rest.
    with ended as (
        select r.id, r.deadline_at <= v_now as past_deadline
        from resume.runs r
        where r.workflow = p_workflow
          and r.completed_at is null
          and r.failed_at is null
          and r.available_at <= v_now
          and (r.attempt - r.released >= r.max_attempts or r.deadline_at <= v_now)
        limit 100
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
    returning r.id, r.idempotency_key, r.input, r.attempt, r.attempt - r.released, r.max_attempts,
              c.leased;
end;
$$;
