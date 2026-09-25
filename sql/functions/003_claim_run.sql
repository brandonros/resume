-- Returns zero or one run. Commit this claim before executing user code.
-- The lease must outlast one step, since begin_step renews it at the start of each step.
create or replace function resume.claim_run(
    p_workflow text,
    p_lease_seconds double precision
)
returns setof resume.runs
language plpgsql
as $$
declare
    v_now timestamptz := clock_timestamp();
begin
    if p_lease_seconds is null or p_lease_seconds <= 0 then
        raise exception 'lease seconds must be positive' using errcode = '22023';
    end if;

    -- Fail runs whose final attempt's lease expired, as after a crash. An attempt that returns
    -- an error goes through fail_attempt instead. No attempt holds these claims, so this
    -- cannot go through fail_run. Skip locked runs so a busy worker cannot hold up claims.
    with exhausted as (
        select r.id
        from resume.runs r
        where r.workflow = p_workflow
          and r.completed_at is null
          and r.failed_at is null
          and r.attempt - r.released >= r.max_attempts
          and r.available_at <= v_now
        for update skip locked
    )
    update resume.runs r
    set failed_at = clock_timestamp(),
        last_error = 'the final attempt''s lease expired'
    from exhausted e
    where r.id = e.id;

    return query
    with candidate as (
        select r.id
        from resume.runs r
        where r.workflow = p_workflow
          and r.completed_at is null
          and r.failed_at is null
          and r.attempt - r.released < r.max_attempts
          and r.available_at <= v_now
        order by r.available_at, r.id
        limit 1
        for update skip locked
    )
    update resume.runs r
    set attempt = r.attempt + 1,
        available_at = clock_timestamp() + make_interval(secs => p_lease_seconds)
    from candidate c
    where r.id = c.id
    returning r.*;
end;
$$;
