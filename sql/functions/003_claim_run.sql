-- Returns zero or one run. Commit this claim before executing user code.
-- The lease covers the whole attempt; saving a step does not extend it.
create or replace function resume.claim_run(
    p_workflow text,
    p_lease_seconds integer default 60
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

    -- Fail runs whose final attempt's lease has expired. It may still complete while its
    -- lease is valid. No attempt holds the claim, so this cannot go through fail_run.
    -- Skip locked runs so a busy worker cannot hold up other claims.
    with exhausted as (
        select r.id
        from resume.runs r
        where r.workflow = p_workflow
          and r.completed_at is null
          and r.failed_at is null
          and r.attempt >= r.max_attempts
          and r.available_at <= v_now
        for update skip locked
    )
    update resume.runs r
    set failed_at = clock_timestamp()
    from exhausted e
    where r.id = e.id;

    return query
    with candidate as (
        select r.id
        from resume.runs r
        where r.workflow = p_workflow
          and r.completed_at is null
          and r.failed_at is null
          and r.attempt < r.max_attempts
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
