-- Fails up to 100 due runs across all versions of a workflow. Call during worker polling.
-- Failure is committed by this transaction, so anything triggered by it commits with it.
create or replace function resume.expire_runs(p_workflow text)
returns void
language plpgsql
as $$
declare
    v_now timestamptz := clock_timestamp();
begin
    -- Sweep missed deadlines and expired final attempts without an owning worker.
    -- Skip active locks and bound the batch, including anything failure triggers,
    -- so cleanup cannot hold up claims. Later calls sweep the rest.
    with ended as (
        select r.id, r.deadline_at <= v_now as past_deadline
        from resume.runs r
        where r.workflow = p_workflow
          and r.completed_at is null
          and r.failed_at is null
          and r.available_at <= v_now
          and (r.attempts_used >= r.max_attempts or r.deadline_at <= v_now)
        limit 100
        for update skip locked
    )
    update resume.runs r
    set failed_at = clock_timestamp(),
        last_error = case when e.past_deadline then 'the run passed its deadline'
            else format('attempt %s''s lease expired', r.attempts_used) end
    from ended e
    where r.id = e.id;
end;
$$;
