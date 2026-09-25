-- For operators: stops a run that has not finished. A waiting run is never claimed again, and
-- a run in progress stops at its next step, because lock_run rejects failed runs. The step in
-- progress, if any, finishes first: this waits for its lock.
create or replace function resume.cancel_run(p_run_id bigint)
returns void
language plpgsql
as $$
begin
    update resume.runs
    set failed_at = clock_timestamp(), last_error = 'cancelled by an operator'
    where id = p_run_id and completed_at is null and failed_at is null;
    if not found then
        raise exception 'run % has already finished', p_run_id using errcode = '55000';
    end if;
end;
$$;
