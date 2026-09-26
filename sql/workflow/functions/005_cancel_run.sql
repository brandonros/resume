-- Cancels an unfinished run. Waits for any step holding the row lock to finish; subsequent
-- steps and claims are rejected. A step_once action in flight still saves its result
-- (see save_step), so cancelling mid-call leaves nothing for an operator to resolve.
create or replace function resume.cancel_run(p_run_id bigint)
returns void
language plpgsql
as $$
begin
    update resume.runs
    set failed_at = clock_timestamp(), cancelled_at = clock_timestamp(),
        last_error = 'cancelled by an operator'
    where id = p_run_id and completed_at is null and failed_at is null;
    if not found then
        raise exception 'run % has already finished', p_run_id using errcode = '55000';
    end if;
end;
$$;
