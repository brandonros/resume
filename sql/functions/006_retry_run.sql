-- Schedules the next attempt after a failed one and returns the delay in seconds.
create or replace function resume.retry_run(p_run_id bigint, p_attempt bigint, p_error text)
returns double precision
language plpgsql
as $$
declare
    v_run resume.runs;
    v_delay interval;
begin
    perform resume.lock_run(p_run_id, p_attempt);
    select * into v_run from resume.runs where id = p_run_id;
    if v_run.attempt - v_run.released >= v_run.max_attempts then
        raise exception 'run % has no attempts left', p_run_id using errcode = '55000';
    end if;

    -- Double the delay for each attempt used, up to the maximum, then keep a random 50-100%
    -- of it so runs that failed together do not all retry at the same moment.
    v_delay := least(
        v_run.retry_max_delay,
        v_run.retry_delay * power(2, least(v_run.attempt - v_run.released - 1, 30))
    ) * (0.5 + random() / 2);
    update resume.runs
    set available_at = clock_timestamp() + v_delay, last_error = p_error
    where id = p_run_id;
    return extract(epoch from v_delay);
end;
$$;
