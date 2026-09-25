-- Fails the run for permanent errors or exhausted attempts; otherwise schedules a retry.
-- Returns the retry delay in seconds, or null on terminal failure.
create or replace function resume.end_attempt(
    p_run_id bigint,
    p_attempt bigint,
    p_error text,
    p_permanent boolean
)
returns double precision
language plpgsql
as $$
declare
    v_run resume.runs;
    v_delay interval;
begin
    v_run := resume.lock_run(p_run_id, p_attempt);

    if p_permanent or v_run.attempt - v_run.released >= v_run.max_attempts then
        update resume.runs set failed_at = clock_timestamp(), last_error = p_error
        where id = p_run_id;
        return null;
    end if;

    -- Double the delay for each attempt used, up to the maximum, then keep a random 50-100%
    -- of it so runs that failed together do not all retry at the same moment.
    v_delay := least(
        v_run.retry_max_delay,
        v_run.retry_delay * power(2, least(v_run.attempt - v_run.released - 1, 30))
    ) * (0.5 + random() / 2);
    -- Wake for an earlier deadline so the claim sweep can fail the run on time.
    update resume.runs
    set available_at = least(clock_timestamp() + v_delay, deadline_at),
        leased = false,
        last_error = p_error
    where id = p_run_id;
    return extract(epoch from v_delay);
end;
$$;
