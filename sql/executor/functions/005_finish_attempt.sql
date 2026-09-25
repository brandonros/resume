-- Ends an unsuccessful attempt. A null retry delay or exhausted budget fails the run;
-- otherwise releases ownership and schedules the supplied delay. No backoff policy lives here.
-- Returns the supplied delay in seconds, or null on terminal failure.
create or replace function resume.finish_attempt(
    p_run_id bigint,
    p_attempt bigint,
    p_error text,
    p_retry_after interval
)
returns double precision
language plpgsql
as $$
declare
    v_run resume.runs;
begin
    v_run := resume.lock_run(p_run_id, p_attempt);

    if p_retry_after is null or v_run.attempts_used >= v_run.max_attempts then
        update resume.runs set failed_at = clock_timestamp(), last_error = p_error
        where id = p_run_id;
        return null;
    end if;
    if p_retry_after < interval '0' then
        raise exception 'retry delay must be nonnegative' using errcode = '22023';
    end if;

    -- Wake for an earlier deadline so expiry cleanup can fail the run on time.
    update resume.runs
    set available_at = least(clock_timestamp() + p_retry_after, deadline_at),
        leased = false,
        last_error = p_error
    where id = p_run_id;
    return extract(epoch from p_retry_after);
end;
$$;
