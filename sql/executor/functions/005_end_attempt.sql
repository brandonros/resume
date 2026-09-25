-- Ends this attempt with an error. The run may be claimed again after p_retry_after_seconds;
-- it fails instead when that is null or its attempt budget is spent. The attempt stays charged
-- (compare release_run). Returns whether a retry was scheduled.
-- How long to wait, and whether an error is worth retrying, is the caller's policy.
create or replace function resume.end_attempt(
    p_run_id bigint,
    p_attempt bigint,
    p_error text,
    p_retry_after_seconds double precision
)
returns boolean
language plpgsql
as $$
declare
    v_run resume.runs;
begin
    if p_retry_after_seconds < 0 or p_retry_after_seconds >= 'Infinity'::double precision then
        raise exception 'retry delay seconds must be finite and nonnegative' using errcode = '22023';
    end if;
    v_run := resume.lock_run(p_run_id, p_attempt);

    if p_retry_after_seconds is null or v_run.attempts_used >= v_run.max_attempts then
        update resume.runs set failed_at = clock_timestamp(), last_error = p_error
        where id = p_run_id;
        return false;
    end if;

    -- Wake for an earlier deadline so expiry cleanup can fail the run on time.
    update resume.runs
    set available_at = least(
            clock_timestamp() + make_interval(secs => p_retry_after_seconds), deadline_at
        ),
        leased = false,
        last_error = p_error
    where id = p_run_id;
    return true;
end;
$$;
