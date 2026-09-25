-- Gives the claim back, optionally delaying the next claim. The attempt does not count
-- against max_attempts. A snooze cannot postpone checking the run's deadline.
create or replace function resume.release_run(
    p_run_id bigint,
    p_attempt bigint,
    p_delay_seconds double precision
)
returns void
language plpgsql
as $$
begin
    if p_delay_seconds is null or p_delay_seconds < 0
       or p_delay_seconds >= 'Infinity'::double precision then
        raise exception 'release delay seconds must be finite and nonnegative' using errcode = '22023';
    end if;
    perform resume.lock_run(p_run_id, p_attempt);
    update resume.runs
    set released = released + 1,
        available_at = least(
            clock_timestamp() + make_interval(secs => p_delay_seconds), deadline_at
        ),
        leased = false
    where id = p_run_id;
end;
$$;
