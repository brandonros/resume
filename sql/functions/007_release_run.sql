-- Gives the claim back, so another worker continues the run now instead of after the lease
-- expires. The attempt does not count against max_attempts.
create or replace function resume.release_run(p_run_id bigint, p_attempt bigint)
returns void
language plpgsql
as $$
begin
    perform resume.lock_run(p_run_id, p_attempt);
    update resume.runs
    set released = released + 1, available_at = clock_timestamp()
    where id = p_run_id;
end;
$$;
