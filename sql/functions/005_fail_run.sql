-- Marks the run failed, so it is never claimed again.
create or replace function resume.fail_run(p_run_id bigint, p_attempt bigint)
returns void
language plpgsql
as $$
begin
    perform resume.lock_run(p_run_id, p_attempt);
    update resume.runs set failed_at = clock_timestamp() where id = p_run_id;
end;
$$;
