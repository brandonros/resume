create or replace function resume.complete_run(p_run_id bigint, p_attempt bigint)
returns void
language plpgsql
as $$
begin
    -- Safe to repeat if the caller lost the response to the first completion.
    -- A completed run never changes again, so this needs no lock.
    if exists (select 1 from resume.runs
               where id = p_run_id and attempt = p_attempt and completed_at is not null) then
        return;
    end if;

    perform resume.lock_run(p_run_id, p_attempt);
    update resume.runs set completed_at = clock_timestamp() where id = p_run_id;
end;
$$;
