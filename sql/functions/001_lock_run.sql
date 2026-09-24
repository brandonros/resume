-- Locks the run and raises unless this attempt still holds its claim.
-- Every function that changes a claimed run calls this first.
create or replace function resume.lock_run(p_run_id bigint, p_attempt bigint)
returns void
language plpgsql
as $$
declare
    v_run resume.runs;
begin
    select * into v_run from resume.runs where id = p_run_id for update;

    -- Check time after acquiring the lock, since acquiring it might have waited.
    if not found
       or v_run.attempt = 0
       or v_run.attempt is distinct from p_attempt
       or v_run.completed_at is not null
       or v_run.failed_at is not null
       or v_run.available_at <= clock_timestamp() then
        raise exception 'run % claim is no longer valid', p_run_id
            using errcode = '55000';
    end if;
end;
$$;
