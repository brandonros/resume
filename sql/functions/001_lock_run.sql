-- Locks the run; raises RS002 if this attempt no longer owns an unfinished claim.
-- Lease expiry is checked only by begin_step. Once a step holds the row lock, it may
-- save its result or record failure after expiry because no other worker can reclaim it.
create or replace function resume.lock_run(p_run_id bigint, p_attempt bigint)
returns void
language plpgsql
as $$
declare
    v_run resume.runs;
begin
    select * into v_run from resume.runs where id = p_run_id for update;

    if v_run.failed_at is not null then
        raise exception 'run % has failed: %', p_run_id, v_run.last_error using errcode = 'RS002';
    end if;
    if not found
       or v_run.attempt = 0
       or v_run.attempt is distinct from p_attempt
       or not v_run.leased
       or v_run.completed_at is not null then
        raise exception 'run % claim is no longer valid', p_run_id
            using errcode = 'RS002';
    end if;
end;
$$;
