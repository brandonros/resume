-- Locks the run and raises with SQLSTATE RS002 unless this attempt still owns it: no later claim
-- replaced it, it has not handed the run back, and the run has not finished. Every function that
-- changes a claimed run calls this first. It does not check the lease: once a step's transaction
-- holds this lock, no one can claim the run, so an attempt may save a step or record how it ended
-- even after its lease ran out. Only starting a step needs a live lease; see begin_step.
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
