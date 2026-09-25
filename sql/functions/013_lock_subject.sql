-- For Run::is_latest: locks the run's subject until the calling transaction ends, then returns
-- whether the run is still the newest of its workflow for that subject. Holding the lock through
-- the step means a newer run's step waits for this one to commit, so it applies after it.
create or replace function resume.lock_subject(p_run_id bigint)
returns boolean
language plpgsql
as $$
declare
    v_run resume.runs;
begin
    select * into v_run from resume.runs where id = p_run_id;
    if v_run.subject is null then
        raise exception 'run % has no subject; submit it with submit_for to use is_latest',
            p_run_id using errcode = '22023';
    end if;

    perform resume.lock_resource(v_run.subject);
    return not exists (
        select 1 from resume.runs r
        where r.workflow = v_run.workflow and r.subject = v_run.subject and r.id > p_run_id
    );
end;
$$;
