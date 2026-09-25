-- Locks the subject for the transaction and returns whether this is its newest workflow run.
-- Hold through the step so newer runs apply their changes after this one commits.
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

    -- The same lock as Rust's lock_resource takes for this name.
    perform pg_advisory_xact_lock(hashtextextended(v_run.subject, 0));
    return not exists (
        select 1 from resume.runs r
        where r.workflow = v_run.workflow and r.subject = v_run.subject and r.id > p_run_id
    );
end;
$$;
