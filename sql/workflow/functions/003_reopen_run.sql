-- Requeues a failed run with fresh attempts and no deadline. Runs that dispatched a failure
-- handler cannot reopen; reopen the handler instead.
-- For an unresolved step_once action, check the vendor and supply the step key and output.
-- Reopening without resolving that outcome is rejected.
create or replace function resume.reopen_run(
    p_run_id bigint,
    p_key text default null,
    p_output jsonb default null
)
returns void
language plpgsql
as $$
declare
    v_handled boolean;
    v_step text;
begin
    select on_failure_workflow is not null into v_handled
    from resume.runs where id = p_run_id and failed_at is not null for update;
    if not found then
        raise exception 'run % is not failed', p_run_id using errcode = '55000';
    end if;

    -- A failed run with a handler has already handed responsibility to that workflow.
    if v_handled then
        raise exception 'run % has dispatched its failure handler and cannot resume; reopen the failed handler instead',
            p_run_id using errcode = '55000';
    end if;

    if p_key is not null then
        if p_output is null then
            raise exception 'pass the output of step %; use ''null'' for JSON null', p_key
                using errcode = '22023';
        end if;
        update resume.steps
        set completed_at = clock_timestamp(), output = p_output
        where run_id = p_run_id and key = p_key and completed_at is null;
        if not found then
            raise exception 'run % has no step % with an unknown outcome', p_run_id, p_key
                using errcode = '55000';
        end if;
    end if;

    select key into v_step from resume.steps
    where run_id = p_run_id and completed_at is null;
    if found then
        raise exception 'run % step % has an unknown outcome; check the vendor and pass the step''s key and output',
            p_run_id, v_step using errcode = '55000';
    end if;

    update resume.runs
    set failed_at = null, cancelled_at = null, deadline_at = null, attempts_used = 0,
        leased = false, available_at = clock_timestamp()
    where id = p_run_id;
end;
$$;
