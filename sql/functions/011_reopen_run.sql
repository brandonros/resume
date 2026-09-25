-- For operators: puts a failed run back in the queue with a fresh set of attempts, for example
-- after fixing what made it fail. Clears the deadline, since the operator now decides when the
-- run is too late. Refuses while a step_once outcome is unknown, since the run would fail again
-- at once; resolve that step with resolve_step instead.
create or replace function resume.reopen_run(p_run_id bigint)
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

    select key into v_step from resume.steps
    where run_id = p_run_id and completed_at is null;
    if found then
        raise exception 'run % step % has an unknown outcome; resolve it with resolve_step',
            p_run_id, v_step using errcode = '55000';
    end if;

    update resume.runs
    set failed_at = null, cancelled_at = null, deadline_at = null, released = attempt,
        leased = false, available_at = clock_timestamp()
    where id = p_run_id;
end;
$$;
