-- Marks the run complete, once the attempt's history agrees with the workflow's code.
-- p_next_position is the attempt's cursor: the position its next step would take. A step
-- whose action errored advances it before rolling back, so a swallowed error there is fine.
-- Raises RS001, as begin_step does when code and history disagree, for:
--   a step without a result: only step_once leaves one, after its action failed or timed out,
--     so the handler swallowed an error whose outcome is unknown;
--   a recorded step at or past the cursor: a suffix this attempt never reached, so
--     begin_step never validated it against the code.
-- Rejection is the attempt's error, so the caller settles it as a permanent failure.
create or replace function resume.complete_run(
    p_run_id bigint,
    p_attempt bigint,
    p_next_position integer
)
returns void
language plpgsql
as $$
declare
    v_step resume.steps;
begin
    if p_next_position is null or p_next_position < 0 then
        raise exception 'next position must be nonnegative' using errcode = '22023';
    end if;

    -- Safe to repeat if the caller lost the response to the first completion.
    -- A completed run never changes again, so this needs no lock.
    if exists (select 1 from resume.runs
               where id = p_run_id and attempt = p_attempt and completed_at is not null) then
        return;
    end if;

    perform resume.lock_run(p_run_id, p_attempt);

    select * into v_step from resume.steps
    where run_id = p_run_id and completed_at is null
    order by position limit 1;
    if found then
        raise exception 'workflow changed: step % started but has no result; its outcome is unknown',
            v_step.key using errcode = 'RS001';
    end if;

    select * into v_step from resume.steps
    where run_id = p_run_id and position >= p_next_position
    order by position limit 1;
    if found then
        raise exception 'workflow changed: step % ran at position %, but this attempt ended at position %',
            v_step.key, v_step.position, p_next_position using errcode = 'RS001';
    end if;

    update resume.runs set completed_at = clock_timestamp() where id = p_run_id;
end;
$$;
