-- Completes the step that begin_step started. Repeating a save returns the original output,
-- even if the new value differs.
-- The run may have failed while a step_once action ran (cancelled, or past its deadline).
-- The action happened, so its output is fact: record it, and the run stops at its next step
-- instead of leaving an outcome for an operator to resolve.
create or replace function resume.save_step(
    p_run_id bigint,
    p_attempt bigint,
    p_key text,
    p_output jsonb
)
returns jsonb
language plpgsql
as $$
begin
    perform resume.lock_run(p_run_id, p_attempt, p_allow_failed => true);

    update resume.steps
    set completed_at = clock_timestamp(), output = p_output
    where run_id = p_run_id and key = p_key and completed_at is null;

    return (select output from resume.steps where run_id = p_run_id and key = p_key);
end;
$$;
