-- Completes the step that begin_step started, or records why step_if skipped it. Repeating a
-- save returns the original output, even if the new value differs.
create or replace function resume.save_step(
    p_run_id bigint,
    p_attempt bigint,
    p_idempotency_key text,
    p_output jsonb,
    p_skipped text default null
)
returns jsonb
language plpgsql
as $$
begin
    perform resume.lock_run(p_run_id, p_attempt);

    update resume.steps
    set completed_at = clock_timestamp(), output = p_output, skipped = p_skipped
    where run_id = p_run_id and idempotency_key = p_idempotency_key and completed_at is null;

    return (select output from resume.steps
            where run_id = p_run_id and idempotency_key = p_idempotency_key);
end;
$$;
