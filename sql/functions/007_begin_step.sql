-- Starts a step: locks the run, checks this attempt still holds its claim, and returns the
-- step's saved output, or SQL null if it has not completed. Call it first in the step's
-- transaction, so the lock covers the rest of the step.
create or replace function resume.begin_step(
    p_run_id bigint,
    p_attempt bigint,
    p_idempotency_key text
)
returns jsonb
language plpgsql
as $$
begin
    perform resume.lock_run(p_run_id, p_attempt);
    return resume.load_step(p_run_id, p_idempotency_key);
end;
$$;
