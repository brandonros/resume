-- Returns the step's saved output, or SQL null if it has not completed.
-- A step whose output is JSON null returns 'null'::jsonb, not SQL null.
create or replace function resume.load_step(p_run_id bigint, p_idempotency_key text)
returns jsonb
language sql
stable
as $$
    select output from resume.steps
    where run_id = p_run_id and idempotency_key = p_idempotency_key;
$$;
