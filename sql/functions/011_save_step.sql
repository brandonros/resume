create or replace function resume.save_step(
    p_run_id bigint,
    p_attempt bigint,
    p_idempotency_key text,
    p_output jsonb
)
returns jsonb
language plpgsql
as $$
begin
    perform resume.lock_run(p_run_id, p_attempt);

    insert into resume.steps (run_id, idempotency_key, output)
    values (p_run_id, p_idempotency_key, p_output)
    on conflict (run_id, idempotency_key) do nothing;

    -- Repeating a save returns the original result, even if the new value differs.
    return resume.load_step(p_run_id, p_idempotency_key);
end;
$$;
