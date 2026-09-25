-- Records that a step_once action may run. Returns false if an earlier attempt
-- already started it, in which case its outcome is unknown.
create or replace function resume.start_step(
    p_run_id bigint,
    p_attempt bigint,
    p_idempotency_key text
)
returns boolean
language plpgsql
as $$
begin
    perform resume.lock_run(p_run_id, p_attempt);

    insert into resume.step_starts (run_id, idempotency_key)
    values (p_run_id, p_idempotency_key)
    on conflict do nothing;
    return found;
end;
$$;
