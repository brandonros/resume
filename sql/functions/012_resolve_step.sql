-- For operators: records the outcome of a step_once action that an interrupted attempt left
-- unknown, after checking the vendor, and reopens the run so it continues from there. The
-- output is what the action would have returned, such as the vendor's ID for what it created.
create or replace function resume.resolve_step(
    p_run_id bigint,
    p_idempotency_key text,
    p_output jsonb
)
returns void
language plpgsql
as $$
begin
    perform 1 from resume.runs where id = p_run_id and failed_at is not null for update;
    if not found then
        raise exception 'run % is not failed', p_run_id using errcode = '55000';
    end if;

    update resume.steps
    set completed_at = clock_timestamp(), output = p_output
    where run_id = p_run_id and idempotency_key = p_idempotency_key and completed_at is null;
    if not found then
        raise exception 'run % has no unresolved step %', p_run_id, p_idempotency_key
            using errcode = '55000';
    end if;

    perform resume.reopen_run(p_run_id);
end;
$$;
