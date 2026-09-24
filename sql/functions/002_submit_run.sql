-- Returns the run for this key, creating it if needed. A key maps to one run for good,
-- even after that run fails, so a step_once action cannot repeat through a new run.
create or replace function resume.submit_run(
    p_workflow text,
    p_idempotency_key text,
    p_input jsonb,
    p_max_attempts integer default 1
)
returns table (run_id bigint, created boolean)
language plpgsql
as $$
declare
    v_run resume.runs;
begin
    insert into resume.runs (workflow, idempotency_key, input, max_attempts)
    values (p_workflow, p_idempotency_key, p_input, p_max_attempts)
    on conflict (workflow, idempotency_key) do nothing
    returning * into v_run;

    if not found then
        -- A separate statement sees the winning insert after a conflict.
        select * into v_run from resume.runs r
        where r.workflow = p_workflow and r.idempotency_key = p_idempotency_key;

        if v_run.input is distinct from p_input then
            raise exception 'idempotency key % already used with different input', p_idempotency_key
                using errcode = '23505';
        end if;
        return query select v_run.id, false;
        return;
    end if;

    return query select v_run.id, true;
end;
$$;
