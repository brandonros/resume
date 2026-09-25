-- Creates a run from an explicit attempt budget and absolute times, or returns its key's run.
-- Keys remain bound even after failure, preventing resubmission from repeating step_once.
-- Policy callers attach their configuration in this same transaction when created is true.
create or replace function resume.create_run(
    p_workflow text,
    p_version text,
    p_idempotency_key text,
    p_input jsonb,
    p_max_attempts integer,
    p_deadline_at timestamptz,
    p_available_at timestamptz
)
returns table (run_id bigint, created boolean)
language plpgsql
as $$
declare
    v_run resume.runs;
begin
    insert into resume.runs (
        workflow, version, idempotency_key, input, max_attempts, deadline_at, available_at
    )
    values (
        p_workflow, p_version, p_idempotency_key, p_input, p_max_attempts, p_deadline_at,
        -- Wake for an earlier deadline so expiry cleanup can fail the run on time.
        least(p_available_at, p_deadline_at)
    )
    on conflict (workflow, idempotency_key) do nothing
    returning * into v_run;

    if not found then
        -- A separate statement sees the winning insert after a conflict.
        select * into v_run from resume.runs r
        where r.workflow = p_workflow and r.idempotency_key = p_idempotency_key;

        if v_run.input is distinct from p_input or v_run.version is distinct from p_version then
            raise exception 'idempotency key % already used with a different input or version',
                p_idempotency_key using errcode = '23505';
        end if;
        return query select v_run.id, false;
        return;
    end if;

    return query select v_run.id, true;
end;
$$;
