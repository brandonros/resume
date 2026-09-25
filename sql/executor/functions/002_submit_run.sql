-- Returns the existing run for this key or creates one. Keys remain bound to failed runs,
-- preventing step_once actions from repeating through resubmission.
-- The run may be claimed from p_available_at (default: now) until p_deadline_at (default:
-- never). Both are instants: how they are chosen is the caller's policy, not the executor's.
-- Parameters after p_input are optional.
create or replace function resume.submit_run(
    p_workflow text,
    p_version text,
    p_idempotency_key text,
    p_input jsonb,
    p_max_attempts integer default 1,
    p_deadline_at timestamptz default null,
    p_available_at timestamptz default null
)
returns table (run_id bigint, created boolean)
language plpgsql
as $$
declare
    v_run resume.runs;
begin
    if p_deadline_at is not null and not isfinite(p_deadline_at) then
        raise exception 'submit deadline must be finite' using errcode = '22023';
    end if;
    if p_available_at is not null and not isfinite(p_available_at) then
        raise exception 'submit available time must be finite' using errcode = '22023';
    end if;
    insert into resume.runs (
        workflow, version, idempotency_key, input, max_attempts, deadline_at, available_at
    )
    values (
        p_workflow, p_version, p_idempotency_key, p_input, p_max_attempts, p_deadline_at,
        -- Wake for an earlier deadline so expiry cleanup can fail the run on time.
        least(coalesce(p_available_at, clock_timestamp()), p_deadline_at)
    )
    on conflict (workflow, idempotency_key) do nothing
    returning * into v_run;

    if not found then
        -- A separate statement sees the winning insert after a conflict.
        select * into v_run from resume.runs r
        where r.workflow = p_workflow and r.idempotency_key = p_idempotency_key;

        if v_run.input is distinct from p_input or v_run.version is distinct from p_version then
            raise exception 'idempotency key % already used with a different input or version',
                p_idempotency_key
                using errcode = '23505';
        end if;
        return query select v_run.id, false;
        return;
    end if;

    return query select v_run.id, true;
end;
$$;
