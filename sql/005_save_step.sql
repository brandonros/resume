create or replace function resume.save_step(
    p_run_id bigint,
    p_attempt bigint,
    p_step_name text,
    p_output jsonb
)
returns jsonb
language plpgsql
as $$
declare
    v_run resume.runs;
begin
    -- Keep ownership validation and the checkpoint atomic with respect to claims.
    select * into v_run from resume.runs where id = p_run_id for update;

    if not found
       or v_run.attempt = 0
       or v_run.attempt is distinct from p_attempt
       or v_run.finished_at is not null
       or v_run.available_at <= clock_timestamp() then
        raise exception 'run % claim is no longer valid', p_run_id
            using errcode = '55000';
    end if;

    insert into resume.steps (run_id, step_name, output)
    values (p_run_id, p_step_name, p_output)
    on conflict (run_id, step_name) do nothing;

    -- Repeating a save returns the original result, even if the new value differs.
    return (select output from resume.steps
            where run_id = p_run_id and step_name = p_step_name);
end;
$$;
