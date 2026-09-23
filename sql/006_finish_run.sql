create or replace function resume.finish_run(p_run_id bigint, p_attempt bigint)
returns void
language plpgsql
as $$
declare
    v_run resume.runs;
begin
    select * into v_run from resume.runs where id = p_run_id for update;

    if not found
       or v_run.attempt = 0
       or v_run.attempt is distinct from p_attempt
       or (v_run.finished_at is null and v_run.available_at <= clock_timestamp()) then
        raise exception 'run % claim is no longer valid', p_run_id
            using errcode = '55000';
    end if;

    -- Safe to repeat if the caller lost the response to the first completion.
    update resume.runs set finished_at = clock_timestamp()
    where id = p_run_id and finished_at is null;
end;
$$;
