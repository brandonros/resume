create or replace function resume.enqueue(
    p_workflow text,
    p_input jsonb,
    p_max_attempts integer default 1
)
returns bigint
language sql
as $$
    insert into resume.runs (workflow, input, max_attempts)
    values (p_workflow, p_input, p_max_attempts)
    returning id;
$$;
