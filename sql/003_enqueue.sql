create or replace function resume.enqueue(p_workflow text, p_input jsonb)
returns bigint
language sql
as $$
    insert into resume.runs (workflow, input)
    values (p_workflow, p_input)
    returning id;
$$;
