-- A row means the step completed. Results are immutable.
create table resume.steps (
    run_id bigint not null references resume.runs (id) on delete cascade,
    step_name text not null check (step_name <> ''),
    output jsonb not null,
    primary key (run_id, step_name)
);
