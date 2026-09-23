-- A row means a step_once action may have run. It is never removed.
create table resume.step_starts (
    run_id bigint not null references resume.runs (id) on delete cascade,
    step_name text not null check (step_name <> ''),
    primary key (run_id, step_name)
);
