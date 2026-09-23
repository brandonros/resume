create schema three_steps;

create table three_steps.results (
    run_id bigint primary key references resume.runs (id) on delete cascade,
    value bigint not null
);
