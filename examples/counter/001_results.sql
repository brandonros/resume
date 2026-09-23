create schema counter;

create table counter.results (
    run_id bigint primary key references resume.runs (id) on delete cascade,
    value bigint not null
);
