create schema resume;

create table resume.runs (
    id bigint generated always as identity primary key,
    workflow text not null check (workflow <> ''),
    input jsonb not null,
    attempt bigint not null default 0 check (attempt >= 0),
    -- Next time this run may be claimed. A claim moves it into the future.
    available_at timestamptz not null default clock_timestamp(),
    finished_at timestamptz
);

create index runs_available on resume.runs (workflow, available_at, id)
    where finished_at is null;
