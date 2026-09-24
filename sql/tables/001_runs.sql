create schema resume;

create table resume.runs (
    id bigint generated always as identity primary key,
    workflow text not null check (workflow <> ''),
    -- Supplied by the producer. Submitting the same key again returns this run.
    idempotency_key text not null check (idempotency_key <> ''),
    input jsonb not null,
    attempt bigint not null default 0 check (attempt >= 0),
    max_attempts integer not null default 1 check (max_attempts > 0),
    -- Next time this run may be claimed. A claim moves it into the future.
    available_at timestamptz not null default clock_timestamp(),
    completed_at timestamptz,
    failed_at timestamptz,
    check (completed_at is null or failed_at is null),
    unique (workflow, idempotency_key)
);

create index runs_available on resume.runs (workflow, available_at, id)
    where completed_at is null and failed_at is null;
