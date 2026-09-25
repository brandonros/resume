create schema resume;

create table resume.runs (
    id bigint generated always as identity primary key,
    workflow text not null check (workflow <> ''),
    -- Supplied by the producer. Submitting the same key again returns this run.
    idempotency_key text not null check (idempotency_key <> ''),
    input jsonb not null,
    -- Increases with every claim and identifies which claim owns the run.
    attempt bigint not null default 0 check (attempt >= 0),
    -- Claims a stopping worker gave back. They do not count against max_attempts.
    released integer not null default 0 check (released >= 0),
    max_attempts integer not null default 1 check (max_attempts > 0),
    -- The wait after a failed attempt starts at retry_delay and doubles, up to retry_max_delay.
    retry_delay interval not null default '1 second' check (retry_delay > interval '0'),
    retry_max_delay interval not null default '1 minute' check (retry_max_delay >= retry_delay),
    -- Next time this run may be claimed. While claimed, this is when the lease expires;
    -- each step renews it.
    available_at timestamptz not null default clock_timestamp(),
    completed_at timestamptz,
    failed_at timestamptz,
    last_error text,
    check (completed_at is null or failed_at is null),
    unique (workflow, idempotency_key)
);

create index runs_available on resume.runs (workflow, available_at, id)
    where completed_at is null and failed_at is null;
