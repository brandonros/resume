create schema resume;

-- A run: one execution of a workflow, identified by its idempotency key, that survives crashes.
-- The workflow layer adds its own columns to this table (see workflow/tables/001_runs.sql);
-- no executor function reads them.
create table resume.runs (
    id bigint generated always as identity primary key,
    workflow text not null check (workflow <> ''),
    -- Supplied by the producer. Submitting the same key again returns this run.
    idempotency_key text not null check (idempotency_key <> ''),
    input jsonb not null,
    -- Only workers of this version may claim the run.
    version text not null check (version <> ''),
    -- If set, no new work starts once this passes, whether the run is waiting or in progress,
    -- and expiry cleanup fails it. The executor enforces it; the workflow layer decides its value.
    deadline_at timestamptz,
    -- Increases with every claim and identifies which claim owns the run.
    attempt bigint not null default 0 check (attempt >= 0),
    -- Claims charged against max_attempts. Release refunds one. A run whose last attempt died
    -- is never claimed again; expiry cleanup fails it.
    attempts_used bigint not null default 0 check (attempts_used >= 0),
    max_attempts integer not null default 1 check (max_attempts > 0),
    -- Next time this run may be claimed. While claimed, this is when the lease expires;
    -- each step renews it.
    available_at timestamptz not null default clock_timestamp(),
    -- Set on claim, cleared on release. Remains set after a crash to detect expired attempts.
    leased boolean not null default false,
    completed_at timestamptz,
    failed_at timestamptz,
    last_error text,
    check (completed_at is null or failed_at is null),
    unique (workflow, idempotency_key)
);

create index runs_available on resume.runs (workflow, version, available_at, id)
    where completed_at is null and failed_at is null;
