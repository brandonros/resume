create schema resume;

create table resume.runs (
    id bigint generated always as identity primary key,
    workflow text not null check (workflow <> ''),
    -- Supplied by the producer. Submitting the same key again returns this run.
    idempotency_key text not null check (idempotency_key <> ''),
    input jsonb not null,
    -- The record this run changes, such as 'customer:42'. Job::is_latest says whether a newer
    -- run of the workflow has the same subject.
    subject text check (subject <> ''),
    -- Only workers of this version may claim the run.
    version text not null check (version <> ''),
    -- If set, the run fails once this passes, whether it is waiting or in progress.
    deadline_at timestamptz,
    -- Increases with every claim and identifies which claim owns the run.
    attempt bigint not null default 0 check (attempt >= 0),
    -- Claims charged against max_attempts. Release refunds one; reopening resets the budget.
    attempts_used bigint not null default 0 check (attempts_used >= 0),
    max_attempts integer not null default 1 check (max_attempts > 0),
    -- The wait after a failed attempt starts at retry_delay and doubles, up to retry_max_delay.
    retry_delay interval not null default '1 second' check (retry_delay > interval '0'),
    retry_max_delay interval not null default '1 minute' check (retry_max_delay >= retry_delay),
    -- Next time this run may be claimed. While claimed, this is when the lease expires;
    -- each step renews it.
    available_at timestamptz not null default clock_timestamp(),
    -- Set on claim, cleared on release. Remains set after a crash to detect expired attempts.
    leased boolean not null default false,
    completed_at timestamptz,
    failed_at timestamptz,
    -- Set with failed_at when an operator cancelled the run.
    cancelled_at timestamptz,
    last_error text,
    on_failure_workflow text check (on_failure_workflow <> ''),
    on_failure_version text check (on_failure_version <> ''),
    check ((on_failure_workflow is null) = (on_failure_version is null)),
    check (completed_at is null or failed_at is null),
    check (cancelled_at is null or failed_at is not null),
    unique (workflow, idempotency_key)
);

create index runs_subject on resume.runs (workflow, subject, id) where subject is not null;

create index runs_available on resume.runs (workflow, version, available_at, id)
    where completed_at is null and failed_at is null;
