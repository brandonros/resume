create schema resume;

create table resume.runs (
    id bigint generated always as identity primary key,
    workflow text not null check (workflow <> ''),
    -- Supplied by the producer. Submitting the same key again returns this run.
    idempotency_key text not null check (idempotency_key <> ''),
    input jsonb not null,
    -- The record this run changes, such as 'customer:42'. Run::is_latest says whether a newer
    -- run of the workflow has the same subject.
    subject text check (subject <> ''),
    -- The workflow version the producer submitted the run for. Only workers of that version
    -- claim it, so its input and its steps come from the same code.
    version text not null check (version <> ''),
    -- If set, the run fails once this passes, whether it is waiting or in progress.
    deadline_at timestamptz,
    -- Increases with every claim and identifies which claim owns the run.
    attempt bigint not null default 0 check (attempt >= 0),
    -- Claims that do not count against max_attempts: those a stopping or snoozing worker gave
    -- back, and every claim before an operator reopened the run.
    released integer not null default 0 check (released >= 0),
    max_attempts integer not null default 1 check (max_attempts > 0),
    -- The wait after a failed attempt starts at retry_delay and doubles, up to retry_max_delay.
    retry_delay interval not null default '1 second' check (retry_delay > interval '0'),
    retry_max_delay interval not null default '1 minute' check (retry_max_delay >= retry_delay),
    -- Next time this run may be claimed. While claimed, this is when the lease expires;
    -- each step renews it.
    available_at timestamptz not null default clock_timestamp(),
    -- Set by a claim and cleared when the attempt hands the run back. A claim that finds it
    -- still set knows the previous attempt's lease expired, as after a crash.
    leased boolean not null default false,
    completed_at timestamptz,
    failed_at timestamptz,
    last_error text,
    on_failure_workflow text check (on_failure_workflow <> ''),
    on_failure_version text check (on_failure_version <> ''),
    check ((on_failure_workflow is null) = (on_failure_version is null)),
    check (completed_at is null or failed_at is null),
    unique (workflow, idempotency_key)
);

create index runs_subject on resume.runs (workflow, subject, id) where subject is not null;

create index runs_available on resume.runs (workflow, version, available_at, id)
    where completed_at is null and failed_at is null;
