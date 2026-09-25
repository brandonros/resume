-- Workflow policy extends the executor's run row. Install before workflow functions/views.
alter table resume.runs
    -- The record this run changes. Job::is_latest serializes requests for the same subject.
    add column subject text check (subject <> ''),
    -- Retry delays double per charged attempt, up to retry_max_delay, with 50-100% jitter.
    add column retry_delay interval not null default '1 second' check (retry_delay > interval '0'),
    add column retry_max_delay interval not null default '1 minute' check (retry_max_delay >= retry_delay),
    add column cancelled_at timestamptz,
    add column on_failure_workflow text check (on_failure_workflow <> ''),
    add column on_failure_version text check (on_failure_version <> ''),
    add check ((on_failure_workflow is null) = (on_failure_version is null)),
    add check (cancelled_at is null or failed_at is not null);

create index runs_subject on resume.runs (workflow, subject, id) where subject is not null;
