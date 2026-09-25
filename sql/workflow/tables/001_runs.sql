-- What a run carries as a workflow, beyond durable execution. No executor function reads
-- these columns; the workflow layer turns them into the instants and counts the executor
-- enforces (deadline_at, available_at, max_attempts) or acts on them itself (subjects,
-- cancellation, failure handlers).
alter table resume.runs
    -- The record this run changes, such as 'customer:42'. Job::is_latest says whether a newer
    -- run of the workflow has the same subject.
    add column subject text check (subject <> ''),
    -- The wait after a failed attempt starts at retry_delay and doubles, up to retry_max_delay.
    add column retry_delay interval not null default '1 second' check (retry_delay > interval '0'),
    add column retry_max_delay interval not null default '1 minute',
    -- Set with failed_at when an operator cancelled the run.
    add column cancelled_at timestamptz,
    add column on_failure_workflow text check (on_failure_workflow <> ''),
    add column on_failure_version text check (on_failure_version <> ''),
    add constraint runs_retry_max_delay_check check (retry_max_delay >= retry_delay),
    add constraint runs_on_failure_check
        check ((on_failure_workflow is null) = (on_failure_version is null)),
    add constraint runs_cancelled_check check (cancelled_at is null or failed_at is not null);

create index runs_subject on resume.runs (workflow, subject, id) where subject is not null;
