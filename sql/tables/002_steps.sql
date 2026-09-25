-- A step's progress within a run. begin_step inserts the row in the step's transaction, so a
-- failed step leaves nothing behind, except step_once, which commits the start before calling
-- out. A row with no completed_at therefore means a step_once action may have run and its
-- outcome is unknown. A completed step's output never changes.
-- The key is unique within the run and must be stable across attempts; it may be dynamic.
-- position is the order in which the run reached the step; a replay that reaches it at another
-- position means the workflow's code changed, and the run fails instead of going on.
create table resume.steps (
    run_id bigint not null references resume.runs (id) on delete cascade,
    idempotency_key text not null check (idempotency_key <> ''),
    position integer not null check (position >= 0),
    started_at timestamptz not null,
    completed_at timestamptz,
    -- A step whose output is JSON null stores 'null'::jsonb, so SQL null means not completed.
    output jsonb,
    primary key (run_id, idempotency_key),
    unique (run_id, position),
    check ((completed_at is null) = (output is null))
);
