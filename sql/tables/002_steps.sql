-- Normal steps commit their row with their result. step_once commits the start first,
-- so an incomplete row may represent an action with an unknown outcome.
-- Completed outputs are immutable. Keys must remain unique within a run and stable across
-- attempts; changing a step's position on replay fails the run.
create table resume.steps (
    run_id bigint not null references resume.runs (id) on delete cascade,
    key text not null check (key <> ''),
    position integer not null check (position >= 0),
    started_at timestamptz not null,
    completed_at timestamptz,
    -- A step whose output is JSON null stores 'null'::jsonb, so SQL null means not completed.
    output jsonb,
    primary key (run_id, key),
    unique (run_id, position),
    check ((completed_at is null) = (output is null))
);
