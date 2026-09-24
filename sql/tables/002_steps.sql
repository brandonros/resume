-- A row means the step completed. Results are immutable.
-- The key is unique within the run and must be stable across attempts; it may be dynamic.
create table resume.steps (
    run_id bigint not null references resume.runs (id) on delete cascade,
    idempotency_key text not null check (idempotency_key <> ''),
    output jsonb not null,
    primary key (run_id, idempotency_key)
);
