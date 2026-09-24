-- A row means a step_once action may have run. It is never removed.
create table resume.step_starts (
    run_id bigint not null references resume.runs (id) on delete cascade,
    idempotency_key text not null check (idempotency_key <> ''),
    primary key (run_id, idempotency_key)
);
