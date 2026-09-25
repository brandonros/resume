create schema exports;

-- A mock vendor's durable state. It uses a separate connection from the step transaction,
-- just as a real vendor would. Looking at ready_at simulates an asynchronous export.
create table exports.jobs (
    id bigint generated always as identity primary key,
    idempotency_key text not null unique,
    ready_at timestamptz not null
);

-- The application's record of a finished export.
create table exports.results (
    run_id bigint primary key references resume.runs (id),
    export_id bigint not null references exports.jobs (id),
    url text not null,
    recorded_at timestamptz not null default clock_timestamp()
);
