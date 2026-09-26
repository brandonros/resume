create schema if not exists import_app;
create table if not exists import_app.events (
    id text primary key,
    payload jsonb not null
);
create table if not exists import_app.rejections (
    job_id bigint not null references resume.jobs(id),
    line bigint not null,
    raw text not null,
    reason text not null,
    primary key (job_id, line)
);
