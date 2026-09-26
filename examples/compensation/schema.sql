create schema if not exists compensation_app;
create table if not exists compensation_app.inventory (
    sku text primary key,
    available bigint not null check (available >= 0)
);
create table if not exists compensation_app.orders (
    job_id bigint primary key references resume.jobs(id),
    quantity bigint not null check (quantity > 0),
    state text not null check (state in ('reserved', 'compensated')),
    released boolean not null default false
);
