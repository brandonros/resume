create schema if not exists orders_app;
create table if not exists orders_app.inventory (
    sku text primary key check (sku <> ''),
    available bigint not null check (available >= 0)
);
create table if not exists orders_app.orders (
    key text primary key check (key <> ''),
    sku text not null check (sku <> ''),
    quantity bigint not null check (quantity > 0),
    status text not null default 'pending' check (status in ('pending', 'reserved', 'rejected', 'shipped')),
    job_id bigint not null unique references resume.jobs(id)
);
-- Local dispatch ledger for this application, not a real carrier integration.
create table if not exists orders_app.shipments (
    order_key text primary key references orders_app.orders(key),
    receipt text not null unique
);
