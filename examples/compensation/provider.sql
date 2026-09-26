-- Fake payment provider. Run through a separate connection/database, never the step transaction.
-- Each row represents one completed effect, with a stable idempotency key and immutable parameters.
create schema if not exists payment_provider;
create table if not exists payment_provider.charges (
    key text primary key,
    amount bigint not null check (amount > 0),
    calls bigint not null default 1
);
create table if not exists payment_provider.refunds (
    key text primary key,
    charge_key text not null unique references payment_provider.charges(key),
    amount bigint not null check (amount > 0),
    calls bigint not null default 1
);
