create schema checkout;

-- Mock vendor state, written through a separate connection. Undo also creates a closed
-- record when the effect is absent, preventing an in-flight forward call from recreating it.
create table checkout.effects (
    order_key text not null,
    kind text not null check (kind in ('payment', 'reservation')),
    undone boolean not null,
    primary key (order_key, kind)
);
create table checkout.events (
    id bigint generated always as identity primary key,
    order_key text not null,
    action text not null,
    unique (order_key, action)
);
create table checkout.notifications (
    failed_run bigint primary key references resume.runs (id),
    order_key text not null,
    reason text not null
);
