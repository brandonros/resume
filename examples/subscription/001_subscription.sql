create schema subscription;

-- Our record of each customer's plan, written by the workflow.
create table subscription.customers (
    id bigint primary key,
    plan text
);

-- The mock billing vendor's state, committed independently of the workflow's steps. Setting a
-- plan overwrites the previous one.
create schema billing;

create table billing.subscriptions (
    customer_id bigint primary key,
    plan text not null
);
