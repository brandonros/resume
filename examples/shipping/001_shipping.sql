create schema shipping;

-- An order goes from paid to shipped, through the workflow, or from paid to cancelled, by the
-- customer.
create table shipping.orders (
    id bigint primary key,
    status text not null check (status in ('paid', 'shipped', 'cancelled'))
);

-- Orders whose cancellation the customer was told succeeded.
create table shipping.cancellations (
    order_id bigint primary key references shipping.orders (id)
);

-- The mock warehouse's state, committed independently of the workflow's steps. It ships
-- whatever it is asked to.
create schema warehouse;

create table warehouse.shipments (
    id bigint generated always as identity primary key,
    order_id bigint not null
);
