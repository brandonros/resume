create schema onboard;

-- The application's own state, written inside workflow steps.
create table onboard.customers (
    id bigint generated always as identity primary key,
    email text not null unique check (email <> ''),
    crm_contact_id bigint,
    billing_customer_id bigint,
    setup_fee_charge_id bigint,
    welcomed_at timestamptz
);

-- The mock vendors' state, committed independently of the workflow's steps.
create schema vendors;

-- Searchable, but creating a contact is not idempotent.
create table vendors.crm_contacts (
    id bigint generated always as identity primary key,
    customer_id bigint not null,
    email text not null
);

-- Billing accepts idempotency keys.
create table vendors.billing_customers (
    id bigint generated always as identity primary key,
    idempotency_key text not null unique,
    email text not null
);

create table vendors.charges (
    id bigint generated always as identity primary key,
    idempotency_key text not null unique,
    billing_customer_id bigint not null,
    amount_cents bigint not null
);

-- No idempotency key and no way to look up what was sent.
create table vendors.emails (
    id bigint generated always as identity primary key,
    recipient text not null,
    template text not null
);

create table vendors.slack_messages (
    id bigint generated always as identity primary key,
    text text not null
);
