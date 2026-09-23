create schema tickets;

-- The mock issuer's state, committed independently of the workflow's steps.
create table tickets.issued (
    id bigint generated always as identity primary key,
    request_id text not null unique check (request_id <> ''),
    attendee text not null check (attendee <> '')
);
