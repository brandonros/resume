create schema provision;

-- Our own record of each sandbox, written inside the workflow.
create table provision.sandboxes (
    request_id text primary key,
    team text not null,
    vm_id bigint not null
);

-- The mock cloud's state, committed independently of the workflow's steps. Creating a VM is
-- not idempotent, and nothing stops a team from going over its quota.
create schema cloud;

create table cloud.vms (
    id bigint generated always as identity primary key,
    team text not null,
    request_id text not null
);

create index on cloud.vms (team);
create index on cloud.vms (request_id);
