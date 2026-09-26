create schema resume;

create table resume.jobs (
    id bigint generated always as identity primary key,
    workflow text not null check (workflow <> ''),
    key text not null check (key <> ''),
    input jsonb not null,
    attempt bigint not null default 0,
    leased boolean not null default false,
    available_at timestamptz not null default clock_timestamp(),
    completed boolean not null default false,
    last_error text,
    unique (workflow, key)
);

create index jobs_ready on resume.jobs (workflow, available_at, id) where not completed;

create table resume.steps (
    job_id bigint not null references resume.jobs (id),
    key text not null check (key <> ''),
    output jsonb not null,
    primary key (job_id, key)
);

-- The no-op update locks a duplicate and validates its input atomically.
create function resume.submit(p_workflow text, p_key text, p_input jsonb)
returns bigint language plpgsql as $$
declare
    v_id bigint;
begin
    insert into resume.jobs (workflow, key, input)
    values (p_workflow, p_key, p_input)
    on conflict (workflow, key) do update set key = excluded.key
        where jobs.input = excluded.input
    returning id into v_id;
    if not found then
        raise exception 'idempotency key has different input' using errcode = '23505';
    end if;
    return v_id;
end;
$$;

-- Commit the claim before running a handler. Every claim gets a new fencing token.
create function resume.claim(p_workflow text)
returns setof resume.jobs language sql as $$
    with candidate as (
        select id from resume.jobs
        where workflow = p_workflow and not completed and available_at <= clock_timestamp()
        order by available_at, id
        limit 1 for update skip locked
    )
    update resume.jobs j
    set attempt = j.attempt + 1, leased = true,
        available_at = clock_timestamp() + interval '60 seconds'
    from candidate c where j.id = c.id
    returning j.*;
$$;

-- Hold this lock through the step's effects and saved output. Even if the lease
-- expires during the transaction, another worker cannot reclaim this locked job.
create function resume.begin_step(p_id bigint, p_attempt bigint)
returns void language plpgsql as $$
begin
    update resume.jobs
    set available_at = clock_timestamp() + interval '60 seconds'
    where id = p_id and attempt = p_attempt and leased and not completed
        and available_at > clock_timestamp();
    if not found then
        raise exception 'job claim is no longer valid' using errcode = 'RS001';
    end if;
end;
$$;

-- Success is terminal; failure releases the job with a fixed one-second delay.
create function resume.finish(p_id bigint, p_attempt bigint, p_error text)
returns void language plpgsql as $$
begin
    update resume.jobs
    set completed = p_error is null, leased = false, last_error = p_error,
        available_at = clock_timestamp() + interval '1 second'
    where id = p_id and attempt = p_attempt and leased and not completed
        and available_at > clock_timestamp();
    if not found then
        raise exception 'job claim is no longer valid' using errcode = 'RS001';
    end if;
end;
$$;
