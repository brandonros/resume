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
    paused boolean not null default false,
    last_error text,
    unique (workflow, key)
);

create index jobs_ready on resume.jobs (workflow, available_at, id) where not completed and not paused;

create table resume.steps (
    job_id bigint not null references resume.jobs (id),
    key text not null check (key <> ''),
    position integer not null check (position >= 0),
    once boolean not null,
    -- SQL null means started but unresolved; JSON null is a completed output.
    output jsonb,
    primary key (job_id, key),
    unique (job_id, position)
);

-- Operational snapshot. Expired leases may still have a transaction holding the row lock.
-- Filter status = 'paused', or order by attempt desc to inspect repeated claims.
create view resume.job_status as
select id, workflow, key, attempt,
    case
        when completed then 'completed'
        when paused then 'paused'
        when leased and available_at > statement_timestamp() then 'running'
        when leased then 'lease_expired'
        when available_at > statement_timestamp() then 'scheduled'
        else 'ready'
    end as status,
    available_at, last_error
from resume.jobs;

-- A missing output during an active call is normal, not evidence of a stuck job.
-- Inspect the external outcome before resolving; lease expiry does not prove the call stopped.
create view resume.unresolved_steps as
select j.id as job_id, j.workflow, j.key as job_key, j.attempt, j.status,
    j.available_at, j.last_error, s.key as step_key, s.position
from resume.job_status j
join resume.steps s on s.job_id = j.id
where s.once and s.output is null;

-- The no-op update validates duplicate input without restarting the job.
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
        where workflow = p_workflow and not completed and not paused
            and available_at <= clock_timestamp()
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

-- Validate history before any action. Regular steps roll this marker back on failure;
-- step_once commits it before invoking the action.
create function resume.start_step(p_id bigint, p_attempt bigint, p_key text,
    p_position integer, p_once boolean)
returns jsonb language plpgsql as $$
declare
    v_step resume.steps;
begin
    perform resume.begin_step(p_id, p_attempt);
    if exists (select 1 from resume.steps where job_id = p_id
        and (key = p_key or position = p_position)
        and (key <> p_key or position <> p_position or once <> p_once)) then
        raise exception 'step history differs at position %: %', p_position, p_key using errcode = 'RS002';
    end if;
    if exists (select 1 from resume.steps where job_id = p_id
        and output is null and position <= p_position) then
        raise exception 'an earlier step outcome is unknown; inspect before proceeding' using errcode = 'RS002';
    end if;
    select * into v_step from resume.steps where job_id = p_id and key = p_key;
    if found then
        return v_step.output;
    end if;
    insert into resume.steps (job_id, key, position, once) values (p_id, p_key, p_position, p_once);
    return null;
end;
$$;

-- Fence the writer again: a step_once action runs without the job's row lock.
-- A step already holding the lock may finish after lease expiry, but not after reclaim.
create function resume.save_step(p_id bigint, p_attempt bigint, p_key text, p_output jsonb)
returns jsonb language plpgsql as $$
declare
    v_output jsonb;
begin
    perform 1 from resume.jobs where id = p_id and attempt = p_attempt
        and leased and not completed for update;
    if not found then
        raise exception 'job claim is no longer valid' using errcode = 'RS001';
    end if;
    if p_output is null then
        raise exception 'step output must not be SQL null' using errcode = '22023';
    end if;
    update resume.steps set output = p_output
    where job_id = p_id and key = p_key and output is null;
    select output into v_output from resume.steps where job_id = p_id and key = p_key;
    if not found then
        raise exception 'step has not started' using errcode = 'RS002';
    end if;
    return v_output;
end;
$$;

-- Atomically record failure and the application decision: NULL delay pauses, otherwise schedule.
create function resume.finish(p_id bigint, p_attempt bigint, p_error text, p_position integer default 0,
    p_retry_after_seconds double precision default null)
returns void language plpgsql as $$
begin
    if p_retry_after_seconds < 0 or p_retry_after_seconds >= 'Infinity'::double precision then
        raise exception 'retry delay must be finite and nonnegative' using errcode = '22023';
    end if;
    perform 1 from resume.jobs where id = p_id and attempt = p_attempt
        and leased and not completed and available_at > clock_timestamp() for update;
    if not found then
        raise exception 'job claim is no longer valid' using errcode = 'RS001';
    end if;
    if p_error is null and (p_position is null or p_position < 0 or exists (
        select 1 from resume.steps where job_id = p_id
        and (output is null or position >= p_position)
    )) then
        raise exception 'cannot complete: unresolved or omitted steps' using errcode = 'RS002';
    end if;
    update resume.jobs
    set completed = p_error is null, leased = false, last_error = p_error,
        paused = p_error is not null and p_retry_after_seconds is null,
        available_at = clock_timestamp() + make_interval(secs => coalesce(p_retry_after_seconds, 0))
    where id = p_id and attempt = p_attempt and leased and not completed
        and available_at > clock_timestamp();
    if not found then
        raise exception 'job claim is no longer valid' using errcode = 'RS001';
    end if;
end;
$$;

-- Operator action, after verifying the external outcome. Recording a fact does not
-- authorize execution: pause the job and fence the old worker, retaining its history.
-- Lease expiry does not prove the external call has stopped; verification is essential.
create function resume.resolve_step(p_id bigint, p_key text, p_output jsonb)
returns void language plpgsql as $$
declare
    v_job resume.jobs;
begin
    if p_output is null then
        raise exception 'pass a verified output; use JSON null rather than SQL null' using errcode = '22023';
    end if;
    select * into v_job from resume.jobs where id = p_id for update;
    if not found or v_job.completed or (v_job.leased and v_job.available_at > clock_timestamp()) then
        raise exception 'job must be unfinished and not actively leased' using errcode = '55000';
    end if;
    update resume.steps set output = p_output
    where job_id = p_id and key = p_key and once and output is null;
    if not found then
        raise exception 'no unresolved step_once with this key' using errcode = '55000';
    end if;
    update resume.jobs set paused = true, leased = false where id = p_id;
end;
$$;

-- Operator action: schedule a paused or abandoned job, retaining saved outputs and claim tokens.
create function resume.requeue(p_id bigint, p_delay_seconds double precision default 0)
returns void language plpgsql as $$
declare
    v_job resume.jobs;
begin
    if p_delay_seconds is null or p_delay_seconds < 0 or p_delay_seconds >= 'Infinity'::double precision then
        raise exception 'delay must be finite and nonnegative' using errcode = '22023';
    end if;
    select * into v_job from resume.jobs where id = p_id for update;
    if not found or v_job.completed or (v_job.leased and v_job.available_at > clock_timestamp()) then
        raise exception 'job must be unfinished and not actively leased' using errcode = '55000';
    end if;
    if exists (select 1 from resume.steps where job_id = p_id and output is null) then
        raise exception 'resolve unknown step outcomes before retrying' using errcode = '55000';
    end if;
    if not v_job.paused and not v_job.leased then
        raise exception 'job is already queued' using errcode = '55000';
    end if;
    update resume.jobs
    set paused = false, leased = false,
        available_at = clock_timestamp() + make_interval(secs => p_delay_seconds)
    where id = p_id;
end;
$$;
