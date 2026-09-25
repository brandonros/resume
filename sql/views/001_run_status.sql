-- Current committed state for operators. A lease describes a claim, not proof that a worker
-- is running. A step's uncommitted progress and lease renewal are not visible here.
-- newer_run_id means another request exists, not that this run skipped any step. Superseded
-- steps and attempt history are not stored, so this view cannot report them as outcomes.
create or replace view resume.run_status as
select
    r.id,
    r.workflow,
    r.version,
    r.idempotency_key,
    r.subject,
    case
        when r.completed_at is not null then 'completed'
        when r.cancelled_at is not null then 'cancelled'
        when r.failed_at is not null then 'failed'
        when r.leased and r.available_at <= statement_timestamp() then 'lease_expired'
        when r.leased then 'leased'
        when r.available_at > statement_timestamp() then 'waiting'
        else 'ready'
    end as status,
    r.attempt,
    r.released,
    r.attempt - r.released as attempts_used,
    r.max_attempts,
    -- For a leased run this is its recorded lease expiry; otherwise its next eligible time.
    -- Claiming can still be prevented by a row lock, deadline, or exhausted attempt budget.
    r.available_at,
    r.deadline_at,
    r.completed_at,
    r.failed_at,
    r.last_error,
    steps.completed as steps_completed,
    steps.pending as step_results_pending,
    r.failed_at is not null and steps.pending > 0 as needs_resolution,
    newer.id as newer_run_id,
    r.input
from resume.runs r
cross join lateral (
    select
        count(*) filter (where s.completed_at is not null) as completed,
        count(*) filter (where s.completed_at is null) as pending
    from resume.steps s where s.run_id = r.id
) steps
left join lateral (
    select n.id from resume.runs n
    where n.workflow = r.workflow and n.subject = r.subject and n.id > r.id
    order by n.id desc
    limit 1
) newer on true;
