-- Normal steps appear only after commit. An incomplete committed row records a step_once
-- start: its action might still be running, or might have ended without a recorded result.
-- Only a failed run requires operator resolution; do not label every such row a failure.
create or replace view resume.step_status as
select
    s.run_id,
    r.workflow,
    r.version,
    s.idempotency_key,
    s.position,
    case
        when s.completed_at is not null then 'completed'
        when r.failed_at is not null then 'unknown'
        else 'outcome_pending'
    end as status,
    s.started_at,
    s.completed_at,
    s.output,
    s.completed_at is null and r.failed_at is not null as needs_resolution
from resume.steps s
join resume.runs r on r.id = s.run_id;
