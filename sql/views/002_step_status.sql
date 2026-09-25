-- Normal steps appear after commit. An incomplete step_once row may still be running;
-- it requires operator resolution only once the run has failed.
create or replace view resume.step_status as
select
    s.run_id,
    r.workflow,
    r.version,
    s.key,
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
