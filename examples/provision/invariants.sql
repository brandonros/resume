-- Returns one row per violated invariant of the provision example; no rows means all hold.
-- The quota of 3 matches QUOTA in main.rs. Check after every run has completed or failed.
with runs as (
    select r.*, r.input ->> 'team' as team
    from resume.runs r
    where r.workflow = 'provision'
),
refused as (
    select * from runs where failed_at is not null and last_error like '%quota%'
)
select format('run %s is still pending', id)
from runs where completed_at is null and failed_at is null
union all
select format('run %s failed for another reason: %s', id, last_error)
from runs where failed_at is not null and last_error not like '%quota%'

-- The quota holds, and no request got two VMs.
union all
select format('team %s has %s VMs, over its quota of 3', team, count(*))
from cloud.vms group by team having count(*) > 3
union all
select format('request %s has %s VMs', request_id, count(*))
from cloud.vms group by request_id having count(*) > 1

-- A completed request has its VM, and our record points at it.
union all
select format('completed request %s has no matching sandbox and VM', r.idempotency_key)
from runs r
where r.completed_at is not null
  and not exists (
      select 1 from provision.sandboxes s join cloud.vms v on v.id = s.vm_id
      where s.request_id = r.idempotency_key and s.team = r.team
        and v.request_id = r.idempotency_key and v.team = r.team
  )

-- A refused request created nothing, and was refused only because its team was full.
union all
select format('refused request %s has a VM', r.idempotency_key)
from refused r
where exists (select 1 from cloud.vms v where v.request_id = r.idempotency_key)
union all
select format('request %s was refused, but team %s has only %s VMs', r.idempotency_key, r.team, n)
from refused r
cross join lateral (select count(*) as n from cloud.vms v where v.team = r.team) vms
where vms.n < 3
