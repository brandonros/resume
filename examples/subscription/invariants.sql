-- Returns one row per violated invariant of the subscription example; no rows means all hold.
-- Check after every run has completed or failed.
with runs as (
    select * from resume.runs where workflow = 'subscription'
),
-- The plan each customer asked for last: the input of their newest run.
latest as (
    select distinct on (subject)
        (input ->> 'customer_id')::bigint as customer_id, input ->> 'plan' as plan
    from runs
    order by subject, id desc
)
select format('run %s is still pending', id)
from runs where completed_at is null and failed_at is null
union all
select format('run %s failed: %s', id, last_error)
from runs where failed_at is not null

-- Every customer ends on the plan they asked for last, at billing and in our record.
union all
select format('customer %s last asked for %s, but billing has %s and we recorded %s',
              l.customer_id, l.plan, coalesce(b.plan, 'nothing'), coalesce(c.plan, 'nothing'))
from latest l
left join billing.subscriptions b on b.customer_id = l.customer_id
left join subscription.customers c on c.id = l.customer_id
where l.plan is distinct from b.plan or l.plan is distinct from c.plan
