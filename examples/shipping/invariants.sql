-- Returns one row per violated invariant of the shipping example; no rows means all hold.
-- Check after every run has completed or failed.
select format('run %s is still pending', id)
from resume.runs where workflow = 'shipping' and completed_at is null and failed_at is null
union all
select format('run %s failed: %s', id, last_error)
from resume.runs where workflow = 'shipping' and failed_at is not null

-- Nothing ships after the customer was told it was cancelled, and nothing ships twice.
union all
select format('order %s shipped after its cancellation succeeded', c.order_id)
from shipping.cancellations c
where exists (select 1 from warehouse.shipments s where s.order_id = c.order_id)
union all
select format('order %s shipped %s times', order_id, count(*))
from warehouse.shipments group by order_id having count(*) > 1

-- Every order ended shipped or cancelled, and our record matches the warehouse.
union all
select format('order %s is %s, with %s shipments', o.id, o.status,
              (select count(*) from warehouse.shipments s where s.order_id = o.id))
from shipping.orders o
where o.status = 'paid'
   or (o.status = 'shipped') <> exists (select 1 from warehouse.shipments s where s.order_id = o.id)
