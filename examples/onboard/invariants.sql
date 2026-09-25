-- Returns one row per violated invariant of the onboard example; no rows means all hold.
-- Check after every run has completed or failed.
with runs as (
    select r.*, r.input ->> 'email' as email, r.input ->> 'plan' as plan
    from resume.runs r
    where r.workflow = 'onboard'
),
customers as (
    select c.*, r.id as run_id, r.plan, r.completed_at is not null as completed
    from onboard.customers c
    join runs r on r.email = c.email
),
vendor as (
    select c.id as customer_id,
        (select count(*) from vendors.crm_contacts x where x.customer_id = c.id) as crm_contacts,
        (select min(x.id) from vendors.crm_contacts x where x.customer_id = c.id) as crm_contact_id,
        (select count(*) from vendors.billing_customers x where x.email = c.email) as billing_customers,
        (select min(x.id) from vendors.billing_customers x where x.email = c.email) as billing_customer_id,
        (select count(*) from vendors.charges x
         where x.idempotency_key = 'setup_fee:' || c.id) as charges,
        (select min(x.id) from vendors.charges x
         where x.idempotency_key = 'setup_fee:' || c.id) as charge_id,
        (select count(*) from vendors.emails x where x.recipient = c.email) as emails,
        (select min(x.id) from vendors.emails x where x.recipient = c.email) as email_id,
        (select count(*) from vendors.slack_messages x
         where x.text = 'onboarded ' || c.email || ' on ' || c.plan) as slack_messages
    from customers c
)

-- Every run finished, within its attempts, and a failed run says why.
select format('run %s is still pending', id)
from runs where completed_at is null and failed_at is null
union all
select format('run %s used %s of %s attempts', id, attempts_used, max_attempts)
from runs where attempts_used > max_attempts
union all
select format('failed run %s has no last_error', id)
from runs where failed_at is not null and last_error is null

-- No duplicates, whatever the outcome: each effect happened at most once.
union all
select format('%s has %s CRM contacts, %s billing customers, %s charges, %s emails',
              c.email, v.crm_contacts, v.billing_customers, v.charges, v.emails)
from customers c join vendor v on v.customer_id = c.id
where v.crm_contacts > 1 or v.billing_customers > 1 or v.charges > 1 or v.emails > 1

-- A completed run did everything, and our records point at the vendors' records.
union all
select format('completed %s (%s) has %s CRM contacts, %s billing customers, %s charges, '
              '%s emails, %s Slack messages',
              c.email, c.plan, v.crm_contacts, v.billing_customers, v.charges, v.emails,
              v.slack_messages)
from customers c join vendor v on v.customer_id = c.id
where c.completed
  and (v.crm_contacts <> 1 or v.billing_customers <> 1 or v.charges <> (c.plan = 'pro')::int
       or v.emails <> 1 or v.slack_messages < 1)
union all
select format('completed %s does not link to its vendor records', c.email)
from customers c join vendor v on v.customer_id = c.id
where c.completed
  and (c.crm_contact_id is distinct from v.crm_contact_id
       or c.billing_customer_id is distinct from v.billing_customer_id
       or c.setup_fee_charge_id is distinct from v.charge_id
       or c.welcomed_at is null)

-- Every sent email has a started step_once row, so no email went out unrecorded.
union all
select format('email %s to %s has no send_welcome_email step', e.id, e.recipient)
from vendors.emails e
join runs r on r.email = e.recipient
where not exists (
    select 1 from resume.steps s
    where s.run_id = r.id and s.key = 'send_welcome_email'
)

-- Every saved step output matches what the vendor or our database holds.
union all
select format('run %s step %s saved %s, but the record is %s',
              s.run_id, s.key, s.output, coalesce(actual.id::text, 'missing'))
from resume.steps s
join customers c on c.run_id = s.run_id
join vendor v on v.customer_id = c.id
cross join lateral (
    select case s.key
        when 'create_customer' then c.id
        when 'ensure_crm_contact' then v.crm_contact_id
        when 'ensure_billing_customer' then v.billing_customer_id
        when 'ensure_setup_fee' then v.charge_id
        when 'send_welcome_email' then v.email_id
    end as id
) actual
where s.completed_at is not null
  and s.key in ('create_customer', 'ensure_crm_contact', 'ensure_billing_customer',
                            'ensure_setup_fee', 'send_welcome_email')
  and s.output is distinct from to_jsonb(actual.id)
