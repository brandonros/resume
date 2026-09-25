server := "postgresql://brandon@localhost:5432"
export DATABASE_URL := server + "/resume"
export PATH := "/Users/brandon/Applications/Postgres.app/Contents/Versions/18/bin:" + env("PATH")

# Drops and recreates the resume database with every schema.
reset: && schema counter-schema onboard-schema provision-schema subscription-schema tickets-schema
    dropdb --if-exists --force --maintenance-db "{{server}}/postgres" resume
    createdb --maintenance-db "{{server}}/postgres" resume

# Runs the tests against a scratch database, recreated each time.
test:
    dropdb --if-exists --force --maintenance-db "{{server}}/postgres" resume_test
    createdb --maintenance-db "{{server}}/postgres" resume_test
    psql "{{server}}/resume_test" -X -q -v ON_ERROR_STOP=1 --single-transaction \
        $(printf ' -f %s' sql/tables/*.sql sql/functions/*.sql)
    DATABASE_URL="{{server}}/resume_test" cargo test --workspace -q

schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction \
        $(printf ' -f %s' sql/tables/*.sql sql/functions/*.sql)

# For operators: after checking the vendor, record an interrupted step_once step's output and
# continue the run, e.g. just resolve-step 7 send_welcome_email 42
resolve-step run key output:
    echo "select resume.resolve_step(:'run', :'key', :'output')" | psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 \
        -v run={{quote(run)}} -v key={{quote(key)}} -v output={{quote(output)}}

# For operators: put a failed run back in the queue with fresh attempts.
reopen-run run:
    echo "select resume.reopen_run(:'run')" | psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -v run={{quote(run)}}

# For operators: stop a run that has not finished. A run in progress stops at its next step.
cancel-run run:
    echo "select resume.cancel_run(:'run')" | psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -v run={{quote(run)}}

counter-schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction -f examples/counter/001_results.sql

counter-submit key:
    cargo run -p counter -- submit {{quote(key)}}

counter-process:
    cargo run -p counter -- work

onboard-schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction -f examples/onboard/001_onboard.sql

onboard-submit email plan="pro":
    cargo run -p onboard -- submit {{quote(email)}} {{quote(plan)}}

# faults: a plan such as "charge.crash=once" or "seed=7 latency_ms=100 email.error=0.2"
onboard-process faults="":
    FAULTS={{quote(faults)}} cargo run -p onboard -- work

# Clears the onboard data, then onboards one customer per fault point, each firing once.
onboard-each:
    cargo run -q -p onboard -- each

# Clears the onboard data, then onboards customers under random faults and signals.
onboard-chaos seed="1" customers="50" workers="3":
    cargo run -q -p onboard -- chaos {{seed}} {{customers}} {{workers}}

# Clears the onboard data, then submits every customer from many producers at once while many
# workers process them.
onboard-race producers="10" workers="10" customers="100":
    cargo run -q -p onboard -- race {{producers}} {{workers}} {{customers}}

onboard-check:
    cargo run -q -p onboard -- check

# Shows each run and what the mock vendors did.
onboard-show:
    psql "$DATABASE_URL" -X \
        -c "select r.id, r.idempotency_key as email, r.attempt, \
                case when r.completed_at is not null then 'completed' \
                     when r.failed_at is not null then 'failed' else 'pending' end as status, \
                (select count(*) from resume.steps s where s.run_id = r.id and s.completed_at is not null) as saved_steps, \
                r.last_error \
            from resume.runs r where r.workflow = 'onboard' order by r.id" \
        -c "select * from vendors.charges order by id" \
        -c "select * from vendors.emails order by id"

provision-schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction -f examples/provision/001_provision.sql

# Clears the provision data, then has workers race to create VMs for requests across teams,
# under a per-team quota. Pass "unlocked" to drop the lock and watch teams go over quota.
provision-race teams="3" requests="50" workers="20" lock="locked":
    cargo run -q -p provision -- race {{teams}} {{requests}} {{workers}} {{lock}}

provision-check:
    cargo run -q -p provision -- check

subscription-schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction -f examples/subscription/001_subscription.sql

# Clears the subscription data, then has customers change plans repeatedly while workers apply
# the changes with step_latest. Pass "step" to use a plain step and watch older plans win.
subscription-race customers="20" changes="10" workers="10" mode="step_latest":
    cargo run -q -p subscription -- race {{customers}} {{changes}} {{workers}} {{mode}}

subscription-check:
    cargo run -q -p subscription -- check

tickets-schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction -f examples/tickets/001_tickets.sql

tickets-submit request_id attendee="Ada":
    cargo run -p tickets -- submit {{quote(request_id)}} {{quote(attendee)}}

tickets-process:
    cargo run -p tickets -- work
