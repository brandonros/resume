# The PostgreSQL server, without a database name; the Postgres tools must be on PATH.
server := env("RESUME_SERVER", "postgresql://localhost:5432")
export DATABASE_URL := env("DATABASE_URL", server + "/resume")
executor := "sql/executor/tables/*.sql sql/executor/functions/*.sql"
workflow := "sql/workflow/tables/*.sql sql/workflow/functions/*.sql sql/workflow/views/*.sql"
schema := executor + " " + workflow

# Drops and recreates the resume database with every schema, including the examples'.
reset:
    dropdb --if-exists --force --maintenance-db "{{server}}/postgres" resume
    createdb --maintenance-db "{{server}}/postgres" resume
    psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 --single-transaction \
        $(printf ' -f %s' {{schema}} examples/*/0*.sql)

# Runs every regression scenario in examples/checks against a temporary database.
check:
    #!/usr/bin/env bash
    set -euo pipefail
    check_db="resume_check_$$"
    createdb --maintenance-db "{{server}}/postgres" "$check_db"
    trap 'dropdb --if-exists --force --maintenance-db "{{server}}/postgres" "$check_db"' EXIT
    export DATABASE_URL="{{server}}/$check_db"
    psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 --single-transaction $(printf ' -f %s' {{schema}})
    cargo run --quiet -p checks

# Installs or refreshes the inspection views on an existing schema.
views:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction \
        $(printf ' -f %s' sql/workflow/views/*.sql)

# For operators: after checking the vendor, record an interrupted step_once step's output and
# continue the run, e.g. just resolve-step 7 send_welcome_email 42
resolve-step run key output:
    echo "select resume.reopen_run(:'run', :'key', :'output')" | psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 \
        -v run={{quote(run)}} -v key={{quote(key)}} -v output={{quote(output)}}

# For operators: put a failed run back in the queue with fresh attempts.
reopen-run run:
    echo "select resume.reopen_run(:'run')" | psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -v run={{quote(run)}}

# For operators: stop a run that has not finished. A run in progress stops at its next step.
cancel-run run:
    echo "select resume.cancel_run(:'run')" | psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -v run={{quote(run)}}

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
        -c "select id, idempotency_key as email, status, attempt, steps_completed, last_error \
            from resume.run_status where workflow = 'onboard' order by id" \
        -c "select * from vendors.charges order by id" \
        -c "select * from vendors.emails order by id"

# Clears the provision data, then has workers race to create VMs for requests across teams,
# under a per-team quota. Pass "unlocked" to drop the lock and watch teams go over quota.
provision-race teams="3" requests="50" workers="20" lock="locked":
    cargo run -q -p provision -- race {{teams}} {{requests}} {{workers}} {{lock}}

provision-check:
    cargo run -q -p provision -- check

# Clears the shipping data, then ships orders while customers cancel them, checking each order
# is still paid in the same step that ships it. Pass "separate" to check in an earlier step and
# watch cancelled orders ship.
shipping-race orders="200" workers="10" mode="same_step":
    cargo run -q -p shipping -- race {{orders}} {{workers}} {{mode}}

# Clears the subscription data, then has customers change plans repeatedly while workers apply
# the changes, skipping any a newer request replaced (is_latest). Pass "plain" to leave out the
# check and watch older plans win.
subscription-race customers="20" changes="10" workers="10" mode="is_latest":
    cargo run -q -p subscription -- race {{customers}} {{changes}} {{workers}} {{mode}}

subscription-check:
    cargo run -q -p subscription -- check

tickets-submit request_id attendee="Ada":
    cargo run -p tickets -- submit {{quote(request_id)}} {{quote(attendee)}}

tickets-process:
    cargo run -p tickets -- work

checkout-submit key mode="fail":
    cargo run -p checkout -- submit {{quote(key)}} {{quote(mode)}}

# Runs the checkout and failure-handler workers in one process.
checkout-process:
    cargo run -p checkout -- work

checkout-show:
    psql "$DATABASE_URL" -X \
        -c "select id, workflow, status, attempt, last_error from resume.run_status \
            where workflow like 'checkout%' order by id" \
        -c "select * from checkout.events order by id" \
        -c "select * from checkout.notifications order by failed_run"

# Submit a job and wait for its outcome, with a caller-side timeout in milliseconds.
waiting-submit key mode="success" timeout_ms="5000":
    cargo run -p waiting -- submit {{quote(key)}} {{quote(mode)}} {{quote(timeout_ms)}}

waiting-process:
    cargo run -p waiting -- work
