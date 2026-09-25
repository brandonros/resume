server := "postgresql://brandon@localhost:5432"
export DATABASE_URL := server + "/resume"
export PATH := "/Users/brandon/Applications/Postgres.app/Contents/Versions/18/bin:" + env("PATH")

# Drops and recreates the resume database with every schema.
reset: && schema counter-schema onboard-schema tickets-schema
    dropdb --if-exists --force --maintenance-db "{{server}}/postgres" resume
    createdb --maintenance-db "{{server}}/postgres" resume

schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction \
        $(printf ' -f %s' sql/tables/*.sql sql/functions/*.sql)

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

tickets-schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction -f examples/tickets/001_tickets.sql

tickets-submit request_id attendee="Ada":
    cargo run -p tickets -- submit {{quote(request_id)}} {{quote(attendee)}}

tickets-process:
    cargo run -p tickets -- work
