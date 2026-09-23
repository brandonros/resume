export DATABASE_URL := "postgresql://brandon@localhost:5432/brandon"
export PATH := "/Users/brandon/Applications/Postgres.app/Contents/Versions/18/bin:" + env("PATH")

schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction \
        -f sql/001_runs.sql \
        -f sql/002_steps.sql \
        -f sql/003_enqueue.sql \
        -f sql/004_claim.sql \
        -f sql/005_save_step.sql \
        -f sql/006_finish_run.sql

counter-schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction -f examples/counter/001_results.sql

counter-enqueue:
    cargo run -p counter -- enqueue

counter-process:
    cargo run -p counter -- work

tickets-schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction -f examples/tickets/001_tickets.sql

tickets-enqueue request_id attendee="Ada":
    cargo run -p tickets -- enqueue {{quote(request_id)}} {{quote(attendee)}}

tickets-process:
    cargo run -p tickets -- work
