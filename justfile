export DATABASE_URL := "postgresql://brandon@localhost:5432/brandon"
export PATH := "/Users/brandon/Applications/Postgres.app/Contents/Versions/18/bin:" + env("PATH")

schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction \
        -f sql/tables/001_runs.sql \
        -f sql/tables/002_steps.sql \
        -f sql/tables/003_step_starts.sql \
        -f sql/functions/001_lock_run.sql \
        -f sql/functions/002_submit_run.sql \
        -f sql/functions/003_claim_run.sql \
        -f sql/functions/004_complete_run.sql \
        -f sql/functions/005_fail_run.sql \
        -f sql/functions/006_load_step.sql \
        -f sql/functions/007_start_step.sql \
        -f sql/functions/008_save_step.sql

counter-schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction -f examples/counter/001_results.sql

counter-submit key:
    cargo run -p counter -- submit {{quote(key)}}

counter-process:
    cargo run -p counter -- work

tickets-schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction -f examples/tickets/001_tickets.sql

tickets-submit request_id attendee="Ada":
    cargo run -p tickets -- submit {{quote(request_id)}} {{quote(attendee)}}

tickets-process:
    cargo run -p tickets -- work
