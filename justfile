export DATABASE_URL := "postgresql://brandon@localhost:5432/brandon"
export PATH := "/Users/brandon/Applications/Postgres.app/Contents/Versions/18/bin:" + env("PATH")

schema:
    psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 --single-transaction \
        -f sql/001_runs.sql \
        -f sql/002_steps.sql \
        -f sql/003_enqueue.sql \
        -f sql/004_claim.sql \
        -f sql/005_save_step.sql \
        -f sql/006_finish_run.sql \
        -f examples/three_steps/001_results.sql

three-steps-enqueue:
    cargo run -p three_steps -- enqueue

three-steps-process:
    cargo run -p three_steps -- work
