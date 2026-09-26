# Findings from application exercises

`orders` reserves inventory and uses a child job to record dispatch. `importer`
snapshots a JSON-lines file, imports batches as children, retains rejected rows,
and joins the results. Dispatch is a local ledger, not a carrier integration.
`compensation` handles an uncertain charge, a confirmed shipment rejection, and
durable refund/release steps using `Steps::saga`. Its fake payment provider can use a separate database.

| Repeated need | Evidence | Decision |
| --- | --- | --- |
| Inspect child failures | A parent can be waiting while a child is paused; the parent's error is empty. | Added `parent_id` and `failures` to `resume.job_status`. Both app status commands show children. |
| Classify database errors | Both apps inspect serialization/deadlock errors and cap reported failures. | Candidate for a small optional classifier if another integration needs it. Retry limits and delays remain app policy. |
| Poll multiple workflows | Both apps have a short `drain` loop and separate clients for continuous parent/child workers. | Keep local for now. Idle does not mean completed: delayed or paused jobs can remain. A shared runner would need an explicit result/error contract. |
| Durable business validation | Orders record insufficient stock as a rejection; imports retain bad rows and continue. | App logic. These are successful business outcomes, not necessarily retryable job failures. |
| Stable input and deduplication | Orders submit a job and business record in one transaction; imports snapshot input and compare duplicate event content. | App chooses keys, transaction boundaries, and conflict policy using existing primitives. |
| Batch size and fan-out | Import batches contain 100 rows; each child commits effects and its saved result together. | App policy. No generic batch API needed yet. |
| Compensation and reconciliation | Named undo handlers receive saved forward receipts. The app verifies unknown effects and explicitly requests compensation. | The saga API persists the decision, enforces reverse order, and checkpoints compensation. Undo semantics and provider idempotency remain application responsibilities. |

All runs used disposable local PostgreSQL databases and debug Rust builds:

| Workload | Result |
| --- | --- |
| 32 parents, 256 children, 8 workers; every child fails once after committing an effect | Passed; all completed, each effect recorded once. |
| 1,000 parents, 8,000 children, 32 workers; 8,000 injected failures | Passed in about 12 seconds; maximum observed parent claims: 6. |
| 128 parents, 1,024 children, 2 workers; 1,024 injected failures | Passed in about 2.3 seconds. |
| 64 competing two-unit orders against 65 units, 8 workers | Exactly 32 shipped, 32 rejected, one unit remaining. |
| 205 valid import rows plus malformed/conflicting rows; pause after batch commit, inspect, requeue | Completed with 205 accepted and two rejected; replay did not duplicate effects. |
| Compensation across two databases; lost charge/refund responses, then worker termination after the refund checkpoint | One charge, one refund effect from two requests, inventory released once. Missing provider evidence left the job unresolved. |

These are bounded correctness exercises, not capacity or uptime guarantees. They
do not simulate lost commit acknowledgments, database failover, real carrier
failures, or a prolonged network partition. No new engine bug was reproduced.

For each run, install `schema.sql` into a fresh disposable database and set
`DATABASE_URL`. Run all checks with `cargo test --all-targets -- --ignored`.
Scale the stress case with `RESUME_STRESS_PARENTS` and `RESUME_STRESS_WORKERS`
(at least two workers): `cargo test --test stress -- --ignored --nocapture`.
