-- Every terminal failure path (including deadlines and cancellation) queues the handler
-- in the same transaction. The handler is an ordinary run with three attempts.
create or replace function resume.enqueue_failure()
returns trigger
language plpgsql
as $$
begin
    insert into resume.runs (workflow, version, idempotency_key, input, max_attempts)
    values (
        new.on_failure_workflow, new.on_failure_version,
        format('resume:on_failure:%s', new.id),
        jsonb_build_object('failed_run', new.id, 'error', new.last_error, 'input', new.input),
        3
    );
    return new;
end;
$$;

create trigger enqueue_failure
after update of failed_at on resume.runs
for each row
when (old.failed_at is null and new.failed_at is not null and new.on_failure_workflow is not null)
execute function resume.enqueue_failure();
