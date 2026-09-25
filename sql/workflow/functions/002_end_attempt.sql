-- Workflow retry policy: computes backoff and delegates the transition to finish_attempt.
-- Permanent errors supply no retry delay. The executor independently enforces the budget.
-- Returns the retry delay in seconds, or null on terminal failure.
create or replace function resume.end_attempt(
    p_run_id bigint,
    p_attempt bigint,
    p_error text,
    p_permanent boolean
)
returns double precision
language plpgsql
as $$
declare
    v_run resume.runs;
    v_delay interval;
begin
    if p_permanent then
        return resume.finish_attempt(p_run_id, p_attempt, p_error, null);
    end if;

    v_run := resume.lock_run(p_run_id, p_attempt);

    -- Double the delay for each attempt used, up to the maximum, then keep a random 50-100%
    -- of it so runs that failed together do not all retry at the same moment.
    v_delay := least(
        v_run.retry_max_delay,
        v_run.retry_delay * power(2, least(v_run.attempts_used - 1, 30))
    ) * (0.5 + random() / 2);
    return resume.finish_attempt(p_run_id, p_attempt, p_error, v_delay);
end;
$$;
