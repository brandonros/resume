//! Runnable regression scenarios against a fresh PostgreSQL database. Run with `just check`.
mod common;
mod errors;
mod expiry;
mod ownership;
mod policy;
mod recovery;
mod scheduling;
mod snooze;
mod waiting;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut passed = 0;
    macro_rules! check {
        ($scenario:path) => {
            println!("Checking {}", stringify!($scenario));
            tokio::time::timeout(std::time::Duration::from_secs(30), $scenario())
                .await
                .expect(concat!("scenario timed out: ", stringify!($scenario)));
            passed += 1;
        };
    }

    check!(expiry::cleanup_is_bounded_skips_locks_and_claims_ignore_unswept_runs);
    check!(ownership::new_claim_rejects_previous_attempt);
    check!(ownership::attempt_that_handed_back_the_run_cannot_complete_it);
    check!(ownership::failed_step_keeps_ownership_to_record_the_failure);
    check!(ownership::step_saves_after_outlasting_its_lease);
    check!(ownership::expired_lease_cannot_start_a_step);
    check!(ownership::completed_step_replays_its_output);
    check!(ownership::passing_the_deadline_fails_the_run_without_starting_the_step);
    check!(ownership::a_step_key_used_twice_fails_the_run);
    check!(ownership::shutdown_finishes_current_step_and_releases_saved_progress);
    check!(errors::wrapped_errors_preserve_failure_and_snooze_policy);
    check!(errors::nonretryable_claim_error_stops_the_worker);
    check!(errors::transient_claim_error_retries_then_processes_work);
    check!(policy::executor_finish_enforces_ownership_budget_and_deadline);
    check!(policy::retry_policy_uses_charged_attempts_and_caps_backoff);
    check!(policy::submission_is_atomic_and_duplicate_keeps_winning_policy);
    check!(policy::new_policy_constraints_and_duplicate_identity);
    check!(scheduling::delayed_runs_are_stored_but_only_claimed_when_due);
    check!(scheduling::earlier_deadline_fails_a_scheduled_run_without_claiming_it);
    check!(scheduling::submission_rejects_invalid_schedules_and_zero_is_immediately_eligible);
    check!(scheduling::absolute_times_preserve_offsets_and_duplicate_submissions_keep_the_schedule);
    check!(scheduling::past_times_are_eligible_and_the_last_schedule_setter_wins);
    check!(scheduling::an_absolute_schedule_cannot_postpone_the_deadline);
    check!(scheduling::reopening_a_run_that_passed_its_deadline_clears_the_deadline);
    check!(snooze::snooze_releases_worker_and_locks_preserves_progress_and_costs_no_retry);
    check!(snooze::snooze_cannot_delay_the_deadline);
    check!(snooze::step_once_cannot_snooze_and_repeat_its_action);
    check!(snooze::release_validates_delay_and_zero_releases_immediately);
    check!(recovery::failure_and_handler_commit_together);
    check!(recovery::every_terminal_path_queues_the_handler);
    check!(recovery::success_retry_and_snooze_do_not_queue_handlers);
    check!(recovery::failed_handler_reopens_with_saved_progress);
    check!(recovery::reserved_handler_keys_cannot_be_submitted);
    check!(recovery::reopen_resolves_an_unknown_step_once_outcome);
    check!(recovery::completion_rejects_unresolved_and_unvisited_steps);
    check!(recovery::step_once_error_fails_the_run_without_retrying);
    check!(recovery::cancelling_during_a_step_once_action_keeps_its_result);
    check!(recovery::worker_exhaustion_queues_handler_without_step_output);
    check!(waiting::timeout_leaves_job_available_and_wait_observes_completion);
    check!(waiting::wait_reports_failure_cancellation_missing_jobs_and_query_timeout);
    println!("All {passed} checks passed.");
}
