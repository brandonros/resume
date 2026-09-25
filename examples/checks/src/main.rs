//! Runnable regression scenarios against a fresh PostgreSQL database. Run with `just check`.
mod ownership;
mod recovery;
mod scheduling;
mod snooze;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    macro_rules! check {
        ($scenario:path) => {
            println!("Checking {}", stringify!($scenario));
            tokio::time::timeout(std::time::Duration::from_secs(30), $scenario())
                .await
                .expect(concat!("scenario timed out: ", stringify!($scenario)));
        };
    }

    check!(ownership::new_claim_rejects_previous_attempt);
    check!(ownership::attempt_that_handed_back_the_run_cannot_complete_it);
    check!(ownership::failed_step_keeps_ownership_to_record_the_failure);
    check!(ownership::step_saves_after_outlasting_its_lease);
    check!(ownership::expired_lease_cannot_start_a_step);
    check!(ownership::completed_step_replays_its_output);
    check!(scheduling::delayed_runs_are_stored_but_only_claimed_when_due);
    check!(scheduling::earlier_deadline_fails_a_scheduled_run_without_claiming_it);
    check!(scheduling::submission_rejects_invalid_schedules_and_zero_is_immediately_eligible);
    check!(scheduling::absolute_times_preserve_offsets_and_duplicate_submissions_keep_the_schedule);
    check!(scheduling::past_times_are_eligible_and_the_last_schedule_setter_wins);
    check!(scheduling::an_absolute_schedule_cannot_postpone_the_deadline);
    check!(snooze::snooze_releases_worker_and_locks_preserves_progress_and_costs_no_retry);
    check!(snooze::snooze_cannot_delay_the_deadline);
    check!(snooze::step_once_cannot_snooze_and_repeat_its_action);
    check!(snooze::release_validates_delay_and_zero_releases_immediately);
    check!(recovery::failure_and_handler_commit_together);
    check!(recovery::every_terminal_path_queues_the_handler);
    check!(recovery::success_retry_and_snooze_do_not_queue_handlers);
    check!(recovery::failed_handler_reopens_with_saved_progress);
    check!(recovery::worker_exhaustion_queues_handler_without_step_output);
    println!("All 21 checks passed.");
}
