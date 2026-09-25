create schema reminders;

-- A local notification inbox: inserting here represents delivering the reminder.
create table reminders.deliveries (
    run_id bigint primary key references resume.runs (id),
    message text not null,
    delivered_at timestamptz not null default clock_timestamp()
);
