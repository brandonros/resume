# Cleanup in a failure handler

Checkout charges a payment, reserves inventory, then fails shipping. One ordinary failure
workflow releases inventory, refunds payment, and records a notification, using three saved steps.

```rust
Producer::new(&client, "checkout", VERSION)
    .on_failure("checkout_failure", VERSION)
    .submit("order-1", &input)
    .await?;
```

After `just reset`, which installs every schema, run from the repository root:

```sh
just checkout-submit order-1
just checkout-process
```

In another terminal, `just checkout-show` shows the runs and vendor events. Payments and
notifications are local mock database records.

To crash after the refund commits but before its step saves the result:

```sh
just checkout-submit order-2 crash-refund
just checkout-process  # exits with code 99
just checkout-process  # resumes after the three-second lease expires
```

Release's saved step is replayed. Refund is safely repeated, then notification runs.

The framework queues the handler in the same transaction that fails the original run,
including exhausted retries, deadlines, and cancellation. Ordinary retries and snoozes do
not queue it. Its input is `{"failed_run": id, "error": reason, "input": original_input}`.
It gets three attempts and otherwise behaves like any workflow.

The application chooses what to undo and in which order. Cleanup must use stable resource
keys and tolerate absent effects and repeated calls: missing step output does not mean the
vendor did nothing. This mock records an undo even for absent effects to prevent a late
forward call from recreating them. Real vendors need equivalent coordination or reconciliation.

If cleanup fails, inspect its ordinary run and steps, fix the cause, then use
`just reopen-run <handler_run_id>`. Its saved progress remains. The original run stays failed
and cannot reopen once it has handed responsibility to its failure handler.

The `resume:on_failure:<id>` idempotency key is reserved for framework-created handler runs.
