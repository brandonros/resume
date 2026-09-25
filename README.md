# resume

A toy durable workflow engine.

Run `just check` for the [runnable regression checks](examples/checks/README.md).
It creates and removes its own temporary PostgreSQL database.

See [checkout](examples/checkout/README.md) for cleanup in a run failure handler, including a crash during cleanup and recovery after restart.
