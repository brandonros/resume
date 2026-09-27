# Examples

Install the root schema first and set `RESUME_CONNECTION_STRING`. Prefix each
command below with `dotnet run --project examples/Resume.Examples --`.

| Command | Demonstrates |
| --- | --- |
| `basic` | Transactional greeting step; runs a worker until Ctrl-C. |
| `flow` | Child workflows, joins, durable sleep, and recurring submissions. |
| `recovery` | Lost external response, verified receipt, explicit requeue. |
| `orders init`, `orders stock book 10`, `orders submit order-1 book 2`, `orders drain`, `orders status order-1` | Atomic submission with application data, inventory reservation, child dispatch. |
| `importer init`, `importer load import-1 examples/importer/sample.jsonl`, `importer drain`, `importer status import-1` | Input snapshot, batches, deduplication, durable rejections, joined results. |

Orders and importer also offer `work` for continuous workers. `drain` exits when
nothing is currently runnable; paused, scheduled, or waiting jobs may remain.
Dispatch is a local ledger, not a carrier integration. Business validation,
batch sizes, retry budgets, and schedules remain application choices.

## Compensation

The fake provider uses a separate connection. Optionally set
`RESUME_PAYMENT_CONNECTION_STRING` to use a separate database. No real money
moves. Its first charge and refund responses are deliberately lost after commit.

```text
compensation init
compensation submit order-1 2 500
compensation run                 # charge committed, response lost; pauses
compensation status order-1
compensation reconcile order-1  # inspect provider, save verified receipt
compensation resume order-1
compensation run                 # confirmed rejection; refund response lost
compensation run                 # same refund key; release inventory
compensation status order-1
```

The two lost-response runs intentionally return errors. Compensation names and
receipts are durable; keep their interpretation compatible when deploying changes.
Missing provider evidence leaves an unknown outcome unresolved. A distributed
lock would not establish whether the payment happened.
