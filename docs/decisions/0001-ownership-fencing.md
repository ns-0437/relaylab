# 0001: Ownership fencing via epoch + authoritative-clock lease

## Decision

A task's authoritative owner and epoch live in MySQL, not in message
delivery or in-memory worker state. `claimTask` is a single `UPDATE ...
WHERE status='unclaimed' OR lease_expires_at < NOW(3)`, relying on InnoDB
row-level locking for mutual exclusion between concurrent claimants.
`epoch` increments by exactly 1 on every successful claim/takeover.
Commands carry the epoch the caller believes is current; `applyTaskCommand`
re-checks it against the row under `FOR UPDATE` before any mutation.

## Alternatives rejected

- **Distributed lock service (etcd/Redis Redlock)**: adds an operational
  dependency for a guarantee MySQL row locking already provides, given that
  MySQL is already the durable state store. Revisit only if lease churn
  becomes a measured bottleneck.
- **Message-order-implies-ownership** (whoever's message arrives "first" at
  the consumer owns the task): rejected outright -- this is exactly what
  invariant A forbids. NATS does not guarantee a single global delivery
  order across redelivery/retry, so this would be silently wrong under
  load, not just inelegant.
- **Application-level mutex (in-process lock, e.g. a Map<taskId, Promise>)**:
  only serializes within one Node process; does nothing for two separate
  worker processes, which is the actual scenario being protected against.

## Consequence

Lease renewal and fencing checks always read-then-write the same row inside
one transaction (`SELECT ... FOR UPDATE` then `UPDATE`), so every ownership
decision pays one row lock's worth of contention per task. This is fine at
Phase-1 scale (one row per task, contention only during a takeover race)
and is called out in CLAUDE.md as a thing to revisit if task counts grow
large enough for lock contention on hot tasks to matter.
