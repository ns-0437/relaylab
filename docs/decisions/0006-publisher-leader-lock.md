# 0006: Publisher leader lock -- a single-row epoch+lease lock, reusing the task-ownership mechanism

## Problem

Phase 1's original design merely documented "run exactly one publisher
process" as an unenforced assumption. The Phase 1.1 audit required either
enforcing that, or making concurrent publishing compatible with consumer
semantics, with "the smallest defensible mechanism compatible with the
existing database and lifecycle design."

## Decision

A single row in a new `publisher_leader` table (`lock_name` PK, `owner_id`,
`epoch`, `lease_expires_at`) is claimed and renewed with the EXACT SAME
SQL pattern as task ownership in `task-store.ts`'s `claimTaskImpl`: one
`UPDATE ... WHERE (unclaimed OR lease expired OR same owner)` statement,
relying on InnoDB row locking for mutual exclusion, with the database's
own `NOW(3)` as the only clock consulted (see 0001 -- the same clock-skew
hazard applies here and is avoided the same way).

`publisher-main.ts` calls `tryAcquireOrRenewPublisherLeadership` once per
poll iteration (short lease, 3s) before calling `runPublisherOnce`. If it
is not the leader, it skips publishing that iteration and tries again
next iteration. When the active leader stops renewing (crashed, lost its
DB connection), its lease expires and the next process to try takes over,
with `epoch` incrementing -- exactly like a task handoff.

## Scope and failure behavior (read before assuming more than this gives)

- **Scope**: one lock per NATS namespace (`lock_name = 'default'` for
  Phase 1, since there is one events stream). A second namespace would
  need its own lock row if it were ever added.
- **What happens when the owner loses its DB connection**: it simply
  stops being able to renew (the next `UPDATE` from it would fail or, if
  its own connection is dead, it cannot even attempt one); its lease
  expires on schedule and a healthy process takes over. There is no
  separate "heartbeat" mechanism beyond the lease itself.
- **What this does NOT guarantee, explicitly**: a lease expiring does
  **not** reach into another process and cancel an in-flight `js.publish()`
  network call. If the old leader's call to NATS was already in flight
  when its lease expired, that call can still complete after a new leader
  has taken over. The two possible outcomes are:
  1. The old leader's call fails (connection reset, etc.) -- no issue.
  2. The old leader's call succeeds -- the SAME outbox row may now be
     published twice, under the SAME `event_id`/`msgID`. This is
     harmless: NATS's own msgID dedup window absorbs it in the common
     case (see `tests/integration/consumer-dedup.test.ts`, "publisher:
     republishing..."), and even if that broker-level dedup ever failed,
     the consumer's OWN durable dedup (`consumer_processed`, keyed on
     application `event_id`+content hash, NOT on NATS delivery identity --
     see 0005) is what actually prevents a duplicate local effect. The
     lock's job is to make polling/batch ordering single-threaded in the
     common case, not to fence the network call itself.
  This is why consumers were built to be correct under duplicate OR
  reordered delivery independently of this lock (sections 3 and 5 of the
  audit) -- the lock narrows how OFTEN that correctness gets exercised, it
  is not the thing making it correct.

## Alternatives rejected

- **A dedicated distributed lock service (etcd, Redis Redlock, ZooKeeper)**:
  explicitly rejected by the audit's own instruction ("do not add an
  elaborate distributed lock framework unnecessarily") and for the same
  reason as 0001 -- MySQL is already the durable, consulted-for-everything
  store; a second lock service would be a new operational dependency for a
  guarantee the existing one already provides at this scale.
- **A process-local boolean / `pm2`-style "only run one instance" convention**:
  explicitly rejected by the audit ("a process-local boolean ... is
  insufficient") -- it provides no automatic failover and nothing stops a
  second instance from actually running.
- **Fencing tokens enforced at the NATS publish call itself** (e.g.
  rejecting a publish from a stale epoch at the broker): NATS has no
  built-in concept of "publisher epoch"; building one would mean wrapping
  every publish in an application-level fencing check against the epoch,
  which is strictly more mechanism for a guarantee the lock plus
  consumer-side dedup already deliver together. Revisit only if a
  measured problem (not a hypothetical one) shows up.
