# 0003: Event ordering -- durable pending events with cascade application

**Revised during the Phase 1.1 audit.** The original version of this
decision (kept below, "Superseded Phase 1 policy") was a real gap: it
applied an out-of-order event immediately and only set a flag, meaning the
projection silently advanced past a missing event. Section 5 of the
Phase 1.1 audit required a bounded, coherent policy that never does that.

## Current decision

Each outbox event carries a per-task `seq`. The consumer tracks
`last_applied_seq` per `(consumer_name, task_id)`: the point up to which
the projection is CONTIGUOUSLY complete.

- `seq === last_applied_seq + 1`: applied immediately, THEN any
  contiguous pending rows (`last_applied_seq+2`, `+3`, ...) are drained
  from `task_consumer_pending` and applied in order, in the SAME
  transaction ("cascade apply").
- `seq > last_applied_seq + 1`: durably buffered in
  `task_consumer_pending` (a real MySQL table, not an in-memory queue --
  survives a process restart). NOT applied. `gap_detected = 1` is set so
  the projection visibly reports itself as incomplete.
- Bounded: `maxPendingPerTask` (default 50) caps how many early events one
  task can have buffered. Past the cap, further early events are acked
  (so they do not jam redelivery of OTHER tasks/events) but logged loudly
  and left unapplied -- the gap is a stuck, visible condition
  (`task_consumer_state.gap_detected = 1`, queryable), not a silent stall
  and not unbounded growth.
- Every outcome (applied, buffered, deduped, conflict) results in an ACK.
  A message is never left unacked waiting for a gap to close, so
  redelivery of an ALREADY-DURABLY-RECORDED event can never become a
  permanent retry loop. (A gap that never closes is a stuck PROJECTION,
  not a stuck MESSAGE -- the broker's view of that message is already
  settled.)

Answering section 5's specific questions directly, for a 1, 3, 2 delivery:
- Is sequence 3 acknowledged? Yes.
- Is its effect applied? No -- durably buffered, not applied.
- Is it durably stored for later? Yes, in `task_consumer_pending`.
- Does the projection show itself as incomplete? Yes, `gap_detected = 1`.
- Does arrival of sequence 2 restore correctness? Yes -- applying 2
  cascades into applying the buffered 3, and `gap_detected` clears once
  no pending rows remain.
- Can redelivery create a permanent retry loop? No -- every outcome acks.

See `src/messaging/consumer.ts` (`applyEventWithDedup`) and
`tests/integration/gap-handling.test.ts` (1-3-2, duplicates around the
gap, durability across a simulated restart, and the bounded-cap case).

## Alternatives rejected (still valid)

- **NATS subject-per-task with a single consumer per subject**: gives
  strict per-task order from the broker, but multiplies the number of
  durable consumers by the number of tasks, which does not fit Phase 1's
  single-stream design and was explicitly out of scope to build now.
- **Reject (NAK) out-of-order events until the gap fills**: would create
  exactly the permanent-retry-loop risk section 5 asks to rule out, if the
  missing event's publish had permanently failed.
- **An in-memory reorder buffer**: explicitly rejected by the audit
  ("avoid an unbounded in-memory buffer") and also not durable across a
  process restart, unlike the MySQL-table-backed buffer actually used.

---

## Superseded Phase 1 policy (kept for history, DO NOT implement this)

> Each outbox event carries a per-task `seq`. The consumer tracks
> `(consumer_name, task_id) -> last_seq` and, on receiving an event with
> `seq > last_seq + 1`, sets `gap_detected = 1` on that row but STILL
> APPLIES the event and advances `last_seq`.

This was replaced because it violates the core requirement: a "complete"
projection (as far as `last_seq` claimed) could silently include an event
applied out of order relative to a still-missing one, with no mechanism
to ever go back and apply the missing event in the correct position.
