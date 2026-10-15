# 0005: event_id reuse with different content is a protocol violation

## Decision

`consumer_processed` now stores a `content_hash` (stable-stringified
SHA-256 of `{taskId, seq, type, payload}`) alongside each `event_id`. When
a message arrives whose `event_id` is already known but whose
`content_hash` differs, the consumer does NOT treat it as an ordinary
duplicate (silently ack, do nothing) and does NOT apply it as a new event.
It records the conflict in `consumer_event_conflicts` (first hash,
conflicting hash, when detected) and returns the distinct outcome
`"conflict"` -- logged loudly, acked (so it does not jam redelivery), but
never silently absorbed.

This should never happen if the publisher is implemented correctly
(`event_id` is minted once, at outbox-insert time, and never reused for a
different logical event). Detecting it anyway is a protocol-bug tripwire,
not a feature the normal data path relies on.

## Why detect this at all instead of just trusting `event_id` uniqueness

Section 3 of the Phase 1.1 audit asked explicitly: "do not silently treat
conflicting content as an ordinary duplicate." The two real tests this
protects against:
- A future bug in the publisher or a schema migration that accidentally
  reuses an `event_id` (e.g. a sequence reset, or a bad backfill script).
- Two independent publisher processes racing (see
  docs/decisions/0006-publisher-leader-lock.md) and somehow generating
  colliding IDs through a bug in ID generation, rather than the normal
  `randomUUID()` path.

## Alternatives rejected

- **Trust `event_id` uniqueness and skip the content hash entirely**:
  cheaper, but turns a real bug into silent data corruption (whichever
  version arrives second would either silently overwrite nothing, since
  dedup already blocks it, OR -- if dedup were ever keyed differently --
  silently apply the wrong content under an old identity). Explicitly
  rejected by the audit's own instruction.
- **Reject the whole batch / crash the consumer on conflict**: too
  aggressive for Phase 1 -- one malformed/colliding event_id would stop
  the consumer from making progress on every OTHER unrelated event in the
  same batch. Recording + logging + continuing keeps the blast radius to
  the one conflicting event.
