# 0002: Idempotency keys and fingerprinting

## Decision

- Command identity: `(task_id, command_id)`, unique in `command_receipts`.
  `command_id` is caller-supplied (so a retrying caller can present the
  same ID). `fingerprint = sha256(stableStringify(command payload))`
  detects accidental or malicious ID reuse with a different payload.
- Event identity: `event_id`, a UUID minted when the outbox row is
  inserted (inside the same transaction as the command receipt). Globally
  unique, not scoped to a task, because the consumer's dedup table
  (`consumer_processed`) is scoped by `(consumer_name, event_id)` and a
  consumer may in principle subscribe across tasks.

## Why `stableStringify` (recursive key sort) instead of raw `JSON.stringify`

`JSON.stringify({a:1,b:2})` and `JSON.stringify({b:2,a:1})` are different
strings but the same logical command. Fingerprinting the raw string would
make the conflict check depend on incidental client-side object
construction order -- a caller retrying with a differently-ordered object
(e.g. because it was rebuilt from a `Map` or came from a different
serializer) would be wrongly flagged as `CommandConflict`. Sorting object
keys recursively (arrays keep order, since order is semantically
meaningful there) removes that dependency. See
`src/domain/fingerprint.ts` and `tests/unit/fingerprint.test.ts`.

## Alternatives rejected

- **Deep-equal comparison against the stored payload JSON** instead of a
  hash: works, but means every retry re-parses and deep-compares the full
  stored payload. A fixed-length hash comparison is cheaper and the stored
  `fingerprint CHAR(64)` column can be indexed trivially if needed later.
- **Client-chosen idempotency key with no payload binding** (Stripe-style
  bare idempotency key, no fingerprint check): rejected because it silently
  accepts "retry with a different payload" as if it were the original
  command, which is a correctness hazard the spec explicitly calls out
  (invariant C: "reusing the same ID for a different payload is rejected").
