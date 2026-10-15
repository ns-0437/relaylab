# 0007: claimTask is idempotent for the current rightful owner; renewLease is merely safe-to-retry

## Problem found during the Phase 1.1 audit

`claimTaskImpl`'s original `UPDATE ... WHERE (status = 'unclaimed' OR
lease_expires_at < NOW(3))` only matched an unclaimed or expired row. If
the RIGHTFUL CURRENT owner retried its own successful claim (e.g. because
the first response was lost to a network blip, a classic retry trigger),
the retry's `UPDATE` matched zero rows (the row is already `owned`, by
the same caller, with a still-valid lease) and threw a spurious
`ClaimLost` -- even though nothing was actually wrong.

## Decision

The `UPDATE`'s `WHERE` clause now also matches `owner_id = ?` (the
caller's own id), and the `SET epoch = ...` expression only increments
when the row is NOT already held by this same caller with a valid lease:

```sql
UPDATE tasks
   SET owner_id = ?,
       epoch = CASE WHEN owner_id = ? AND lease_expires_at >= NOW(3) THEN epoch ELSE epoch + 1 END,
       lease_expires_at = DATE_ADD(NOW(3), INTERVAL ? MICROSECOND),
       status = 'owned'
 WHERE task_id = ?
   AND status != 'completed'
   AND (status = 'unclaimed' OR lease_expires_at IS NULL OR lease_expires_at < NOW(3) OR owner_id = ?)
```

So: claiming while unclaimed, or taking over after expiry, still
increments `epoch` (a genuine takeover, which MUST fence out the old
owner's in-flight commands -- invariant B). Re-claiming while you already
validly hold it does not increment `epoch` (it is the same claim
continuing, not a takeover) -- this is what makes it safe to retry.

## claimTask vs renewLease vs applyTaskCommand: three different idempotency stories

| Operation | Retry semantics | Mechanism |
|---|---|---|
| `applyTaskCommand` | Strictly idempotent: identical (command_id, payload, owner_id) always returns the identical stored result, never re-runs the transition | `command_receipts` ledger (0004) |
| `claimTask` | Idempotent FOR THE RIGHTFUL OWNER: retrying your own valid claim succeeds again with the SAME epoch | The `owner_id = ?` branch in the `UPDATE` above |
| `renewLease` | Safe to retry, but NOT idempotent in the strict sense: each successful retry extends the lease further. Retrying never fails incorrectly and never corrupts anything, but two successful calls do not produce identical results (the second extends the expiry past the first) | The lease-extension `UPDATE` always moves `lease_expires_at` forward by a fresh `leaseMs` from "now" |

`renewLease` was deliberately NOT given a receipt-style idempotency
ledger: a renewal's only effect is "push the expiry further out," and
pushing it out twice in a row is indistinguishable in consequence from
pushing it out once and then renewing again slightly later -- there is no
"wrong" outcome to guard against the way there is for `applyTaskCommand`
(a double side effect) or `claimTask` (a spurious failure for the
rightful owner).

## Alternatives rejected

- **Give claimTask its own idempotency-key ledger (like command_receipts)**:
  unnecessary -- the `owner_id`-aware `UPDATE` above already gives the
  needed property (safe retry by the rightful owner) using the existing
  single-statement, row-lock-based mechanism, without a second ledger
  table to keep consistent.
- **Have callers catch `ClaimLost` and call `getTask` to check if they
  already own it**: workable, but pushes a footgun onto every caller
  (forget this pattern once, get a spurious failure) for a problem fixable
  once, at the source.
