# 0008: UPDATE SET-clause evaluation order is left-to-right and self-referencing

## Problem found during the Phase 1.2 audit

Both `claimTaskImpl` (`src/db/task-store.ts`) and
`tryAcquireOrRenewPublisherLeadership` (`src/messaging/publisher-lock.ts`)
used this shape:

```sql
UPDATE t
   SET owner_id = ?,
       epoch = CASE WHEN owner_id = ? AND lease_expires_at >= NOW(3) THEN epoch ELSE epoch + 1 END,
       lease_expires_at = ...
 WHERE ...
```

An external review flagged that `owner_id` is assigned BEFORE the `epoch`
expression reads it. Verified empirically against this project's own
MySQL 8.4 instance:

```sql
CREATE TABLE t (owner VARCHAR(20), epoch INT);
INSERT INTO t VALUES ('A', 5);
UPDATE t SET owner = 'B', epoch = CASE WHEN owner = 'B' THEN 999 ELSE epoch END WHERE owner != 'ZZZ';
SELECT * FROM t;  -- owner='B', epoch=999
```

The row's original owner was `'A'`, so a naive reading of `WHEN owner =
'B'` should be false -- but it evaluates true, because MySQL evaluates a
multi-column `UPDATE`'s `SET` list left to right, and a later expression
referencing a column assigned EARLIER in the same list sees the
JUST-WRITTEN value, not the original row. (This matches the MySQL
Reference Manual's description of `UPDATE` assignment evaluation order
for single-table updates.)

## Was this an actual bug, or correct-by-accident?

Traced through every call path: **correct-by-accident, not wrong, but
fragile.** Because the identical `ownerId` value is used for every `?`
placeholder bound to the caller's own id, the corrupted `owner_id = ?`
comparison inside the `CASE` becomes vacuously true (the column was just
set to the caller's own id, and it's being compared to the caller's own
id). That leaves only the `lease_expires_at >= NOW(3)` half of the `AND`
doing real work -- and because the `WHERE` clause already restricts this
`UPDATE` to only ever match when either the row was unclaimed/unowned,
the lease had expired, or the caller IS the current owner, the one
situation where the corrupted check would have given a WRONG answer (a
different, non-expired owner) can never reach the `SET` clause at all.
So every one of the required properties below held in the actual
installed code -- but for a reason a reader could not verify by reading
the `CASE` expression alone, and a reason that would silently break if
the `WHERE` clause were ever loosened without someone re-deriving this
argument.

## Fix

Reordered the `SET` list so `epoch`'s `CASE` is evaluated FIRST, before
`owner_id` or `lease_expires_at` are reassigned:

```sql
UPDATE t
   SET epoch = CASE WHEN owner_id = ? AND lease_expires_at >= NOW(3) THEN epoch ELSE epoch + 1 END,
       owner_id = ?,
       lease_expires_at = ...
 WHERE ...
```

Now both references inside the `CASE` read the genuine pre-update row.
Correctness no longer depends on the `WHERE` clause's gating as a second,
unstated line of defense -- it holds by construction.

## Verified properties (real MySQL, see the regression tests)

- `claimTask`: `tests/integration/ownership.test.ts` /
  `tests/integration/retry-authorization.test.ts` (unchanged by this fix,
  still pass -- see below).
- `publisher_leader` lock, specifically:
  `tests/integration/publisher-lock-epoch.test.ts` adds direct, real-MySQL
  coverage for exactly the six properties the audit asked about: an
  expired leader replaced by a new one increments epoch; a valid renewal
  by the same owner preserves epoch; reacquisition after expiration always
  increments (the documented rule -- see below); a competing owner cannot
  acquire while the lease is unexpired; the expiration boundary is
  consistently `< NOW(3)` (expired) / `>= NOW(3)` (valid) on both sides;
  and a new owner never inherits the previous owner's epoch value itself
  (only ever `epoch+1` or a fresh/unchanged epoch it already held).

## Documented epoch rule for reacquisition after expiration

Reacquiring a lock/task after the previous holder's lease has expired
ALWAYS increments the epoch by exactly 1, regardless of how long it was
expired or how many previous holders there were. There is no "catch-up"
increment proportional to elapsed time or missed renewals -- epoch is a
takeover counter, not a clock. The new owner starts at
`previous_epoch + 1`, never at `previous_epoch` (that would mean
inheriting the old owner's epoch, which fencing depends on NOT happening)
and never at an arbitrary higher number (that would make epoch
comparisons in command fencing unpredictable).

## Alternatives rejected

- **Read-then-write in two statements (SELECT ... FOR UPDATE, then
  UPDATE with the epoch value computed in application code)**: works, and
  is arguably more readable, but trades one atomic statement for two
  round-trips plus an explicit lock-then-write pattern already used
  elsewhere in this codebase for a DIFFERENT reason (`applyTaskCommand`'s
  `FOR UPDATE` select exists because it needs to run DOMAIN logic between
  read and write, which a single UPDATE cannot do). Here there is no
  domain logic in between -- just a column comparison -- so reordering the
  single UPDATE's SET list is the smaller, equally-correct fix.
- **CTE/derived-table self-join to capture the "before" row**: more
  verbose than reordering two clauses, for the same result.
