# 0004: Command receipt replay requires identity match, decoupled from fencing

## Problem found during the Phase 1.1 audit

`applyTaskCommand`'s idempotency fast path (look up `command_receipts` by
`(task_id, command_id)`, return the stored result if the fingerprint
matches) ran with NO authorization check at all. It correctly let the
ORIGINAL caller replay its own already-committed command even after
ownership/epoch moved on (which is required -- see invariant C and
requirement C of the Phase 1.1 audit). But it also let ANY caller who
merely knew `(task_id, command_id)` and the command payload retrieve that
receipt, regardless of whether they were ever a legitimate participant.
That is not a mutation hazard (replay never re-runs the transition), but
it is an authorization gap: "returning an old receipt must not ... expose
another principal's task."

## Decision

`command_receipts` now stores `owner_id` and `committed_epoch` at commit
time. On replay, the caller's `ownerId` must match the receipt's stored
`owner_id` -- checked BEFORE the fingerprint check, and BEFORE any
fencing (epoch/lease) check, which does not apply to a replay at all (see
0001). A mismatch raises `NotAuthorized`, distinct from `CommandConflict`
(which means "right caller, wrong payload") and from `ClaimLost`/
`EpochStale` (which are about CURRENT fencing, not replay authorization).

## The trusted-caller boundary (read this before assuming more than it says)

Phase 1 has no authentication system. `ownerId` is a string the caller
supplies; nothing cryptographically verifies that the caller "is" that
owner. This check therefore defends against:
- Accidental `command_id` collisions between two different workers
  (extremely cheap to cause: both pick the same `command_id` scheme).
- A bug where a worker reads back a receipt it should not assume is its
  own.

It does NOT defend against a caller that is willing to lie about its own
`ownerId` -- that caller can simply claim to be the original owner.
Closing that gap requires real authentication (verified worker identity,
e.g. mTLS client certs or signed tokens), which is out of scope for
Phase 1 and not simulated here. Treat this as "accident-proofing," not
"attacker-proofing," until a later phase adds real authentication.

## Alternatives rejected

- **No stored identity, authorize via current epoch/owner instead**:
  rejected -- this is exactly the bug requirement C warns against (it
  would make replay fail after ownership changes, when replay is supposed
  to succeed across ownership changes).
- **Make `command_id` itself globally unique (e.g. a UUID) and skip
  authorization entirely**: does not solve the actual problem -- a
  globally unique ID still does not prove that the REPLAYING caller is
  the one who originally used it; it only reduces accidental collisions
  between different tasks, not between different callers retrying the
  same task's command.
