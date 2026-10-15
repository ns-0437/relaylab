# 0011: Committed-checkpoint handoff protocol between two workers

Written BEFORE the Phase 2B implementation below it, per that milestone's
own instruction to define the protocol before coding.

## States

1. **Owned, no handoff in progress.** Task owned by A at epoch `E1`; a
   checkpoint may or may not yet be durably recorded
   (`tasks.checkpoint_digest`).
2. **Artifact prepared (LOCAL to A only).** A builds a self-contained Git
   bundle + manifest for its CURRENT, already-durably-recorded checkpoint
   (`createHandoffArtifact`, src/worker/artifact.ts). Written atomically
   (temp file -> validated -> renamed to an immutable, digest-addressed
   final path). Nothing in the database changes here -- this is a pure
   filesystem fact, and the artifact is independently verifiable
   (`git bundle verify` + a recomputed digest/size over its actual bytes)
   before anything downstream ever trusts it.
3. **Transfer request -> atomic ownership change.** The coordinator calls
   `TaskStore.transferOwnership` with the checkpoint digest the artifact
   in state 2 was built from. ONE atomic transaction: `tasks.owner_id`
   becomes B, `tasks.epoch` becomes `E1 + 1`. A's authority ends AT THIS
   COMMIT, not before and not after -- see "When A loses authority" below.
4. **Destination restoration.** B (now authoritative at `E1 + 1`) takes
   the artifact, validates its digest/size BEFORE consuming its bytes,
   restores it into its OWN independent, empty Git repository (no shared
   object store, no Git alternates, no clone-with-shared-objects path to
   A), verifies the restored commit/tree match the manifest EXACTLY, and
   reruns the SAME fixed verification command against the restored
   worktree -- B's own environment, never inherited from A's prior run.
5. **Restore acknowledgement.** B persists a `RestoreAcknowledged`
   command (src/domain/domain.ts), fenced under B's CURRENT epoch
   (`E1 + 1`) -- idempotent via the same `command_receipts` ledger every
   other command uses. Does NOT touch `checkpointDigest`/`baseCommit`/
   `resultCommit`/`resultTree` -- those keep A's original provenance.
   **Amended, Phase 2B.1**: no longer routed through the generic
   `applyTaskCommand` path -- see "Phase 2B.1 amendments" below.
6. **Failed/interrupted restoration.** B's restoration fails (corrupt
   artifact, re-verification fails, or B crashes before step 5). The
   task remains owned by B at `E1 + 1` -- authority already moved in
   state 3 and is NEVER rolled back. See "Recovery" below.

## When A loses authority

**At the moment `transferOwnership` commits (end of state 3), not
before and not after.** Two alternatives were rejected:

- **Wait for B to succeed before revoking A**: would mean two
  simultaneously-authoritative owners while B is still validating/
  restoring -- A could still publish a new checkpoint during that
  window, directly violating "at most one valid current owner."
- **Revoke A before the artifact exists/is validated**: this is exactly
  what the milestone's own requirement forbids ("do not revoke A based
  on an incomplete or unvalidated artifact"). `transferOwnership`'s own
  precondition -- `checkpointDigest` must equal `tasks.checkpoint_digest`,
  the ALREADY-durably-recorded value from a real, fenced
  `recordGitCheckpoint` call -- combined with requiring the artifact
  itself to pass its own internal validation (bundle verify, digest/size
  over actual written bytes) during state 2, BEFORE state 3 is ever
  requested, is what satisfies this requirement. The ownership change
  itself does not re-validate the artifact file (it has no way to -- the
  DB does not store artifact bytes); it is the CALLER'S responsibility
  (the coordinator) to have completed state 2 first. This is the
  documented trust boundary, not a gap: see "Remaining limitations" in
  docs/ARCHITECTURE.md "Phase 2B".

## Idempotency, conflict, and race policy

- **Repeating the same handoff request must not increment the epoch
  again.** `transferOwnership` reuses the EXISTING `command_receipts`
  ledger keyed by `(taskId, handoffId)` -- a replay (same `handoffId`,
  same `toOwnerId`/`checkpointDigest`) from the ORIGINAL requesting owner
  returns the ORIGINAL stored result (`toEpoch` unchanged), never a
  second increment.
- **Reusing a handoff's operation ID for different content is
  rejected.** The receipt's fingerprint covers `{toOwnerId,
  checkpointDigest}`; a reused `handoffId` with either field changed
  fails the fingerprint comparison and is rejected as `CommandConflict`.
- **Competing handoff requests cannot create two authoritative
  owners.** Two DIFFERENT, genuinely new `handoffId`s racing for the
  same `fromEpoch` serialize on the `tasks` row's own lock (`SELECT ...
  FOR UPDATE`) -- the SAME mechanism `claimTask` already relies on for
  takeover races. The first to commit wins; the second's expected
  `fromEpoch` no longer matches the row it reads after the first
  commits, so it is rejected with `EpochStale`.
- **Historical replay bypasses CURRENT ownership validation, by
  design** (same rule as `recordGitCheckpoint`, docs/decisions/0004) --
  but it does NOT bypass "does the retried payload match the original
  operation": the fingerprint comparison runs unconditionally, on every
  call, replay or not.

## Recovery when B fails after ownership transfer, before acknowledgement

B (same owner identity) simply retries restoration from the SAME
artifact at the SAME epoch -- every step is naturally idempotent:
`git init` on an existing directory, fetching the same bundle twice, and
checking out the same commit are all safe to repeat; the restore
acknowledgement's `commandId` is deterministic
(`restoreAckCommandId(taskId, handoffId)`), so a retry that reaches step
5 again returns the SAME receipt rather than a duplicate event. If B's
OWN lease instead expires before any retry happens, a FRESH claim is a
NORMAL `claimTask` takeover (epoch `E1 + 2`, by B itself or a
replacement), governed entirely by EXISTING ownership-fencing logic --
never a special case this protocol adds, and never a backward epoch
step. A second, independent worker picking up THAT takeover is
explicitly out of scope for this milestone (two-worker handoff only).

## Trusted-local-worker boundary, restated

Matching worker identities (`fromOwnerId`/`toOwnerId` strings) is NOT
cryptographic authentication -- this project still has no real
authentication of caller identity (docs/decisions/0004). A's own
isolated files may still be modified by a stale A process after
transfer; the guarantee this protocol provides is narrower and
specific: a stale A **cannot publish a new AUTHORITATIVE task result**
after transfer (ordinary epoch fencing, unchanged). This is not claimed
to be atomic fencing of every filesystem effect.

## Phase 2B.1 amendments

Three real gaps found on review of the Phase 2B implementation above,
closed in a bounded follow-up patch:

1. **Fresh-transfer lease validity.** State 3's atomic UPDATE never
   checked whether `fromOwnerId`'s lease was still valid -- an owner
   whose lease had already expired, but that nobody had yet taken over,
   could still "transfer" authority. Fixed: a FRESH (non-replay)
   transfer now also requires `lease_expires_at >= NOW(3)` (DB clock,
   enforced in the guarded UPDATE's own WHERE clause, with a diagnostic
   pre-check reading the SAME `NOW(3)`-sourced row). Does NOT apply to
   replay (see "Historical replay bypasses CURRENT ownership
   validation" above -- unchanged, now also explicitly covers lease).
2. **A receipt-lookup-before-row-lock race.** The original
   implementation checked `command_receipts` BEFORE acquiring the
   `tasks` row's `FOR UPDATE` lock. A genuinely concurrent DUPLICATE
   request (same `handoffId`) that lost the lock race would see "no
   receipt yet," wait, then -- holding a now-stale belief -- fail
   against the winner's already-incremented epoch with a misleading
   `EpochStale`, instead of correctly resolving as a replay. Fixed by
   acquiring the row lock FIRST, so the receipt lookup's InnoDB
   REPEATABLE READ snapshot is established AFTER the winner's commit.
3. **Acknowledgement bound to durable handoff identity.** State 5's
   `RestoreAcknowledged`, originally routed through the generic
   `applyTaskCommand` path, had NO validation against the actual handoff
   it claimed to acknowledge. `TaskStore.recordRestoreAcknowledgement`
   (new method) now validates, on a FRESH (non-replay) acknowledgement:
   the handoff exists for this task; it has not already been
   acknowledged under a DIFFERENT `commandId`
   (`task_handoffs.restore_ack_command_id`, new column); the caller's
   owner/epoch match the handoff's recorded destination (`to_owner_id`/
   `to_epoch`); the caller's `artifactDigest` matches the one authorized
   AT TRANSFER TIME (`task_handoffs.artifact_digest`, new column, bound
   when `transferOwnership` commits -- `transferOwnership` itself now
   takes `artifactDigest` as an argument); and the claimed restored
   commit/tree match the CHECKPOINT this handoff was for (joined from
   the EXISTING `task_checkpoints` table via `checkpoint_digest` --
   never a duplicated manifest copy, per the task's own instruction to
   reuse checkpoint metadata rather than re-store it). A REPLAY (same
   `commandId`, same payload) bypasses all of the above, per the
   general historical-replay rule.
4. **The crash test's own barrier mechanism was unsound** (not a
   protocol gap, a TEST-infrastructure gap): `src/testing/failpoint.ts`'s
   hang (`await new Promise(() => {})`) holds no active handle of its
   own; `restore-worker-main.ts`'s call path never opens a real MySQL
   connection before the `restore-after-checkout-before-ack` failpoint
   (unlike `worker-main.ts`, which claims the task -- a DB call -- first),
   so the process could and did exit NATURALLY (`exitCode: 0`) instead
   of being interrupted. Fixed with an explicit, permanent `setInterval`
   handle in `hitFailpoint` itself, a richer structured readiness
   message (pid/task/handoff/owner/epoch), and an independent
   `process.kill(pid, 0)` liveness probe in the test, both at the
   barrier and immediately before the real kill.

## Phase 2B.2 amendments -- closing the handoff milestone

Two further gaps found on review of the Phase 2B.1 patch itself:

5. **The generic `applyTaskCommand` route could bypass
   `recordRestoreAcknowledgement` entirely.** `applyCommand`'s domain
   case for `RestoreAcknowledged` (src/domain/domain.ts) is pure and
   succeeds unconditionally -- it has no way to see `task_handoffs` at
   all. Nothing in Phase 2B.1 stopped a caller from submitting
   `RestoreAcknowledged` through `TaskStore.applyTaskCommand` directly,
   with a VALID current owner/epoch/lease and completely ARBITRARY
   `artifactDigest`/`restoredCommit`/`restoredTree` -- this would emit a
   real `RestoreAcknowledged` success event with none of state 5's
   validation ever applied. CONFIRMED directly (a test submitting
   exactly this succeeded against the generic path before the fix).
   Closed at the public durable boundary: `applyTaskCommandImpl` now
   refuses `RestoreAcknowledged` outright, before any I/O, directing
   the caller to the specialized method -- never routed INTO it (that
   would mean either a nested transaction or sharing an open `conn`
   across two unrelated call paths, both worse than a flat refusal for
   a command this project never legitimately submits generically).
   Re-verified: the "already acknowledged via a different commandId"
   rejection (state 5's own duplicate-form-3 handling) was checked
   explicitly and confirmed to NEVER return success regardless of
   whether the new request's content happens to match the original --
   it is a flat rejection, not a content-equality check.
6. **The receipt-lookup-before-row-lock race (amendment 2, above) also
   affected `applyTaskCommandImpl` and `recordGitCheckpointImpl`** --
   the SAME code shape, the SAME exposure, confirmed by direct
   reproduction before fixing (two byte-identical concurrent
   `applyTaskCommand` calls produced a misleading `CommandConflict` for
   the loser instead of a replay). Fixed the same way: row lock first,
   receipt lookup second. This closes the LAST remaining instance of
   this pattern in `src/db/task-store.ts` -- all four
   `command_receipts`-backed mutation methods now use the same lock
   ordering.

Stopped here, per this patch's own instruction -- the handoff milestone
(Phase 2B/2B.1/2B.2) is considered finished; no further phase started
in this patch.
