import { randomUUID } from "node:crypto";
import { Context, Effect, Layer } from "effect";
import type mysql from "mysql2/promise";
import type { PoolConnection, RowDataPacket, ResultSetHeader } from "mysql2/promise";
import { applyCommand, type Command, type TaskState } from "../domain/domain.js";
import { DomainRejection } from "../domain/domain.js";
import {
  ClaimLost,
  CommandConflict,
  CommandRejected,
  EpochStale,
  NotAuthorized,
  PermanentStoreError,
  TaskNotFound,
  TransientStoreError,
  type StoreError,
} from "../domain/errors.js";
import { fingerprint } from "../domain/fingerprint.js";
import { isDuplicateKeyError, isRetryableDbError, withTransaction } from "./pool.js";
import { captureCurrentCarrier } from "../telemetry/propagation.js";
import { withRelaylabSpan } from "../telemetry/with-span.js";

export interface ClaimResult {
  readonly taskId: string;
  readonly ownerId: string;
  readonly epoch: number;
  readonly leaseExpiresAtMs: number;
}

export interface CommandReceipt {
  readonly taskId: string;
  readonly commandId: string;
  readonly result: unknown;
  readonly replay: boolean;
}

export class TaskStore extends Context.Tag("relaylab/TaskStore")<
  TaskStore,
  {
    /**
     * Attempts to claim or take over a task. Wins iff the row is unclaimed
     * or its lease has expired (invariant A/B). InnoDB row-level locking on
     * the single UPDATE statement is the actual mutual-exclusion mechanism:
     * two concurrent claims on the same row serialize at the engine level,
     * and the loser's UPDATE re-evaluates its WHERE clause against the
     * winner's already-committed row and affects zero rows.
     */
    readonly claimTask: (
      taskId: string,
      ownerId: string,
      leaseMs: number,
    ) => Effect.Effect<ClaimResult, ClaimLost | TransientStoreError | PermanentStoreError>;

    readonly renewLease: (
      taskId: string,
      ownerId: string,
      epoch: number,
      leaseMs: number,
    ) => Effect.Effect<ClaimResult, EpochStale | ClaimLost | TransientStoreError | PermanentStoreError>;

    readonly getTask: (taskId: string) => Effect.Effect<TaskState, TaskNotFound | TransientStoreError>;

    /**
     * Applies one command atomically: fencing check, idempotency check,
     * pure transition, receipt insert, state update, and outbox insert all
     * commit together or none do (invariant D).
     */
    readonly applyTaskCommand: (args: {
      taskId: string;
      ownerId: string;
      epoch: number;
      commandId: string;
      command: Command;
    }) => Effect.Effect<CommandReceipt, StoreError>;

    /**
     * Phase 2A: persists a Git checkpoint manifest, a command receipt
     * (the SAME idempotency ledger `applyTaskCommand` uses -- see
     * docs/decisions/0002-idempotency-keys.md), the task's state update,
     * and the resulting outbox event, ALL in one transaction (invariant
     * D, unchanged) -- reusing the EXISTING `Checkpoint { digest }`
     * domain command/event (src/domain/domain.ts) rather than adding a
     * new one to the protocol. See src/worker/checkpoint.ts for manifest
     * construction and docs/ARCHITECTURE.md "Phase 2A worker" for the
     * full design rationale.
     *
     * Phase 2A.2, section 3: `manifestJson` is RUNTIME-VALIDATED here
     * (shape, then digest recomputed and compared against
     * `manifestDigest`, then cross-checked against `taskId`/`attemptId`/
     * `epoch`) before anything is persisted -- see
     * `parseAndValidateGitCheckpointManifest` below. Every persisted
     * checkpoint column (base/result commit, result tree, manifest
     * version, verification fields, runtime fingerprint) is DERIVED FROM
     * THAT VALIDATED MANIFEST, never from a separately-passed argument
     * that could silently disagree with it -- this interface therefore
     * does NOT accept `baseCommit`/`resultCommit`/`resultTree`/
     * `verificationCommandJson`/`verificationOutcome`/
     * `runtimeFingerprintJson`/`manifestVersion` as separate arguments
     * (an earlier version of this interface did, which is exactly the
     * "accepting contradictory representations" gap this patch closes).
     */
    readonly recordGitCheckpoint: (args: {
      taskId: string;
      ownerId: string;
      epoch: number;
      commandId: string;
      attemptId: string;
      manifestDigest: string;
      manifestJson: string;
    }) => Effect.Effect<CommandReceipt, StoreError>;

    /**
     * Phase 2B: atomically transfers authoritative ownership of a task
     * from `fromOwnerId` (at `fromEpoch`) to `toOwnerId`, incrementing
     * epoch by exactly 1 -- modeled as its own method (like `claimTask`),
     * NOT routed through `applyTaskCommand`, because it mutates
     * `owner_id`/`epoch` themselves rather than business state under an
     * unchanged epoch. Requires `checkpointDigest` to equal the task's
     * CURRENT, already-durably-recorded `checkpoint_digest` -- a transfer
     * can only ever hand off an already-validated checkpoint, never an
     * arbitrary or incomplete one ("do not revoke A based on an
     * incomplete or unvalidated artifact"). Idempotent via the SAME
     * `command_receipts` ledger `recordGitCheckpoint` uses, keyed by
     * `(taskId, handoffId)`: a replay (same handoffId, same content) from
     * the ORIGINAL requesting owner returns the original result WITHOUT
     * incrementing epoch again; reusing `handoffId` with DIFFERENT
     * content is rejected as `CommandConflict`. Two different, genuinely
     * new `handoffId`s racing for the same `fromEpoch` serialize on the
     * `tasks` row's own lock (the SAME mechanism `claimTask` already
     * relies on) -- the loser's expected `fromEpoch` no longer matches
     * after the winner commits, so it is rejected with `EpochStale`,
     * never a second authoritative owner.
     *
     * Phase 2B.1: a FRESH (non-replay) transfer now ALSO requires
     * `fromOwnerId`'s lease to still be valid, checked against the
     * DATABASE's own clock (the same `NOW(3)`-sourced value every other
     * lease check in this file uses, never Node's `Date.now()`) and
     * enforced in the guarded UPDATE's own WHERE clause -- an expired
     * owner cannot transfer authority it no longer actually holds, even
     * if nobody else has claimed it yet. `artifactDigest` is the
     * artifact being authorized for this specific transfer -- bound
     * into `task_handoffs` here so `recordRestoreAcknowledgement` below
     * can later verify the destination acknowledges the SAME artifact
     * this transfer actually authorized, not a different one. `leaseMs`
     * is validated against the same bounds as every lease this method
     * grants (`assertValidLeaseMs`).
     */
    readonly transferOwnership: (args: {
      taskId: string;
      handoffId: string;
      fromOwnerId: string;
      fromEpoch: number;
      toOwnerId: string;
      checkpointDigest: string;
      artifactDigest: string;
      leaseMs: number;
    }) => Effect.Effect<TransferReceipt, StoreError>;

    /**
     * Phase 2B.1 (new): the DESTINATION worker's post-restore
     * acknowledgement, now bound to DURABLE handoff identity rather than
     * accepted as an unvalidated generic command. Mirrors
     * `recordGitCheckpointImpl`'s structure (own method, NOT reusing
     * `applyTaskCommandImpl` directly, so this handoff-specific
     * validation never puts the well-tested generic command path at
     * risk) -- see that method's own doc comment in the implementation
     * for the exact validation order and the three duplicate-behavior
     * cases (same commandId+payload replays; same commandId+different
     * payload conflicts; same handoff acknowledged via a DIFFERENT
     * commandId is rejected before ever reaching the domain layer).
     */
    readonly recordRestoreAcknowledgement: (args: {
      taskId: string;
      handoffId: string;
      ownerId: string;
      epoch: number;
      commandId: string;
      artifactDigest: string;
      restoredCommit: string;
      restoredTree: string;
    }) => Effect.Effect<CommandReceipt, StoreError>;
  }
>() {}

export interface TransferReceipt {
  readonly taskId: string;
  readonly handoffId: string;
  readonly fromOwnerId: string;
  readonly toOwnerId: string;
  readonly fromEpoch: number;
  readonly toEpoch: number;
  readonly checkpointDigest: string;
  readonly artifactDigest: string;
  readonly replay: boolean;
}

interface TaskRow extends RowDataPacket {
  task_id: string;
  owner_id: string | null;
  epoch: number;
  lease_expires_at: Date | null;
  status: "unclaimed" | "owned" | "completed";
  checkpoint_digest: string | null;
  candidate_id: string | null;
  verification_status: "pending" | "passed" | "failed" | null;
  verification_attempt: number;
  seq: number;
}

interface HandoffRow extends RowDataPacket {
  task_id: string;
  handoff_id: string;
  from_owner_id: string;
  to_owner_id: string;
  from_epoch: number;
  to_epoch: number;
  checkpoint_digest: string;
  artifact_digest: string;
  restore_ack_command_id: string | null;
}

function rowToState(row: TaskRow): TaskState {
  return {
    taskId: row.task_id,
    ownerId: row.owner_id,
    epoch: row.epoch,
    leaseExpiresAtMs: row.lease_expires_at ? row.lease_expires_at.getTime() : null,
    status: row.status,
    checkpointDigest: row.checkpoint_digest,
    candidateId: row.candidate_id,
    verificationStatus: row.verification_status,
    verificationAttempt: row.verification_attempt,
    seq: row.seq,
  };
}

function wrapDbError(err: unknown): TransientStoreError | PermanentStoreError {
  if (isRetryableDbError(err)) return new TransientStoreError("retryable database conflict", err);
  return new PermanentStoreError("database operation failed", err);
}

const COMMIT_OR_TREE_SHA_RE = /^[0-9a-f]{40}$/;

/**
 * Phase 2B.1: no lease-duration bound existed ANYWHERE in this codebase
 * before this patch -- `claimTask`/`renewLease`/the publisher leader
 * lock all accept any numeric `leaseMs` uncritically. This patch adds
 * bounds for `transferOwnership` specifically (the method this patch
 * touches); `claimTask`/`renewLease` are deliberately NOT retrofitted
 * here -- that would be an unrelated change to already-working,
 * already-tested code, out of scope for this bounded patch (see
 * CLAUDE.md "Phase 2B.1" for this stated explicitly).
 */
const MIN_LEASE_MS = 1_000;
const MAX_LEASE_MS = 24 * 60 * 60 * 1000;
function assertValidLeaseMs(callerLabel: string, leaseMs: number): void {
  if (!Number.isInteger(leaseMs) || leaseMs < MIN_LEASE_MS || leaseMs > MAX_LEASE_MS) {
    throw new CommandRejected(`${callerLabel}: leaseMs must be an integer between ${MIN_LEASE_MS} and ${MAX_LEASE_MS} (inclusive), got ${leaseMs}`);
  }
}

/**
 * The shape `recordGitCheckpointImpl` trusts after validation -- mirrors
 * src/worker/checkpoint.ts's `CheckpointManifest` field-for-field (kept
 * as an independent, DUCK-TYPED structural check here rather than a
 * type-only import of that interface, so this module's own runtime
 * validation does not depend on `worker/`'s types ever staying in sync
 * by construction -- see the validator's own doc comment below for why
 * that distinction matters at this boundary).
 */
interface ValidatedGitCheckpointManifest {
  manifestVersion: number;
  taskId: string;
  attemptId: string;
  epoch: number;
  baseCommit: string;
  resultCommit: string;
  resultTree: string;
  verification: {
    command: { executable: string; args: string[]; version: number };
    outcome: "passed" | "failed";
    exitCode: number | null;
    durationMs: number;
    containerImageId?: string | null;
  };
  runtimeFingerprint: {
    nodeVersion: string;
    execPath: string;
    platform: string;
    gitVersion: string;
  };
}

/**
 * Phase 2A.2, section 3: the ACTUAL runtime validation behind
 * `recordGitCheckpoint` -- this project's own standing rule ("validate at
 * system boundaries," CLAUDE.md) applied to the one boundary where an
 * in-memory manifest object (src/worker/checkpoint.ts) crosses into a
 * database write as a JSON string plus a claimed digest. That string and
 * digest are treated as UNTRUSTED here -- not already proven correct
 * merely because TypeScript's compile-time types said so at the caller
 * (the one production caller, `publishCheckpoint`, IS internally
 * consistent today, but this is the store's own, independently-verified
 * guarantee, not something it takes on faith from any particular
 * caller -- a test, or a future caller, can and does construct a
 * `manifestJson`/`manifestDigest` pair directly). Three checks, in order:
 *
 *   1. Structural shape: every field this table's columns and this
 *      store's own domain logic depend on is present and the right type.
 *   2. Digest integrity: `manifestDigest` is RECOMPUTED from the parsed
 *      manifest content (the SAME `fingerprint()` function
 *      src/worker/checkpoint.ts's `computeManifestDigest` uses -- see
 *      src/domain/fingerprint.ts) and compared against the supplied
 *      value -- a caller cannot persist a digest that does not actually
 *      correspond to the manifest JSON it is paired with.
 *   3. Request/manifest agreement: the manifest's OWN `taskId`/
 *      `attemptId`/`epoch` fields must equal the request's -- a caller
 *      cannot (even by accident) persist a checkpoint for one task's
 *      request carrying a different task's manifest content, or an
 *      attempt/epoch that disagrees with what fencing is about to check
 *      against the authoritative `tasks` row.
 *
 * Also rejects a manifest whose OWN `verification.outcome` is not
 * `"passed"` -- mirroring src/worker/run-task.ts's invariant ("a failing
 * verification never publishes a checkpoint") at the durable persistence
 * boundary itself, rather than trusting every caller to have already
 * enforced it upstream.
 *
 * Every persisted column in `recordGitCheckpointImpl` below
 * (`result_commit`, `result_tree`, `base_commit`, `manifest_version`,
 * `verification_*`, `runtime_fingerprint`) is DERIVED FROM THE RETURNED,
 * VALIDATED MANIFEST -- never from a separately-passed, independently-
 * contradictable argument. There is exactly one source of truth for what
 * gets written.
 */
/**
 * Phase 2B, Part A: the shape+digest half of validation, split out so it
 * can run on BOTH the replay path and the new-write path (see
 * `recordGitCheckpointImpl` below). A full-code review of this patch's
 * OWN prior session found a real, demonstrated gap: the REPLAY branch
 * matched on `fingerprint({type:"Checkpoint", digest: manifestDigest})`
 * alone -- a hash of the CLAIMED digest value, never of the manifestJson
 * actually supplied alongside it -- so a retry supplying the SAME
 * (already-committed) `manifestDigest` together with a DIFFERENT,
 * altered `manifestJson` was silently accepted as an identical retry,
 * returning the original stored result, without ever checking that the
 * freshly-supplied manifestJson actually corresponds to that digest.
 * This function closes that gap: it is now called unconditionally,
 * before EITHER branch returns, so a mismatched (digest, content) pair
 * is rejected regardless of whether this turns out to be a replay or a
 * new write. This is "manifest-internal consistency" -- it does NOT
 * check the manifest's own taskId/attemptId/epoch against the request
 * (see `assertManifestIdentityMatchesRequest` below for that, which
 * stays new-write-only, since a REPLAY must still succeed even after
 * the task's epoch has since moved on -- docs/decisions/0004).
 */
function parseAndValidateGitCheckpointManifestShapeAndDigest(manifestJson: string, manifestDigest: string): ValidatedGitCheckpointManifest {
  let raw: unknown;
  try {
    raw = JSON.parse(manifestJson);
  } catch (err) {
    throw new CommandRejected(`recordGitCheckpoint: manifestJson is not valid JSON: ${(err as Error).message}`);
  }
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new CommandRejected("recordGitCheckpoint: manifest must be a JSON object");
  }
  const m = raw as Record<string, unknown>;
  const fail = (field: string, detail: string): never => {
    throw new CommandRejected(`recordGitCheckpoint: manifest field "${field}" ${detail}`);
  };

  if (typeof m.manifestVersion !== "number") fail("manifestVersion", "must be a number");
  if (typeof m.taskId !== "string" || m.taskId.length === 0) fail("taskId", "must be a non-empty string");
  if (typeof m.attemptId !== "string" || m.attemptId.length === 0) fail("attemptId", "must be a non-empty string");
  if (typeof m.epoch !== "number") fail("epoch", "must be a number");
  if (typeof m.baseCommit !== "string" || !COMMIT_OR_TREE_SHA_RE.test(m.baseCommit)) fail("baseCommit", "must be a 40-character commit SHA");
  if (typeof m.resultCommit !== "string" || !COMMIT_OR_TREE_SHA_RE.test(m.resultCommit)) fail("resultCommit", "must be a 40-character commit SHA");
  if (typeof m.resultTree !== "string" || !COMMIT_OR_TREE_SHA_RE.test(m.resultTree)) fail("resultTree", "must be a 40-character tree SHA");

  const verificationRaw = m.verification as Record<string, unknown> | undefined;
  if (typeof verificationRaw !== "object" || verificationRaw === null) fail("verification", "must be an object");
  const verification = verificationRaw!;
  const commandRaw = verification.command as Record<string, unknown> | undefined;
  if (typeof commandRaw !== "object" || commandRaw === null) fail("verification.command", "must be an object");
  const command = commandRaw!;
  if (typeof command.executable !== "string") fail("verification.command.executable", "must be a string");
  if (!Array.isArray(command.args) || !command.args.every((a) => typeof a === "string")) fail("verification.command.args", "must be a string array");
  if (typeof command.version !== "number") fail("verification.command.version", "must be a number");
  if (verification.outcome !== "passed" && verification.outcome !== "failed") fail("verification.outcome", 'must be "passed" or "failed"');
  if (verification.exitCode !== null && typeof verification.exitCode !== "number") fail("verification.exitCode", "must be a number or null");
  if (typeof verification.durationMs !== "number") fail("verification.durationMs", "must be a number");
  if (verification.outcome !== "passed") {
    fail("verification.outcome", 'must be "passed" -- a checkpoint may never be recorded for a non-passing verification result');
  }
  // Phase 2C: OPTIONAL/nullable by design -- the host-process adapter
  // (still used by tests that don't need Docker) has no image at all,
  // and a manifest built before this field existed (MANIFEST_VERSION 1)
  // simply omits it. Backward compatible: never required, never
  // defaulted to a value that would misleadingly imply a real image was
  // used when none was.
  if (verification.containerImageId !== undefined && verification.containerImageId !== null && typeof verification.containerImageId !== "string") {
    fail("verification.containerImageId", "must be a string, null, or omitted");
  }

  const runtimeFingerprint = m.runtimeFingerprint as Record<string, unknown> | undefined;
  if (typeof runtimeFingerprint !== "object" || runtimeFingerprint === null) fail("runtimeFingerprint", "must be an object");
  for (const f of ["nodeVersion", "execPath", "platform", "gitVersion"] as const) {
    if (typeof (runtimeFingerprint as Record<string, unknown>)[f] !== "string") fail(`runtimeFingerprint.${f}`, "must be a string");
  }

  const manifest = raw as ValidatedGitCheckpointManifest;

  const recomputedDigest = fingerprint(manifest);
  if (recomputedDigest !== manifestDigest) {
    throw new CommandRejected(
      `recordGitCheckpoint: supplied manifestDigest ("${manifestDigest}") does not match the digest recomputed from manifestJson's own content ("${recomputedDigest}") -- refusing to accept a digest that does not correspond to the manifest it is paired with (this check runs on BOTH a replay and a new write)`,
    );
  }
  return manifest;
}

/**
 * Phase 2B, Part A: request/manifest AGREEMENT -- the manifest's own
 * taskId/attemptId/epoch must equal the request's. Deliberately called
 * ONLY on the new-write path (never on a replay): a replay from the
 * original committing owner must still succeed even after the task's
 * epoch has since moved on (docs/decisions/0004-receipt-authorization.md),
 * so cross-checking against the CALLER's current, possibly-stale epoch
 * would otherwise break that guarantee. This is "current task ownership"
 * territory, distinct from the shape/digest check above, which is pure
 * manifest-internal consistency and runs unconditionally.
 */
function assertManifestIdentityMatchesRequest(manifest: ValidatedGitCheckpointManifest, expected: { taskId: string; attemptId: string; epoch: number }): void {
  if (manifest.taskId !== expected.taskId) {
    throw new CommandRejected(`recordGitCheckpoint: manifest.taskId ("${manifest.taskId}") does not match the request's taskId ("${expected.taskId}")`);
  }
  if (manifest.attemptId !== expected.attemptId) {
    throw new CommandRejected(`recordGitCheckpoint: manifest.attemptId ("${manifest.attemptId}") does not match the request's attemptId ("${expected.attemptId}")`);
  }
  if (manifest.epoch !== expected.epoch) {
    throw new CommandRejected(`recordGitCheckpoint: manifest.epoch (${manifest.epoch}) does not match the request's epoch (${expected.epoch})`);
  }
}

/**
 * Phase 2C, Part 5: every lease-expiry check in this file used to read
 * `db_now` bundled INTO the same `SELECT *, NOW(3) AS db_now ... FOR
 * UPDATE` statement that acquires the row lock. Confirmed by direct
 * empirical probe against this project's own MySQL instance (held a
 * real row lock open on a separate connection for ~1.5s while a second
 * connection's blocked `SELECT ..., NOW(3) ... FOR UPDATE` waited on
 * it): MySQL evaluates `NOW(3)` at/near STATEMENT DISPATCH time (within
 * single-digit ms of dispatch), not after the lock wait resolves -- the
 * blocked statement's own `db_now` was ~1.5s stale by the time it
 * finally returned. A lease that expires WHILE a write is blocked
 * waiting for the row lock was therefore evaluated against a timestamp
 * from BEFORE it expired, incorrectly treating it as still valid.
 * Fixed by taking a SEPARATE, trivial, non-blocking `SELECT NOW(3)`
 * immediately AFTER the row lock is confirmed held (the blocking
 * SELECT ... FOR UPDATE above it has already returned by the time this
 * runs), rather than folding it into the same statement that blocks.
 * Every lease-expiry check in this file must read its `db_now` from
 * this helper, called AFTER the relevant row lock(s), never from a
 * timestamp bundled into the lock-acquiring query itself.
 */
async function freshDbNow(conn: PoolConnection): Promise<Date> {
  const [rows] = await conn.query<(RowDataPacket & { db_now: Date })[]>(`SELECT NOW(3) AS db_now`);
  // A bare `SELECT NOW(3)` with no FROM/WHERE always returns exactly one row.
  return rows[0]!.db_now;
}

export function makeTaskStoreLive(pool: mysql.Pool) {
  async function ensureRow(conn: PoolConnection, taskId: string): Promise<void> {
    await conn.query(
      `INSERT IGNORE INTO tasks (task_id, status, epoch, seq) VALUES (?, 'unclaimed', 0, 0)`,
      [taskId],
    );
  }

  /**
   * Claim/retry semantics (section 2 audit, "establish whether
   * claim/renew/transfer operations are covered by command idempotency"):
   *
   * claimTask is idempotent FOR THE CURRENT RIGHTFUL OWNER: if `ownerId`
   * already holds the task with a still-valid lease, retrying the exact
   * same claim call (e.g. because the first response was lost to a
   * network blip) succeeds again WITHOUT incrementing epoch -- it is
   * treated as the same claim continuing, not a takeover. This was a real
   * gap found during the Phase 1.1 audit: the original WHERE clause only
   * matched `status = 'unclaimed' OR lease expired`, so a rightful owner
   * retrying its own still-valid claim got a spurious ClaimLost instead of
   * its claim succeeding again. See docs/decisions/0007-claim-idempotency.md
   * and tests/integration/claim-idempotency.test.ts.
   *
   * claimTask is NOT idempotent across a genuine takeover: a DIFFERENT
   * ownerId claiming after the rightful owner's lease expires DOES
   * increment epoch (that is the whole point of fencing -- see invariant B).
   *
   * renewLease is "safe to retry but not strictly idempotent": retrying a
   * successful renewLease call just extends the lease further each time,
   * which is benign (consistent with "renew" semantics) and does not need
   * a receipt-style idempotency ledger.
   */
  async function claimTaskImpl(taskId: string, ownerId: string, leaseMs: number): Promise<ClaimResult> {
    return withRelaylabSpan(
      "relaylab.ownership.claim",
      { taskId },
      () => claimTaskTransaction(),
      (result) => ({ outcome: "acquired", epoch: result.epoch }),
    );

    async function claimTaskTransaction(): Promise<ClaimResult> {
      return withTransaction(pool, async (conn) => {
      await ensureRow(conn, taskId);
      // Column order in SET matters: MySQL evaluates a multi-column
      // UPDATE's SET list left to right, and a later expression that
      // references a column assigned EARLIER in the same list sees the
      // NEW value, not the original row (verified empirically against
      // this project's own MySQL instance -- see
      // docs/decisions/0008-set-clause-evaluation-order.md). `epoch`'s
      // CASE is listed FIRST, before `owner_id`/`lease_expires_at` are
      // reassigned, so it genuinely reads the PRE-UPDATE row -- this was
      // fixed during the Phase 1.2 audit; it previously happened to
      // produce the right answer anyway only because the WHERE clause's
      // own gating ruled out the one case where the corruption would
      // have mattered, which made it correct by accident, not by
      // construction.
      const [result] = await conn.query<ResultSetHeader>(
        `UPDATE tasks
           SET epoch = CASE WHEN owner_id = ? AND lease_expires_at >= NOW(3) THEN epoch ELSE epoch + 1 END,
               owner_id = ?,
               lease_expires_at = DATE_ADD(NOW(3), INTERVAL ? MICROSECOND),
               status = 'owned'
         WHERE task_id = ?
           AND status != 'completed'
           AND (status = 'unclaimed' OR lease_expires_at IS NULL OR lease_expires_at < NOW(3) OR owner_id = ?)`,
        [ownerId, ownerId, leaseMs * 1000, taskId, ownerId],
      );
      if (result.affectedRows !== 1) {
        throw new ClaimLost(taskId);
      }
      const [rows] = await conn.query<TaskRow[]>(`SELECT * FROM tasks WHERE task_id = ? FOR UPDATE`, [taskId]);
      const row = rows[0];
      if (!row || row.lease_expires_at === null) throw new PermanentStoreError("claim committed but row missing lease", null);
        return { taskId, ownerId, epoch: row.epoch, leaseExpiresAtMs: row.lease_expires_at.getTime() };
      });
    }
  }

  async function renewLeaseImpl(
    taskId: string,
    ownerId: string,
    epoch: number,
    leaseMs: number,
  ): Promise<ClaimResult> {
    return withRelaylabSpan(
      "relaylab.ownership.renew",
      { taskId, epoch },
      () => renewLeaseTransaction(),
      () => ({ outcome: "renewed" }),
    );

    async function renewLeaseTransaction(): Promise<ClaimResult> {
      return withTransaction(pool, async (conn) => {
      const [current] = await conn.query<TaskRow[]>(`SELECT * FROM tasks WHERE task_id = ? FOR UPDATE`, [taskId]);
      const row = current[0];
      if (!row) throw new ClaimLost(taskId);
      if (row.epoch !== epoch) throw new EpochStale(taskId, epoch, row.epoch);
      if (row.owner_id !== ownerId) throw new ClaimLost(taskId);
      // The actual lease-expiry gate is the UPDATE's WHERE clause below
      // (`lease_expires_at >= NOW(3)`), which compares against the
      // database's own clock. Pre-checking here against Node's Date.now()
      // would reintroduce exactly the host/container clock-skew hazard
      // invariant B is meant to rule out, so this method deliberately does
      // not duplicate that check in JS.

      const [result] = await conn.query<ResultSetHeader>(
        `UPDATE tasks
           SET lease_expires_at = DATE_ADD(NOW(3), INTERVAL ? MICROSECOND)
         WHERE task_id = ? AND owner_id = ? AND epoch = ? AND lease_expires_at >= NOW(3)`,
        [leaseMs * 1000, taskId, ownerId, epoch],
      );
      if (result.affectedRows !== 1) throw new ClaimLost(taskId);
        return { taskId, ownerId, epoch, leaseExpiresAtMs: Date.now() + leaseMs };
      });
    }
  }

  /**
   * Phase 2B: a deliberate, coordinated handoff -- NOT a timeout-based
   * takeover (`claimTask` already covers that case). See the `TaskStore`
   * interface doc comment above for the full idempotency/conflict/race
   * story; this implementation mirrors `recordGitCheckpointImpl`'s
   * replay/idempotency structure exactly (same `command_receipts`
   * ledger, same NotAuthorized/CommandConflict semantics on replay),
   * with the actual mutation being an owner/epoch change instead of a
   * business-state update.
   *
   * Phase 2B.1, two fixes found on review of this exact function:
   *
   * 1. **Lease validity was never checked.** The fresh-transfer branch
   *    checked owner/epoch/status/checkpoint-digest but NOT whether
   *    `fromOwnerId`'s lease was still valid -- an owner whose lease had
   *    already expired (but that nobody had yet taken over) could still
   *    "transfer" authority it no longer actually held. Fixed the same
   *    way `renewLeaseImpl`/`claimTaskImpl` already enforce lease
   *    validity: the REAL enforcement lives in the guarded UPDATE's own
   *    `AND lease_expires_at >= NOW(3)` (the database's own clock, never
   *    `Date.now()`), with a diagnostic pre-check using THIS SAME
   *    transaction's own `NOW(3)`-sourced `db_now` reading (not a second,
   *    independent clock read) so the thrown error (`ClaimLost`) is
   *    accurate rather than a generic "0 rows affected."
   * 2. **A receipt-lookup-before-row-lock race.** The ORIGINAL version
   *    checked `command_receipts` BEFORE acquiring `tasks`' row lock. A
   *    concurrent, GENUINELY DUPLICATE request (same handoffId) that
   *    lost the race to acquire that lock would see "no receipt yet" in
   *    its own pre-lock read, then block on the lock, then -- once the
   *    winner commits and releases it -- resume holding a now-STALE
   *    belief that no receipt exists, check the row's epoch (now
   *    incremented by the winner), and fail with a MISLEADING
   *    `EpochStale` instead of correctly being recognized as a replay.
   *    Fixed by acquiring the row lock FIRST: under InnoDB REPEATABLE
   *    READ, a transaction's consistent-read snapshot is established at
   *    its FIRST read, including a locking read -- so by moving the
   *    `tasks` lock acquisition (which must wait for the winner's commit
   *    to proceed) BEFORE the `command_receipts` lookup, the receipt
   *    lookup's snapshot is correctly established AFTER the winner's
   *    commit, and it now reliably sees the winner's just-written
   *    receipt row.
   */
  async function transferOwnershipImpl(args: {
    taskId: string;
    handoffId: string;
    fromOwnerId: string;
    fromEpoch: number;
    toOwnerId: string;
    checkpointDigest: string;
    artifactDigest: string;
    leaseMs: number;
  }): Promise<TransferReceipt> {
    const { taskId, handoffId, fromOwnerId, fromEpoch, toOwnerId, checkpointDigest, artifactDigest, leaseMs } = args;
    assertValidLeaseMs("transferOwnership", leaseMs);
    // Fingerprinted over every field that changing WOULD change the
    // actual operation -- reusing this handoffId with a different
    // toOwnerId, checkpointDigest, or artifactDigest is a conflict, not
    // an identical retry.
    const fp = fingerprint({ type: "TransferOwnership", toOwnerId, checkpointDigest, artifactDigest });

    return withRelaylabSpan(
      "relaylab.handoff.transfer",
      { taskId, commandId: handoffId, commandType: "TransferOwnership", epoch: fromEpoch },
      () => transaction(),
      (result) => ({ outcome: result.replay ? "replayed" : "committed", toEpoch: result.toEpoch }),
    );

    async function transaction(): Promise<TransferReceipt> {
      return withTransaction(pool, async (conn) => {
        // Row lock FIRST (see fix #2 above) -- this also means the row
        // is read before the receipt lookup. `db_now` is read via
        // `freshDbNow` AFTER this lock is held (Phase 2C, Part 5 --
        // see that helper's comment for why it must not be bundled
        // into this same blocking statement).
        const [rows] = await conn.query<TaskRow[]>(`SELECT * FROM tasks WHERE task_id = ? FOR UPDATE`, [taskId]);
        const row = rows[0];
        if (!row) throw new TaskNotFound(taskId);
        const dbNow = await freshDbNow(conn);

        const [existingRows] = await conn.query<(RowDataPacket & { fingerprint: string; result: unknown; owner_id: string })[]>(
          `SELECT fingerprint, result, owner_id FROM command_receipts WHERE task_id = ? AND command_id = ?`,
          [taskId, handoffId],
        );
        const existing = existingRows[0];
        if (existing) {
          // A REPLAY of an already-committed transfer: per this
          // project's receipt-authorization rule, this succeeds for the
          // ORIGINAL requesting owner (A) even though A is, BY
          // DEFINITION, no longer the current owner after a successful
          // transfer, AND even if A's original lease has since expired
          // -- this is exactly "historical replay may bypass CURRENT
          // ownership/lease validation," deliberately, unchanged by the
          // lease-validity fix above (which applies ONLY to the fresh-
          // transfer branch below). It must NOT, however, increment
          // epoch again (the stored result already carries the original
          // toEpoch).
          if (existing.owner_id !== fromOwnerId) throw new NotAuthorized(taskId, handoffId);
          if (existing.fingerprint !== fp) throw new CommandConflict(taskId, handoffId);
          return { ...(existing.result as TransferReceipt), replay: true };
        }

        // Fresh (non-replay) transfer: full fencing, INCLUDING lease
        // validity now, against `dbNow` read AFTER the row lock above
        // (Phase 2C, Part 5) -- never a second, independent (Node-side)
        // clock read.
        if (row.epoch !== fromEpoch) throw new EpochStale(taskId, fromEpoch, row.epoch);
        if (row.owner_id !== fromOwnerId) throw new ClaimLost(taskId);
        if (row.status !== "owned") {
          throw new CommandRejected(`transferOwnership: task ${taskId} is not in "owned" status (got "${row.status}") -- refusing to transfer`);
        }
        if (!row.lease_expires_at || row.lease_expires_at.getTime() < dbNow.getTime()) {
          // The new check: an owner whose lease has already expired
          // cannot transfer authority it no longer holds, even if
          // nobody else has taken over yet.
          throw new ClaimLost(taskId);
        }
        if (row.checkpoint_digest !== checkpointDigest) {
          // The actual "do not revoke A based on an incomplete or
          // unvalidated artifact" guard: a transfer can only hand off
          // the checkpoint digest ALREADY durably recorded as this
          // task's current one (via `recordGitCheckpoint`'s own
          // fencing/validation) -- never an arbitrary or stale value.
          throw new CommandRejected(
            `transferOwnership: supplied checkpointDigest ("${checkpointDigest}") does not match task ${taskId}'s current durably-recorded checkpoint_digest ("${row.checkpoint_digest ?? "null"}") -- refusing to transfer authority over an unvalidated or stale checkpoint`,
          );
        }

        const toEpoch = fromEpoch + 1;
        const [result] = await conn.query<ResultSetHeader>(
          `UPDATE tasks
             SET owner_id = ?, epoch = ?, lease_expires_at = DATE_ADD(NOW(3), INTERVAL ? MICROSECOND), status = 'owned'
           WHERE task_id = ? AND epoch = ? AND owner_id = ? AND checkpoint_digest = ? AND lease_expires_at >= NOW(3)`,
          [toOwnerId, toEpoch, leaseMs * 1000, taskId, fromEpoch, fromOwnerId, checkpointDigest],
        );
        if (result.affectedRows !== 1) {
          // Should be unreachable: we hold the row lock continuously
          // from the first SELECT above, so nothing could have changed
          // the row out from under these exact conditions. Never
          // silently assumed, kept as the same defensive guard the
          // pre-fix version had (now genuinely dead code, not a live
          // race, now that the lock is acquired before every check).
          throw new ClaimLost(taskId);
        }

        const receipt: TransferReceipt = { taskId, handoffId, fromOwnerId, toOwnerId, fromEpoch, toEpoch, checkpointDigest, artifactDigest, replay: false };

        try {
          await conn.query(
            `INSERT INTO command_receipts (task_id, command_id, owner_id, committed_epoch, fingerprint, result)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [taskId, handoffId, fromOwnerId, fromEpoch, fp, JSON.stringify(receipt)],
          );
        } catch (err) {
          if (isDuplicateKeyError(err)) throw new CommandConflict(taskId, handoffId);
          throw err;
        }

        await conn.query(
          `INSERT INTO task_handoffs (task_id, handoff_id, from_owner_id, to_owner_id, from_epoch, to_epoch, checkpoint_digest, artifact_digest)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          [taskId, handoffId, fromOwnerId, toOwnerId, fromEpoch, toEpoch, checkpointDigest, artifactDigest],
        );

        return receipt;
      });
    }
  }

  /**
   * Phase 2B.1 (new): the DESTINATION worker's post-restore
   * acknowledgement, bound to DURABLE handoff identity -- the real gap
   * Phase 2B left open (see `TaskStore` interface doc comment above).
   * Report on what Phase 2B's `applyTaskCommand` path ALREADY provided
   * before this patch, verified by reading it (not assumed): it already
   * fences on CURRENT epoch/owner/lease (the generic checks every
   * command gets) and already gives `RestoreAcknowledged` the same
   * command-receipt idempotency ledger every other command has (same
   * commandId + same payload = replay; same commandId + different
   * payload = `CommandConflict`). What it did NOT provide, and what
   * this function adds: (1) any check that the handoff being
   * acknowledged actually exists, belongs to THIS task, or was actually
   * authorized for THIS caller/epoch/artifact; (2) any check that the
   * claimed restored commit/tree actually match the checkpoint the
   * handoff was for; (3) protection against the SAME handoff being
   * acknowledged a SECOND time under a DIFFERENT, never-before-seen
   * commandId (the generic receipt ledger is keyed by commandId alone,
   * so it cannot catch this on its own).
   *
   * Validation order (mirrors the task's own listed order): existing-
   * receipt replay/conflict first (same as every other command, and
   * REQUIRED to stay first so historical replay keeps succeeding after
   * ownership changes -- docs/decisions/0004); then handoff-identity
   * checks (exists for this task; not already acknowledged under a
   * different commandId; destination owner/epoch match the recorded
   * handoff; artifact digest matches; restored commit/tree match the
   * CHECKPOINT this handoff was for, looked up from `task_checkpoints`
   * by `checkpoint_digest` -- reusing that table's own durable
   * `result_commit`/`result_tree` rather than duplicating a manifest
   * copy here); then the SAME generic current-epoch/owner/lease fencing
   * `applyTaskCommandImpl` already does, read from the SAME row/db_now
   * already fetched (no second clock read). Receipt insert, the
   * `task_handoffs.restore_ack_command_id` claim, the `tasks` row
   * update, and the outbox event insert are all in the SAME transaction
   * (atomic, per the task's own requirement).
   *
   * Ownership-transfer-itself event decision (reported, not invented):
   * `transferOwnership` does NOT emit an outbox event, and this patch
   * does not add one -- this matches the EXISTING, established design:
   * `claimTask`/`renewLease` (ownership/epoch mechanics, like
   * `transferOwnership`) have NEVER gone through the Command/
   * DomainEvent/outbox protocol either; only BUSINESS-state commands
   * do. Adding one now would be inventing a parallel event system for
   * ownership specifically, which the task explicitly says not to do.
   */
  async function recordRestoreAcknowledgementImpl(args: {
    taskId: string;
    handoffId: string;
    ownerId: string;
    epoch: number;
    commandId: string;
    artifactDigest: string;
    restoredCommit: string;
    restoredTree: string;
  }): Promise<CommandReceipt> {
    const { taskId, handoffId, ownerId, epoch, commandId, artifactDigest, restoredCommit, restoredTree } = args;
    const command: Command = { type: "RestoreAcknowledged", handoffId, artifactDigest, restoredCommit, restoredTree };
    const fp = fingerprint(command);

    return withRelaylabSpan(
      "relaylab.handoff.acknowledge",
      { taskId, commandId, commandType: "RestoreAcknowledged", epoch },
      () => transaction(),
      (result) => ({ outcome: result.replay ? "replayed" : "committed" }),
    );

    async function transaction(): Promise<CommandReceipt> {
      return withTransaction(pool, async (conn) => {
        // Lock BOTH rows this operation cares about FIRST (same race-
        // avoidance reasoning as transferOwnershipImpl: the receipt
        // lookup's consistent-read snapshot must be established AFTER
        // waiting for any concurrent committer's lock, not before).
        // `db_now` is read via `freshDbNow` AFTER this lock is held
        // (Phase 2C, Part 5).
        const [taskRows] = await conn.query<TaskRow[]>(`SELECT * FROM tasks WHERE task_id = ? FOR UPDATE`, [taskId]);
        const taskRow = taskRows[0];
        if (!taskRow) throw new TaskNotFound(taskId);
        const dbNow = await freshDbNow(conn);

        const [handoffRows] = await conn.query<HandoffRow[]>(`SELECT * FROM task_handoffs WHERE task_id = ? AND handoff_id = ? FOR UPDATE`, [taskId, handoffId]);
        const handoffRow = handoffRows[0];

        const [existingReceiptRows] = await conn.query<(RowDataPacket & { fingerprint: string; result: unknown; owner_id: string })[]>(
          `SELECT fingerprint, result, owner_id FROM command_receipts WHERE task_id = ? AND command_id = ?`,
          [taskId, commandId],
        );
        const existing = existingReceiptRows[0];
        if (existing) {
          // Same commandId: replay (same payload) or conflict (changed
          // payload) -- the SAME rule every other command follows,
          // deliberately bypassing current ownership/lease validation
          // for the replay case (docs/decisions/0004).
          if (existing.owner_id !== ownerId) throw new NotAuthorized(taskId, commandId);
          if (existing.fingerprint !== fp) throw new CommandConflict(taskId, commandId);
          return { taskId, commandId, result: existing.result, replay: true };
        }

        // Not a replay of THIS commandId -- validate against DURABLE
        // handoff identity before anything else.
        if (!handoffRow) {
          throw new CommandRejected(`RestoreAcknowledged: unknown handoff "${handoffId}" for task ${taskId}`);
        }
        if (handoffRow.restore_ack_command_id !== null && handoffRow.restore_ack_command_id !== commandId) {
          // "Same handoff acknowledged through a different command ID"
          // -- must NOT create a second logical completion event, even
          // though THIS commandId has never been seen before (so the
          // generic receipt ledger above found nothing).
          throw new CommandRejected(
            `RestoreAcknowledged: handoff "${handoffId}" for task ${taskId} was already acknowledged via a different commandId ("${handoffRow.restore_ack_command_id}") -- refusing to create a second completion event`,
          );
        }
        if (handoffRow.to_owner_id !== ownerId) {
          throw new CommandRejected(
            `RestoreAcknowledged: handoff "${handoffId}"'s recorded destination owner ("${handoffRow.to_owner_id}") does not match the caller ("${ownerId}")`,
          );
        }
        if (handoffRow.to_epoch !== epoch) {
          throw new CommandRejected(
            `RestoreAcknowledged: handoff "${handoffId}"'s recorded destination epoch (${handoffRow.to_epoch}) does not match the caller's epoch (${epoch})`,
          );
        }
        if (handoffRow.artifact_digest !== artifactDigest) {
          throw new CommandRejected(
            `RestoreAcknowledged: supplied artifactDigest ("${artifactDigest}") does not match the digest authorized for handoff "${handoffId}" ("${handoffRow.artifact_digest}")`,
          );
        }

        // Reuse checkpoint metadata (never duplicate a manifest copy
        // here): the checkpoint this handoff was FOR already durably
        // recorded its own result_commit/result_tree.
        const [checkpointRows] = await conn.query<(RowDataPacket & { result_commit: string; result_tree: string })[]>(
          `SELECT result_commit, result_tree FROM task_checkpoints WHERE task_id = ? AND manifest_digest = ?`,
          [taskId, handoffRow.checkpoint_digest],
        );
        const checkpointRow = checkpointRows[0];
        if (!checkpointRow) {
          throw new CommandRejected(`RestoreAcknowledged: no durable checkpoint found for task ${taskId} matching handoff "${handoffId}"'s checkpoint digest`);
        }
        if (checkpointRow.result_commit !== restoredCommit || checkpointRow.result_tree !== restoredTree) {
          throw new CommandRejected(
            `RestoreAcknowledged: restored commit/tree ("${restoredCommit}"/"${restoredTree}") do not match the checkpoint handoff "${handoffId}" was for ("${checkpointRow.result_commit}"/"${checkpointRow.result_tree}")`,
          );
        }

        // THE SAME generic current-epoch/owner/lease fencing every
        // other command gets -- "the destination still has authority
        // under the existing lease rules" -- against `dbNow` read
        // AFTER the row lock above (Phase 2C, Part 5), no second
        // (Node-side) clock read.
        if (taskRow.epoch !== epoch) throw new EpochStale(taskId, epoch, taskRow.epoch);
        if (taskRow.owner_id !== ownerId) throw new ClaimLost(taskId);
        if (taskRow.status === "owned" && (!taskRow.lease_expires_at || taskRow.lease_expires_at.getTime() < dbNow.getTime())) {
          throw new ClaimLost(taskId);
        }

        const state = rowToState(taskRow);
        let transition;
        try {
          transition = applyCommand(state, command);
        } catch (err) {
          if (err instanceof DomainRejection) throw new CommandRejected(err.message);
          throw err;
        }
        const nextSeq = transition.state.seq;

        try {
          await conn.query(
            `INSERT INTO command_receipts (task_id, command_id, owner_id, committed_epoch, fingerprint, result)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [taskId, commandId, ownerId, epoch, fp, JSON.stringify({ seq: nextSeq, events: transition.events })],
          );
        } catch (err) {
          if (isDuplicateKeyError(err)) throw new CommandConflict(taskId, commandId);
          throw err;
        }

        await conn.query(
          `UPDATE tasks
             SET checkpoint_digest = ?, candidate_id = ?, verification_status = ?,
                 verification_attempt = ?, status = ?, seq = ?
           WHERE task_id = ? AND epoch = ?`,
          [
            transition.state.checkpointDigest,
            transition.state.candidateId,
            transition.state.verificationStatus,
            transition.state.verificationAttempt,
            transition.state.status,
            nextSeq,
            taskId,
            epoch,
          ],
        );

        // Claim the handoff for THIS commandId, atomically, in the
        // SAME transaction -- this is what makes "a different command
        // ID acknowledging the same handoff" detectable on the NEXT
        // attempt, durably, without relying on the commandId-keyed
        // receipt ledger alone.
        await conn.query(`UPDATE task_handoffs SET restore_ack_command_id = ? WHERE task_id = ? AND handoff_id = ?`, [commandId, taskId, handoffId]);

        const carrier = captureCurrentCarrier();
        for (const event of transition.events) {
          await conn.query(`INSERT INTO outbox_events (event_id, task_id, seq, type, payload, trace_carrier) VALUES (?, ?, ?, ?, ?, ?)`, [
            randomUUID(),
            taskId,
            nextSeq,
            event.type,
            JSON.stringify(event),
            carrier ? JSON.stringify(carrier) : null,
          ]);
        }

        return { taskId, commandId, result: { seq: nextSeq, events: transition.events }, replay: false };
      });
    }
  }

  async function getTaskImpl(taskId: string): Promise<TaskState> {
    const [rows] = await pool.query<TaskRow[]>(`SELECT * FROM tasks WHERE task_id = ?`, [taskId]);
    const row = rows[0];
    if (!row) throw new TaskNotFound(taskId);
    return rowToState(row);
  }

  /**
   * Phase 2B.2, Part 1: `RestoreAcknowledged` MUST NOT be submittable
   * through this generic path -- `applyCommand`'s domain-level case for
   * it (src/domain/domain.ts) succeeds unconditionally (it has no way
   * to see `task_handoffs` at all, by design: the domain layer is pure,
   * no I/O), so routing it through here would accept ARBITRARY,
   * unvalidated `artifactDigest`/`restoredCommit`/`restoredTree` content
   * and still emit a real `RestoreAcknowledged` outbox event -- this was
   * CONFIRMED as a live, reachable bypass of
   * `recordRestoreAcknowledgementImpl`'s entire validation chain (see
   * tests/integration/git-worker-handoff-authorization.test.ts "the
   * generic applyTaskCommand route cannot be used to bypass...").
   * Rejected here, at the public durable boundary, rather than routed
   * into the specialized method -- routing would mean either a SECOND,
   * nested transaction (this function already holds its own) or
   * threading an open `conn` across two unrelated call paths, neither
   * of which is worth the complexity for a command this project never
   * legitimately submits through the generic path anyway. Checked
   * BEFORE any I/O -- pure, synchronous, no transaction opened for a
   * request that is going to be refused regardless.
   */
  async function applyTaskCommandImpl(args: {
    taskId: string;
    ownerId: string;
    epoch: number;
    commandId: string;
    command: Command;
  }): Promise<CommandReceipt> {
    const { taskId, ownerId, epoch, commandId, command } = args;
    if (command.type === "RestoreAcknowledged") {
      throw new CommandRejected(
        `applyTaskCommand: "RestoreAcknowledged" must be submitted through TaskStore.recordRestoreAcknowledgement -- the generic command path refuses it outright, since it cannot validate the command against durable handoff identity (task_handoffs) and must never silently accept unvalidated handoff-completion content`,
      );
    }
    const fp = fingerprint(command);

    // One span per applyTaskCommand call, covering fencing, the domain
    // transition, and the transaction. Committed via startActiveSpan
    // (inside withRelaylabSpan) so that captureCurrentCarrier() below
    // (called deep inside the transaction callback, across several
    // `await`s) sees this span as `context.active()` -- Node's
    // AsyncLocalStorage-based context manager (the SDK default)
    // propagates it through the same async call stack. See
    // docs/OBSERVABILITY.md "span-status policy" for why expected domain
    // rejections (EpochStale, ClaimLost, NotAuthorized, CommandConflict,
    // CommandRejected) do NOT set span status ERROR, while unexpected
    // infrastructure failures do.
    return withRelaylabSpan(
      "relaylab.command.process",
      { taskId, commandId, commandType: command.type, epoch },
      () => applyTaskCommandTransaction(),
      (result) => ({ outcome: result.replay ? "replayed" : "committed" }),
    );

    async function applyTaskCommandTransaction(): Promise<CommandReceipt> {
      return withTransaction(pool, async (conn) => {
      // Phase 2B.2, Part 2: the row lock is acquired FIRST, BEFORE the
      // receipt lookup -- reproduced directly as a real bug before this
      // fix (two genuinely concurrent, byte-identical `applyTaskCommand`
      // calls sharing one `commandId`): under the ORIGINAL ordering
      // (receipt check, THEN row lock), the loser's receipt check reads
      // "not found" using a consistent-read snapshot established at
      // that EARLY point (before the winner commits); it then blocks on
      // the row lock, and once released (after the winner's commit) it
      // proceeds to compute its OWN transition and attempt its OWN
      // INSERT INTO command_receipts -- which then fails with a
      // duplicate-key error, mapped UNCONDITIONALLY to `CommandConflict`
      // regardless of whether the payload actually differs. Confirmed:
      // two identical concurrent `SubmitCandidate` calls produced
      // `{replay:false}` for the winner and a `CommandConflict` ("...was
      // already used with a different payload") for the loser, even
      // though the payload was byte-identical. Fixed the same way
      // `transferOwnershipImpl`/`recordRestoreAcknowledgementImpl`
      // already are (Phase 2B.1): acquiring the row lock FIRST means
      // InnoDB REPEATABLE READ establishes this transaction's
      // consistent-read snapshot AFTER waiting for the winner's commit,
      // so the SUBSEQUENT receipt lookup correctly observes it.
      const [rows] = await conn.query<TaskRow[]>(
        `SELECT * FROM tasks WHERE task_id = ? FOR UPDATE`,
        [taskId],
      );
      const row = rows[0];
      if (!row) throw new TaskNotFound(taskId);
      // `db_now` is read via `freshDbNow` AFTER this lock is held
      // (Phase 2C, Part 5 -- see that helper's comment).
      const dbNow = await freshDbNow(conn);

      // Idempotency + authorization fast path (section 2 audit):
      //   A. authorize the requester (ownerId must match who originally
      //      committed this receipt) -- see docs/decisions/0004-receipt-authorization.md
      //      for the trusted-caller boundary this does and does not cover
      //   B. look up the task-scoped receipt, inside this same transaction
      //   C. same identity + same fingerprint => return the original
      //      result, no new transition, regardless of CURRENT epoch/lease
      //      (a replay of an already-committed command must succeed even
      //      after ownership has since moved on -- that is the whole point
      //      of idempotent retries)
      //   D. same identity + different fingerprint => CommandConflict
      // Fencing (epoch/lease/ownership) below only applies to GENUINELY
      // NEW commands, never to a replay of one that already committed.
      const [existingRows] = await conn.query<
        (RowDataPacket & { fingerprint: string; result: unknown; owner_id: string; committed_epoch: number })[]
      >(
        `SELECT fingerprint, result, owner_id, committed_epoch FROM command_receipts WHERE task_id = ? AND command_id = ?`,
        [taskId, commandId],
      );
      const existing = existingRows[0];
      if (existing) {
        // Authorization check FIRST, distinct from fencing: this is "is the
        // requester the same identity that originally issued this command,"
        // not "is the requester currently the task's owner at the current
        // epoch." A legitimate retry from the ORIGINAL owner must succeed
        // even across an ownership change (see the fingerprint check
        // below); a request claiming a DIFFERENT ownerId than the one on
        // record must not be able to read or "confirm" someone else's
        // receipt. Trusted-caller boundary: ownerId is a caller-supplied
        // claim, not an authenticated principal -- Phase 1 has no
        // authentication layer, so this check stops accidental
        // command_id collisions between workers, not a malicious caller
        // willing to lie about its own ownerId. Real authentication is out
        // of scope for Phase 1 (see CLAUDE.md).
        if (existing.owner_id !== ownerId) throw new NotAuthorized(taskId, commandId);
        if (existing.fingerprint !== fp) throw new CommandConflict(taskId, commandId);
        return { taskId, commandId, result: existing.result, replay: true };
      }

      // `dbNow` was read via `freshDbNow` AFTER the row lock above, so
      // the lease-expiry check below compares two timestamps from the
      // SAME clock (the database's) -- never a DB timestamp against
      // Node's Date.now(), per invariant B -- AND reflects the time
      // AFTER any lock wait, not before it (Phase 2C, Part 5).

      // Fencing: a command from a stale epoch or from a non-owner must not
      // mutate state, even if the row would otherwise accept it.
      if (row.epoch !== epoch) throw new EpochStale(taskId, epoch, row.epoch);
      if (row.owner_id !== ownerId) throw new ClaimLost(taskId);
      if (row.status === "owned" && (!row.lease_expires_at || row.lease_expires_at.getTime() < dbNow.getTime())) {
        throw new ClaimLost(taskId);
      }

      const state = rowToState(row);
      let transition;
      try {
        transition = applyCommand(state, command);
      } catch (err) {
        if (err instanceof DomainRejection) throw new CommandRejected(err.message);
        throw err;
      }

      const nextSeq = transition.state.seq;

      try {
        await conn.query(
          `INSERT INTO command_receipts (task_id, command_id, owner_id, committed_epoch, fingerprint, result)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [taskId, commandId, ownerId, epoch, fp, JSON.stringify({ seq: nextSeq, events: transition.events })],
        );
      } catch (err) {
        if (isDuplicateKeyError(err)) throw new CommandConflict(taskId, commandId);
        throw err;
      }

      await conn.query(
        `UPDATE tasks
           SET checkpoint_digest = ?, candidate_id = ?, verification_status = ?,
               verification_attempt = ?, status = ?, seq = ?
         WHERE task_id = ? AND epoch = ?`,
        [
          transition.state.checkpointDigest,
          transition.state.candidateId,
          transition.state.verificationStatus,
          transition.state.verificationAttempt,
          transition.state.status,
          nextSeq,
          taskId,
          epoch,
        ],
      );

      // Captured ONCE per command (not per event) and persisted alongside
      // each outbox row in the SAME transaction -- no network call, just
      // reading the currently-active span's context (see
      // src/telemetry/propagation.ts). This is what lets a LATER process
      // (the publisher) continue the same trace when it eventually reads
      // this row; see publisher.ts's publishOne.
      const carrier = captureCurrentCarrier();
      for (const event of transition.events) {
        await conn.query(
          `INSERT INTO outbox_events (event_id, task_id, seq, type, payload, trace_carrier) VALUES (?, ?, ?, ?, ?, ?)`,
          [randomUUID(), taskId, nextSeq, event.type, JSON.stringify(event), carrier ? JSON.stringify(carrier) : null],
        );
      }

        return { taskId, commandId, result: { seq: nextSeq, events: transition.events }, replay: false };
      });
    }
  }

  /**
   * Phase 2A: mirrors `applyTaskCommandImpl`'s fencing/idempotency/
   * transaction structure exactly (same receipt ledger, same
   * epoch/owner/lease checks, same outbox insert), with ONE addition:
   * an INSERT into `task_checkpoints` carrying the full manifest, in the
   * SAME transaction. Kept as a SEPARATE function (rather than threading
   * an optional "extra insert" callback through `applyTaskCommandImpl`)
   * so the well-tested generic command path is never put at risk by a
   * Phase-2A-specific change -- the duplication is the SAME small
   * fencing/idempotency block, not new logic.
   */
  async function recordGitCheckpointImpl(args: {
    taskId: string;
    ownerId: string;
    epoch: number;
    commandId: string;
    attemptId: string;
    manifestDigest: string;
    manifestJson: string;
  }): Promise<CommandReceipt> {
    const { taskId, ownerId, epoch, commandId, manifestDigest } = args;
    // Fingerprinted as the SAME synthetic Checkpoint command
    // `applyTaskCommand` would use for an identical digest -- a
    // `recordGitCheckpoint` call and a plain `Checkpoint` command that
    // both record the same digest are the SAME business event for
    // idempotency purposes. Deliberately does NOT incorporate
    // epoch/attemptId -- this is what lets a REPLAY from the original
    // owner succeed below even after the task's epoch has since moved
    // on (docs/decisions/0004-receipt-authorization.md).
    const fp = fingerprint({ type: "Checkpoint", digest: manifestDigest });

    return withRelaylabSpan(
      "relaylab.checkpoint.publish",
      { taskId, commandId, commandType: "Checkpoint", epoch },
      () => transaction(),
      (result) => ({ outcome: result.replay ? "replayed" : "committed" }),
    );

    async function transaction(): Promise<CommandReceipt> {
      return withTransaction(pool, async (conn) => {
        // Phase 2B.2, Part 2: row lock FIRST, THEN the receipt lookup --
        // same fix, same reasoning, as `applyTaskCommandImpl` above (see
        // its own comment for the exact reproduced failure mode: two
        // concurrent, identical requests sharing one `commandId` used to
        // make the loser's receipt-lookup use a too-early consistent-read
        // snapshot, so it would try its OWN insert and fail with a
        // misleading `CommandConflict` instead of resolving as a replay).
        const [rows] = await conn.query<TaskRow[]>(`SELECT * FROM tasks WHERE task_id = ? FOR UPDATE`, [taskId]);
        const row = rows[0];
        if (!row) throw new TaskNotFound(taskId);
        // `db_now` is read via `freshDbNow` AFTER this lock is held
        // (Phase 2C, Part 5 -- see that helper's comment).
        const dbNow = await freshDbNow(conn);

        const [existingRows] = await conn.query<
          (RowDataPacket & { fingerprint: string; result: unknown; owner_id: string; committed_epoch: number })[]
        >(`SELECT fingerprint, result, owner_id, committed_epoch FROM command_receipts WHERE task_id = ? AND command_id = ?`, [
          taskId,
          commandId,
        ]);
        const existing = existingRows[0];
        if (existing) {
          // A REPLAY of an already-committed checkpoint publish: per
          // this project's own replay-authorization rule, this must
          // succeed for the ORIGINAL committing owner even if the
          // task's epoch has since moved on -- so this branch
          // deliberately returns BEFORE the request/manifest IDENTITY
          // check below (taskId/attemptId/epoch), exactly mirroring
          // `applyTaskCommandImpl`. It does NOT, however, skip
          // manifest-internal shape/digest validation (Part A fix,
          // below) -- that is NEVER skipped, replay or not.
          if (existing.owner_id !== ownerId) throw new NotAuthorized(taskId, commandId);
          if (existing.fingerprint !== fp) throw new CommandConflict(taskId, commandId);
          // Part A: even though `fp` already matched (same CLAIMED
          // digest as the original call), that says nothing about
          // whether the manifestJson supplied THIS time actually
          // corresponds to that digest -- recompute and compare now, so
          // a reused digest paired with altered manifest content can
          // never be silently accepted as an identical retry.
          parseAndValidateGitCheckpointManifestShapeAndDigest(args.manifestJson, manifestDigest);
          return { taskId, commandId, result: existing.result, replay: true };
        }

        // Not a replay: a genuinely new write, subject to current
        // fencing. Shape/digest validation (always) plus request/
        // manifest identity agreement (new-write only -- see that
        // function's own doc comment for why it must not run on replay).
        const manifest = parseAndValidateGitCheckpointManifestShapeAndDigest(args.manifestJson, manifestDigest);
        assertManifestIdentityMatchesRequest(manifest, { taskId, attemptId: args.attemptId, epoch });

        // Fencing against `dbNow` read AFTER the row lock above
        // (Phase 2C, Part 5) -- never a second (Node-side) clock read.
        if (row.epoch !== epoch) throw new EpochStale(taskId, epoch, row.epoch);
        if (row.owner_id !== ownerId) throw new ClaimLost(taskId);
        if (row.status === "owned" && (!row.lease_expires_at || row.lease_expires_at.getTime() < dbNow.getTime())) {
          throw new ClaimLost(taskId);
        }

        const state = rowToState(row);
        const transition = applyCommand(state, { type: "Checkpoint", digest: manifestDigest });
        const nextSeq = transition.state.seq;

        try {
          await conn.query(
            `INSERT INTO command_receipts (task_id, command_id, owner_id, committed_epoch, fingerprint, result)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [taskId, commandId, ownerId, epoch, fp, JSON.stringify({ seq: nextSeq, events: transition.events })],
          );
        } catch (err) {
          if (isDuplicateKeyError(err)) throw new CommandConflict(taskId, commandId);
          throw err;
        }

        await conn.query(
          `INSERT INTO task_checkpoints
             (task_id, command_id, manifest_version, attempt_id, epoch, base_commit, result_commit, result_tree,
              verification_command, verification_outcome, runtime_fingerprint, verification_image_id, manifest_digest, manifest_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            taskId,
            commandId,
            manifest.manifestVersion,
            manifest.attemptId,
            epoch,
            manifest.baseCommit,
            manifest.resultCommit,
            manifest.resultTree,
            JSON.stringify(manifest.verification.command),
            manifest.verification.outcome,
            JSON.stringify(manifest.runtimeFingerprint),
            manifest.verification.containerImageId ?? null,
            manifestDigest,
            args.manifestJson,
          ],
        );

        await conn.query(
          `UPDATE tasks
             SET checkpoint_digest = ?, candidate_id = ?, verification_status = ?,
                 verification_attempt = ?, status = ?, seq = ?
           WHERE task_id = ? AND epoch = ?`,
          [
            transition.state.checkpointDigest,
            transition.state.candidateId,
            transition.state.verificationStatus,
            transition.state.verificationAttempt,
            transition.state.status,
            nextSeq,
            taskId,
            epoch,
          ],
        );

        const carrier = captureCurrentCarrier();
        for (const event of transition.events) {
          await conn.query(`INSERT INTO outbox_events (event_id, task_id, seq, type, payload, trace_carrier) VALUES (?, ?, ?, ?, ?, ?)`, [
            randomUUID(),
            taskId,
            nextSeq,
            event.type,
            JSON.stringify(event),
            carrier ? JSON.stringify(carrier) : null,
          ]);
        }

        return { taskId, commandId, result: { seq: nextSeq, events: transition.events }, replay: false };
      });
    }
  }

  return TaskStore.of({
    claimTask: (taskId, ownerId, leaseMs) =>
      Effect.tryPromise({
        try: () => claimTaskImpl(taskId, ownerId, leaseMs),
        catch: (err) => (err instanceof ClaimLost ? err : wrapDbError(err)),
      }),
    renewLease: (taskId, ownerId, epoch, leaseMs) =>
      Effect.tryPromise({
        try: () => renewLeaseImpl(taskId, ownerId, epoch, leaseMs),
        catch: (err) => (err instanceof ClaimLost || err instanceof EpochStale ? err : wrapDbError(err)),
      }),
    getTask: (taskId) =>
      Effect.tryPromise({
        try: () => getTaskImpl(taskId),
        catch: (err) => (err instanceof TaskNotFound ? err : new TransientStoreError("getTask failed", err)),
      }),
    applyTaskCommand: (args) =>
      Effect.tryPromise({
        try: () => applyTaskCommandImpl(args),
        catch: (err) => {
          if (
            err instanceof EpochStale ||
            err instanceof ClaimLost ||
            err instanceof NotAuthorized ||
            err instanceof CommandConflict ||
            err instanceof CommandRejected ||
            err instanceof TaskNotFound
          ) {
            return err;
          }
          return wrapDbError(err);
        },
      }),
    recordGitCheckpoint: (args) =>
      Effect.tryPromise({
        try: () => recordGitCheckpointImpl(args),
        catch: (err) => {
          if (
            err instanceof EpochStale ||
            err instanceof ClaimLost ||
            err instanceof NotAuthorized ||
            err instanceof CommandConflict ||
            err instanceof CommandRejected ||
            err instanceof TaskNotFound
          ) {
            return err;
          }
          return wrapDbError(err);
        },
      }),
    transferOwnership: (args) =>
      Effect.tryPromise({
        try: () => transferOwnershipImpl(args),
        catch: (err) => {
          if (
            err instanceof EpochStale ||
            err instanceof ClaimLost ||
            err instanceof NotAuthorized ||
            err instanceof CommandConflict ||
            err instanceof CommandRejected ||
            err instanceof TaskNotFound
          ) {
            return err;
          }
          return wrapDbError(err);
        },
      }),
    recordRestoreAcknowledgement: (args) =>
      Effect.tryPromise({
        try: () => recordRestoreAcknowledgementImpl(args),
        catch: (err) => {
          if (
            err instanceof EpochStale ||
            err instanceof ClaimLost ||
            err instanceof NotAuthorized ||
            err instanceof CommandConflict ||
            err instanceof CommandRejected ||
            err instanceof TaskNotFound
          ) {
            return err;
          }
          return wrapDbError(err);
        },
      }),
  });
}

export function TaskStoreLive(pool: mysql.Pool): Layer.Layer<TaskStore> {
  return Layer.succeed(TaskStore, makeTaskStoreLive(pool));
}
