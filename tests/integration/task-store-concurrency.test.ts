// Phase 2B.2, Part 2: targeted regression coverage for the receipt-
// lookup-before-row-lock race, repaired in BOTH `applyTaskCommandImpl`
// and `recordGitCheckpointImpl` (src/db/task-store.ts) by acquiring the
// `tasks` row lock BEFORE the `command_receipts` lookup, mirroring the
// Phase 2B.1 fix already applied to `transferOwnershipImpl`/
// `recordRestoreAcknowledgementImpl`. The race was REPRODUCED directly
// before this patch (see docs/PROGRESS.md "Phase 2B.2" for the exact
// repro output: two byte-identical concurrent `applyTaskCommand` calls
// sharing one commandId produced `{replay:false}` for the winner and a
// misleading `CommandConflict` -- "already used with a different
// payload" -- for the loser, despite an IDENTICAL payload). These tests
// use real concurrent `Promise.all` calls against real MySQL --
// deterministic synchronization (the row lock itself) decides the
// outcome, never a sleep-based race.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { Effect } from "effect";
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";
import { buildManifest, buildRuntimeFingerprint, computeManifestDigest, manifestToStableJson, type CheckpointManifest } from "../../src/worker/checkpoint.js";
import { makeIsolatedGitEnv } from "../../src/worker/git.js";

let pool: mysql.Pool;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
});

beforeEach(async () => {
  await truncateAll(pool);
});

after(async () => {
  await pool.end();
});

async function buildFakeManifest(taskId: string, epoch: number, resultCommit: string): Promise<CheckpointManifest> {
  const env = makeIsolatedGitEnv(process.env.TEMP ?? process.env.TMP ?? "/tmp");
  const runtimeFingerprint = await buildRuntimeFingerprint(env);
  return buildManifest({
    taskId,
    attemptId: "attempt-1",
    epoch,
    baseCommit: "a".repeat(40),
    resultCommit,
    resultTree: "c".repeat(40),
    verification: { exitCode: 0, outcome: "passed", stdout: "", stderr: "", stdoutTruncated: false, stderrTruncated: false, durationMs: 1 },
    runtimeFingerprint,
    containerImageId: null,
  });
}

async function receiptCount(taskId: string, commandId: string): Promise<number> {
  const [rows] = await pool.query<(RowDataPacket & { c: number })[]>(`SELECT COUNT(*) AS c FROM command_receipts WHERE task_id = ? AND command_id = ?`, [taskId, commandId]);
  return Number(rows[0]?.c ?? 0);
}
async function outboxCount(taskId: string, type: string): Promise<number> {
  const [rows] = await pool.query<(RowDataPacket & { c: number })[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = ?`, [taskId, type]);
  return Number(rows[0]?.c ?? 0);
}
async function checkpointCount(taskId: string): Promise<number> {
  const [rows] = await pool.query<(RowDataPacket & { c: number })[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  return Number(rows[0]?.c ?? 0);
}

// ---------------------------------------------------------------------
// applyTaskCommand
// ---------------------------------------------------------------------

test("applyTaskCommand: two CONCURRENT IDENTICAL requests produce ONE durable mutation/event -- both callers get the correct committed/replayed result (the reproduced race, fixed)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-concurrent-identical-command");
  const ownerId = "worker-a";
  const claim = await Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));
  const commandId = "cmd-1";
  const command = { type: "SubmitCandidate" as const, candidateId: "cand-1" };

  const call = () => Effect.runPromise(store.applyTaskCommand({ taskId, ownerId, epoch: claim.epoch, commandId, command }));
  const [r1, r2] = await Promise.all([call(), call()]);

  const replays = [r1.replay, r2.replay].sort();
  assert.deepEqual(replays, [false, true], "exactly one fresh commit and one replay -- NEVER two commits, NEVER a spurious conflict for an identical payload");
  assert.deepEqual(r1.result, r2.result, "both callers must observe the SAME logical result");
  assert.equal(await receiptCount(taskId, commandId), 1, "exactly one durable receipt row");
  assert.equal(await outboxCount(taskId, "CandidateSubmitted"), 1, "exactly one logical event, despite two concurrent calls");
});

test("applyTaskCommand: concurrent requests sharing an ID but DIFFERING in payload produce one winner and one conflict, with NO partial writes", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-concurrent-conflicting-command");
  const ownerId = "worker-a";
  const claim = await Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));
  const commandId = "cmd-1";

  const callA = () => Effect.runPromise(store.applyTaskCommand({ taskId, ownerId, epoch: claim.epoch, commandId, command: { type: "SubmitCandidate", candidateId: "cand-A" } }));
  const callB = () => Effect.runPromise(store.applyTaskCommand({ taskId, ownerId, epoch: claim.epoch, commandId, command: { type: "SubmitCandidate", candidateId: "cand-B" } }));

  const results = await Promise.allSettled([callA(), callB()]);
  const fulfilled = results.filter((r) => r.status === "fulfilled");
  const rejected = results.filter((r) => r.status === "rejected");
  assert.equal(fulfilled.length, 1, "exactly one winner");
  assert.equal(rejected.length, 1, "exactly one conflict");
  assert.match((rejected[0] as PromiseRejectedResult).reason?.message ?? "", /already used with a different payload/i);

  assert.equal(await receiptCount(taskId, commandId), 1, "exactly one receipt row -- no partial/duplicate write from the loser");
  assert.equal(await outboxCount(taskId, "CandidateSubmitted"), 1, "exactly one event -- the loser's attempted transition never partially committed");

  const task = await Effect.runPromise(store.getTask(taskId));
  const winnerCandidate = (fulfilled[0] as PromiseFulfilledResult<any>).value.result.events[0].candidateId;
  assert.equal(task.candidateId, winnerCandidate, "the task's actual durable state must reflect exactly the WINNER's payload, never a mix");
});

test("applyTaskCommand: authorized historical replay remains valid after ownership has changed", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-command-replay-after-ownership-change");
  const ownerA = "worker-a";
  const claimA = await Effect.runPromise(store.claimTask(taskId, ownerA, 1_000));
  const commandId = "cmd-1";
  const command = { type: "SubmitCandidate" as const, candidateId: "cand-1" };

  const first = await Effect.runPromise(store.applyTaskCommand({ taskId, ownerId: ownerA, epoch: claimA.epoch, commandId, command }));
  assert.equal(first.replay, false);

  await new Promise((r) => setTimeout(r, 1_200));
  const claimB = await Effect.runPromise(store.claimTask(taskId, "worker-b", 120_000));
  assert.notEqual(claimB.epoch, claimA.epoch);

  // A retries its OWN already-committed command, still claiming its
  // now-stale epoch -- must succeed as a replay regardless.
  const second = await Effect.runPromise(store.applyTaskCommand({ taskId, ownerId: ownerA, epoch: claimA.epoch, commandId, command }));
  assert.equal(second.replay, true);
  assert.deepEqual(second.result, first.result);
  assert.equal(await outboxCount(taskId, "CandidateSubmitted"), 1, "no second event from the replay");
});

test("applyTaskCommand: an UNAUTHORIZED caller (different ownerId) cannot retrieve a successful receipt", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-command-unauthorized-replay");
  const ownerA = "worker-a";
  const claimA = await Effect.runPromise(store.claimTask(taskId, ownerA, 120_000));
  const commandId = "cmd-1";
  const command = { type: "SubmitCandidate" as const, candidateId: "cand-1" };
  await Effect.runPromise(store.applyTaskCommand({ taskId, ownerId: ownerA, epoch: claimA.epoch, commandId, command }));

  await assert.rejects(
    () => Effect.runPromise(store.applyTaskCommand({ taskId, ownerId: "worker-imposter", epoch: claimA.epoch, commandId, command })),
    /originally committed by a different owner/i,
  );
});

// ---------------------------------------------------------------------
// recordGitCheckpoint
// ---------------------------------------------------------------------

test("recordGitCheckpoint: two CONCURRENT IDENTICAL publishes produce ONE durable checkpoint -- both callers get the correct committed/replayed result", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-concurrent-identical-checkpoint");
  const ownerId = "worker-a";
  const claim = await Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));
  const manifest = await buildFakeManifest(taskId, claim.epoch, "b".repeat(40));
  const manifestDigest = computeManifestDigest(manifest);
  const manifestJson = manifestToStableJson(manifest);
  const commandId = `git-checkpoint-${taskId}`;

  const call = () =>
    Effect.runPromise(store.recordGitCheckpoint({ taskId, ownerId, epoch: claim.epoch, commandId, attemptId: "attempt-1", manifestDigest, manifestJson }));
  const [r1, r2] = await Promise.all([call(), call()]);

  const replays = [r1.replay, r2.replay].sort();
  assert.deepEqual(replays, [false, true], "exactly one fresh commit and one replay");
  assert.equal(await checkpointCount(taskId), 1, "exactly one durable checkpoint row despite two concurrent identical publishes");
  assert.equal(await outboxCount(taskId, "CheckpointRecorded"), 1);
});

test("recordGitCheckpoint: concurrent publishes sharing a commandId but with DIFFERENT manifest digests produce one winner and one conflict, no partial writes", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-concurrent-conflicting-checkpoint");
  const ownerId = "worker-a";
  const claim = await Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));
  const commandId = `git-checkpoint-${taskId}`;
  const manifestA = await buildFakeManifest(taskId, claim.epoch, "b".repeat(40));
  const manifestB = await buildFakeManifest(taskId, claim.epoch, "d".repeat(40));

  const callA = () =>
    Effect.runPromise(
      store.recordGitCheckpoint({ taskId, ownerId, epoch: claim.epoch, commandId, attemptId: "attempt-1", manifestDigest: computeManifestDigest(manifestA), manifestJson: manifestToStableJson(manifestA) }),
    );
  const callB = () =>
    Effect.runPromise(
      store.recordGitCheckpoint({ taskId, ownerId, epoch: claim.epoch, commandId, attemptId: "attempt-1", manifestDigest: computeManifestDigest(manifestB), manifestJson: manifestToStableJson(manifestB) }),
    );

  const results = await Promise.allSettled([callA(), callB()]);
  const fulfilled = results.filter((r) => r.status === "fulfilled");
  const rejected = results.filter((r) => r.status === "rejected");
  assert.equal(fulfilled.length, 1);
  assert.equal(rejected.length, 1);
  assert.match((rejected[0] as PromiseRejectedResult).reason?.message ?? "", /already used with a different payload/i);
  assert.equal(await checkpointCount(taskId), 1, "exactly one checkpoint row -- no partial write from the loser");
});

test("recordGitCheckpoint: authorized historical replay remains valid after ownership has changed", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-checkpoint-replay-after-ownership-change");
  const ownerA = "worker-a";
  const claimA = await Effect.runPromise(store.claimTask(taskId, ownerA, 1_000));
  const commandId = `git-checkpoint-${taskId}`;
  const manifest = await buildFakeManifest(taskId, claimA.epoch, "b".repeat(40));
  const manifestDigest = computeManifestDigest(manifest);
  const manifestJson = manifestToStableJson(manifest);

  const first = await Effect.runPromise(store.recordGitCheckpoint({ taskId, ownerId: ownerA, epoch: claimA.epoch, commandId, attemptId: "attempt-1", manifestDigest, manifestJson }));
  assert.equal(first.replay, false);

  await new Promise((r) => setTimeout(r, 1_200));
  const claimB = await Effect.runPromise(store.claimTask(taskId, "worker-b", 120_000));
  assert.notEqual(claimB.epoch, claimA.epoch);

  const second = await Effect.runPromise(store.recordGitCheckpoint({ taskId, ownerId: ownerA, epoch: claimA.epoch, commandId, attemptId: "attempt-1", manifestDigest, manifestJson }));
  assert.equal(second.replay, true);
  assert.equal(await checkpointCount(taskId), 1);
});

test("recordGitCheckpoint: an UNAUTHORIZED caller cannot retrieve a successful receipt", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-checkpoint-unauthorized-replay");
  const ownerA = "worker-a";
  const claimA = await Effect.runPromise(store.claimTask(taskId, ownerA, 120_000));
  const commandId = `git-checkpoint-${taskId}`;
  const manifest = await buildFakeManifest(taskId, claimA.epoch, "b".repeat(40));
  const manifestDigest = computeManifestDigest(manifest);
  const manifestJson = manifestToStableJson(manifest);
  await Effect.runPromise(store.recordGitCheckpoint({ taskId, ownerId: ownerA, epoch: claimA.epoch, commandId, attemptId: "attempt-1", manifestDigest, manifestJson }));

  await assert.rejects(
    () => Effect.runPromise(store.recordGitCheckpoint({ taskId, ownerId: "worker-imposter", epoch: claimA.epoch, commandId, attemptId: "attempt-1", manifestDigest, manifestJson })),
    /originally committed by a different owner/i,
  );
});

test("recordGitCheckpoint: altered manifest content with a REUSED claimed digest remains rejected (Phase 2B Part A, re-verified against the reordered code)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-checkpoint-altered-content-reused-digest");
  const ownerA = "worker-a";
  const claimA = await Effect.runPromise(store.claimTask(taskId, ownerA, 120_000));
  const commandId = `git-checkpoint-${taskId}`;
  const manifest = await buildFakeManifest(taskId, claimA.epoch, "b".repeat(40));
  const digest = computeManifestDigest(manifest);
  const manifestJson = manifestToStableJson(manifest);

  await Effect.runPromise(store.recordGitCheckpoint({ taskId, ownerId: ownerA, epoch: claimA.epoch, commandId, attemptId: "attempt-1", manifestDigest: digest, manifestJson }));

  const tampered: CheckpointManifest = { ...manifest, resultTree: "f".repeat(40) };
  const tamperedJson = manifestToStableJson(tampered);
  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordGitCheckpoint({ taskId, ownerId: ownerA, epoch: claimA.epoch, commandId, attemptId: "attempt-1", manifestDigest: digest, manifestJson: tamperedJson }),
      ),
    /does not match the digest recomputed/i,
  );
  assert.equal(await checkpointCount(taskId), 1, "still exactly the original checkpoint -- the tampered retry must not be persisted");
});

// ---------------------------------------------------------------------
// Phase 2C, Part 5: does lease validity go stale while WAITING for the
// row lock? Investigated directly: `SELECT ..., NOW(3) AS db_now FROM
// tasks WHERE task_id = ? FOR UPDATE` is ONE statement -- MySQL blocks
// the whole statement (including its own `NOW(3)` evaluation) until the
// lock is actually acquired, so `db_now` reflects the time AFTER
// waiting, never a timestamp captured BEFORE the wait. This test proves
// it behaviorally: a lease is deliberately made to expire WHILE a
// genuinely new command is blocked waiting for the row lock (held open
// by a separate, real connection) -- the fresh guarded mutation must
// still reject it once the lock is finally available, not authorize a
// write based on a stale "was valid when we started waiting" belief.
// ---------------------------------------------------------------------

test("lease-staleness-across-lock-wait: applyTaskCommand correctly rejects a lease that expired WHILE waiting for the row lock (lease validity does not go stale across a long lock wait)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-lease-staleness-across-lock-wait");
  const ownerA = "worker-a";
  // A short lease -- deliberately shorter than the lock-hold below, so
  // it has DEFINITELY expired (by real wall-clock time) before the
  // holder releases the row.
  const claim = await Effect.runPromise(store.claimTask(taskId, ownerA, 800));

  // Hold the row's FOR UPDATE lock open, in a SEPARATE real connection/
  // transaction, for longer than the lease -- this is what forces
  // `applyTaskCommand`'s own `SELECT ... FOR UPDATE` below to actually
  // BLOCK and wait, rather than merely racing it.
  const holder = await pool.getConnection();
  await holder.beginTransaction();
  await holder.query(`SELECT * FROM tasks WHERE task_id = ? FOR UPDATE`, [taskId]);

  // `Effect.either` (same convention `Effect.flip` serves elsewhere in
  // this file for an EXPECTED single failure) turns the expected/
  // unexpected failure channel into an ordinary resolved value, so this
  // never rejects the underlying Promise -- rejecting would otherwise
  // surface Effect's own FiberFailure wrapper, not the raw tagged error.
  const eitherPromise = Effect.runPromise(
    Effect.either(
      store.applyTaskCommand({ taskId, ownerId: ownerA, epoch: claim.epoch, commandId: "cmd-1", command: { type: "SubmitCandidate", candidateId: "cand-1" } }),
    ),
  );

  // Hold well past the 800ms lease, THEN release -- `applyTaskCommand`
  // has been blocked on the lock for this entire window.
  await new Promise((r) => setTimeout(r, 1_500));
  await holder.commit();
  holder.release();

  const result = await eitherPromise;
  assert.equal(result._tag, "Left", "a write that only became able to proceed AFTER its lease had already expired (by real wall-clock time, during the lock wait) must be rejected");
  if (result._tag === "Left") {
    assert.equal(result.left._tag, "ClaimLost");
  }
});
