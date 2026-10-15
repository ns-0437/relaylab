// Phase 2B.1: targeted regression coverage for the two correctness gaps
// closed in this patch -- real MySQL, no real Git/fixture needed, since
// both `TaskStore.transferOwnership` and
// `TaskStore.recordRestoreAcknowledgement` are pure DB-boundary
// functions (same convention as git-worker-checkpoint-manifest.test.ts:
// fake-but-well-formed SHAs, no worktree). Calls the durable command
// boundary DIRECTLY, bypassing src/worker/handoff.ts's helpers, per the
// task's own instruction.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { Effect } from "effect";
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";
import { buildManifest, buildRuntimeFingerprint, computeManifestDigest, manifestToStableJson, type CheckpointManifest } from "../../src/worker/checkpoint.js";
import { makeIsolatedGitEnv } from "../../src/worker/git.js";
import { handoffCommandId } from "../../src/worker/handoff.js";

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

const FAKE_ARTIFACT_DIGEST_A = "a1".repeat(32);
const FAKE_ARTIFACT_DIGEST_B = "b2".repeat(32);

async function buildFakeManifest(taskId: string, epoch: number): Promise<CheckpointManifest> {
  const env = makeIsolatedGitEnv(process.env.TEMP ?? process.env.TMP ?? "/tmp");
  const runtimeFingerprint = await buildRuntimeFingerprint(env);
  return buildManifest({
    taskId,
    attemptId: "attempt-1",
    epoch,
    baseCommit: "a".repeat(40),
    resultCommit: "b".repeat(40),
    resultTree: "c".repeat(40),
    verification: { exitCode: 0, outcome: "passed", stdout: "", stderr: "", stdoutTruncated: false, stderrTruncated: false, durationMs: 1 },
    runtimeFingerprint,
    containerImageId: null,
  });
}

/** Claims a task and publishes a (fake-SHA, real DB) checkpoint for it -- the prerequisite every transfer needs. */
async function setUpCheckpointedTask(taskId: string, ownerId: string, leaseMs = 120_000) {
  const store = makeTaskStoreLive(pool);
  const claim = await Effect.runPromise(store.claimTask(taskId, ownerId, leaseMs));
  const manifest = await buildFakeManifest(taskId, claim.epoch);
  const checkpointDigest = computeManifestDigest(manifest);
  await Effect.runPromise(
    store.recordGitCheckpoint({
      taskId,
      ownerId,
      epoch: claim.epoch,
      commandId: `git-checkpoint-${taskId}`,
      attemptId: "attempt-1",
      manifestDigest: checkpointDigest,
      manifestJson: manifestToStableJson(manifest),
    }),
  );
  return { claim, checkpointDigest, resultCommit: manifest.resultCommit, resultTree: manifest.resultTree };
}

async function rowCounts(taskId: string) {
  const [[receipts]] = await Promise.all([pool.query<(RowDataPacket & { c: number })[]>(`SELECT COUNT(*) AS c FROM command_receipts WHERE task_id = ?`, [taskId])]);
  const [handoffs] = await pool.query<(RowDataPacket & { c: number })[]>(`SELECT COUNT(*) AS c FROM task_handoffs WHERE task_id = ?`, [taskId]);
  const [events] = await pool.query<(RowDataPacket & { c: number; }) []>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ?`, [taskId]);
  const [tasks] = await pool.query<RowDataPacket[]>(`SELECT owner_id, epoch FROM tasks WHERE task_id = ?`, [taskId]);
  return { receipts: Number(receipts[0]?.c ?? 0), handoffs: Number(handoffs[0]?.c ?? 0), events: Number(events[0]?.c ?? 0), owner: tasks[0]?.owner_id, epoch: tasks[0]?.epoch };
}

// ---------------------------------------------------------------------
// Section 1: transferOwnership lease validity + the receipt-lookup race
// ---------------------------------------------------------------------

test("transferOwnership: a valid current owner transfers successfully", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-valid-transfer");
  const ownerA = "worker-a";
  const cp = await setUpCheckpointedTask(taskId, ownerA);

  const result = await Effect.runPromise(
    store.transferOwnership({
      taskId,
      handoffId: handoffCommandId(taskId, "h1"),
      fromOwnerId: ownerA,
      fromEpoch: cp.claim.epoch,
      toOwnerId: "worker-b",
      checkpointDigest: cp.checkpointDigest,
      artifactDigest: FAKE_ARTIFACT_DIGEST_A,
      leaseMs: 120_000,
    }),
  );
  assert.equal(result.replay, false);
  assert.equal(result.toEpoch, cp.claim.epoch + 1);
  const after = await rowCounts(taskId);
  assert.equal(after.owner, "worker-b");
  assert.equal(after.epoch, result.toEpoch);
});

test("transferOwnership: an expired owner cannot transfer even when nobody has taken over", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-expired-owner-transfer");
  const ownerA = "worker-a";
  const cp = await setUpCheckpointedTask(taskId, ownerA, 1_000); // MIN_LEASE_MS -- expires fast

  await new Promise((r) => setTimeout(r, 1_200)); // let the lease actually expire, nobody takes over

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.transferOwnership({
          taskId,
          handoffId: handoffCommandId(taskId, "h1"),
          fromOwnerId: ownerA,
          fromEpoch: cp.claim.epoch,
          toOwnerId: "worker-b",
          checkpointDigest: cp.checkpointDigest,
          artifactDigest: FAKE_ARTIFACT_DIGEST_A,
          leaseMs: 120_000,
        }),
      ),
    /owned by another worker|lease/i,
  );
});

test("transferOwnership: rejection (expired lease) changes no owner, epoch, receipt, handoff, or event rows", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-expired-reject-no-side-effects");
  const ownerA = "worker-a";
  const cp = await setUpCheckpointedTask(taskId, ownerA, 1_000);
  const before = await rowCounts(taskId);
  await new Promise((r) => setTimeout(r, 1_200));

  await assert.rejects(() =>
    Effect.runPromise(
      store.transferOwnership({
        taskId,
        handoffId: handoffCommandId(taskId, "h1"),
        fromOwnerId: ownerA,
        fromEpoch: cp.claim.epoch,
        toOwnerId: "worker-b",
        checkpointDigest: cp.checkpointDigest,
        artifactDigest: FAKE_ARTIFACT_DIGEST_A,
        leaseMs: 120_000,
      }),
    ),
  );

  const after = await rowCounts(taskId);
  assert.equal(after.owner, before.owner, "owner must be unchanged");
  assert.equal(after.epoch, before.epoch, "epoch must be unchanged");
  assert.equal(after.receipts, before.receipts, "no new command receipt");
  assert.equal(after.handoffs, before.handoffs, "no new task_handoffs row");
  assert.equal(after.events, before.events, "no new outbox event");
});

test("transferOwnership: an authorized replay of a committed transfer still returns its original result (lease validity does not apply to replay)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-replay-after-lease-irrelevant");
  const ownerA = "worker-a";
  const cp = await setUpCheckpointedTask(taskId, ownerA, 1_000); // A's OWN lease will expire shortly
  const hId = handoffCommandId(taskId, "h1");

  const first = await Effect.runPromise(
    store.transferOwnership({
      taskId,
      handoffId: hId,
      fromOwnerId: ownerA,
      fromEpoch: cp.claim.epoch,
      toOwnerId: "worker-b",
      checkpointDigest: cp.checkpointDigest,
      artifactDigest: FAKE_ARTIFACT_DIGEST_A,
      leaseMs: 120_000,
    }),
  );
  assert.equal(first.replay, false);

  // A's ORIGINAL lease (the one from setUpCheckpointedTask) would have
  // expired by now even if the transfer had never happened -- replay
  // must still succeed for the ORIGINAL requesting owner regardless.
  await new Promise((r) => setTimeout(r, 1_200));

  const second = await Effect.runPromise(
    store.transferOwnership({
      taskId,
      handoffId: hId,
      fromOwnerId: ownerA,
      fromEpoch: cp.claim.epoch,
      toOwnerId: "worker-b",
      checkpointDigest: cp.checkpointDigest,
      artifactDigest: FAKE_ARTIFACT_DIGEST_A,
      leaseMs: 120_000,
    }),
  );
  assert.equal(second.replay, true);
  assert.equal(second.toEpoch, first.toEpoch);
  const after = await rowCounts(taskId);
  assert.equal(after.handoffs, 1, "exactly one handoff row despite the replay");
});

test("transferOwnership: two CONCURRENT requests for the SAME transfer both resolve per the idempotency contract, producing exactly one transfer (the receipt-lookup-before-lock race)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-concurrent-identical-transfer");
  const ownerA = "worker-a";
  const cp = await setUpCheckpointedTask(taskId, ownerA);
  const hId = handoffCommandId(taskId, "h1");
  const call = () =>
    Effect.runPromise(
      store.transferOwnership({
        taskId,
        handoffId: hId,
        fromOwnerId: ownerA,
        fromEpoch: cp.claim.epoch,
        toOwnerId: "worker-b",
        checkpointDigest: cp.checkpointDigest,
        artifactDigest: FAKE_ARTIFACT_DIGEST_A,
        leaseMs: 120_000,
      }),
    );

  const [r1, r2] = await Promise.all([call(), call()]);
  // Without the receipt-lookup-before-lock fix, the loser of the lock
  // race would incorrectly throw EpochStale instead of resolving as a
  // replay -- both calls here must RESOLVE (neither rejects), with
  // exactly one `replay: false` and one `replay: true`, both reporting
  // the SAME toEpoch.
  assert.equal(r1.toEpoch, r2.toEpoch);
  const replays = [r1.replay, r2.replay].sort();
  assert.deepEqual(replays, [false, true], "exactly one fresh commit and one replay, never two fresh commits and never two rejections");
  const after = await rowCounts(taskId);
  assert.equal(after.handoffs, 1, "exactly one durable handoff row despite two concurrent identical requests");
  assert.equal(after.epoch, r1.toEpoch, "epoch incremented exactly once");
});

test("transferOwnership: two COMPETING transfers (different handoffIds, same fromEpoch) produce exactly one winner", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-competing-transfers-auth");
  const ownerA = "worker-a";
  const cp = await setUpCheckpointedTask(taskId, ownerA);

  const attempt = (handoffSuffix: string, toOwnerId: string) =>
    Effect.runPromise(
      store.transferOwnership({
        taskId,
        handoffId: handoffCommandId(taskId, handoffSuffix),
        fromOwnerId: ownerA,
        fromEpoch: cp.claim.epoch,
        toOwnerId,
        checkpointDigest: cp.checkpointDigest,
        artifactDigest: FAKE_ARTIFACT_DIGEST_A,
        leaseMs: 120_000,
      }),
    );

  const results = await Promise.allSettled([attempt("race-x", "worker-x"), attempt("race-y", "worker-y")]);
  const fulfilled = results.filter((r) => r.status === "fulfilled");
  const rejected = results.filter((r) => r.status === "rejected");
  assert.equal(fulfilled.length, 1);
  assert.equal(rejected.length, 1);

  const after = await rowCounts(taskId);
  assert.equal(after.epoch, cp.claim.epoch + 1, "epoch incremented by exactly one -- never two authoritative owners");
  assert.equal(after.handoffs, 1, "exactly one durable handoff row");
});

// ---------------------------------------------------------------------
// Section 2: recordRestoreAcknowledgement bound to durable handoff identity
// ---------------------------------------------------------------------

async function setUpHandoff(taskId: string, fromOwnerId: string, toOwnerId: string, opts: { leaseMs?: number } = {}) {
  const store = makeTaskStoreLive(pool);
  const cp = await setUpCheckpointedTask(taskId, fromOwnerId);
  const rawHandoffId = "h1";
  const transfer = await Effect.runPromise(
    store.transferOwnership({
      taskId,
      handoffId: handoffCommandId(taskId, rawHandoffId),
      fromOwnerId,
      fromEpoch: cp.claim.epoch,
      toOwnerId,
      checkpointDigest: cp.checkpointDigest,
      artifactDigest: FAKE_ARTIFACT_DIGEST_A,
      leaseMs: opts.leaseMs ?? 120_000,
    }),
  );
  return { cp, transfer, rawHandoffId, resultCommit: cp.resultCommit, resultTree: cp.resultTree };
}

async function outboxCount(taskId: string, type: string): Promise<number> {
  const [rows] = await pool.query<(RowDataPacket & { c: number })[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = ?`, [taskId, type]);
  return Number(rows[0]?.c ?? 0);
}

test("recordRestoreAcknowledgement: unknown handoff is rejected", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-unknown-handoff");
  await setUpCheckpointedTask(taskId, "worker-a"); // no transfer/handoff ever created

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId,
          handoffId: handoffCommandId(taskId, "never-existed"),
          ownerId: "worker-b",
          epoch: 1,
          commandId: uniqueId("ack"),
          artifactDigest: FAKE_ARTIFACT_DIGEST_A,
          restoredCommit: "b".repeat(40),
          restoredTree: "c".repeat(40),
        }),
      ),
    /unknown handoff/i,
  );
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 0);
});

test("recordRestoreAcknowledgement: a handoff belonging to ANOTHER task is rejected", async () => {
  const store = makeTaskStoreLive(pool);
  const taskIdReal = uniqueId("task-ack-real");
  const taskIdOther = uniqueId("task-ack-other");
  const h = await setUpHandoff(taskIdReal, "worker-a", "worker-b");
  await setUpCheckpointedTask(taskIdOther, "worker-a");

  // Attempt to acknowledge taskIdReal's handoff ID, but scoped under taskIdOther.
  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId: taskIdOther,
          handoffId: handoffCommandId(taskIdReal, h.rawHandoffId),
          ownerId: "worker-b",
          epoch: h.transfer.toEpoch,
          commandId: uniqueId("ack"),
          artifactDigest: FAKE_ARTIFACT_DIGEST_A,
          restoredCommit: h.resultCommit,
          restoredTree: h.resultTree,
        }),
      ),
    /unknown handoff/i,
  );
  assert.equal(await outboxCount(taskIdOther, "RestoreAcknowledged"), 0);
});

test("recordRestoreAcknowledgement: wrong destination owner is rejected", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-wrong-owner");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b");

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId,
          handoffId: handoffCommandId(taskId, h.rawHandoffId),
          ownerId: "worker-NOT-b",
          epoch: h.transfer.toEpoch,
          commandId: uniqueId("ack"),
          artifactDigest: FAKE_ARTIFACT_DIGEST_A,
          restoredCommit: h.resultCommit,
          restoredTree: h.resultTree,
        }),
      ),
    /destination owner/i,
  );
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 0);
});

test("recordRestoreAcknowledgement: wrong destination epoch is rejected", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-wrong-epoch");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b");

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId,
          handoffId: handoffCommandId(taskId, h.rawHandoffId),
          ownerId: "worker-b",
          epoch: h.transfer.toEpoch + 1,
          commandId: uniqueId("ack"),
          artifactDigest: FAKE_ARTIFACT_DIGEST_A,
          restoredCommit: h.resultCommit,
          restoredTree: h.resultTree,
        }),
      ),
    /destination epoch/i,
  );
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 0);
});

test("recordRestoreAcknowledgement: wrong artifact digest is rejected", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-wrong-artifact");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b");

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId,
          handoffId: handoffCommandId(taskId, h.rawHandoffId),
          ownerId: "worker-b",
          epoch: h.transfer.toEpoch,
          commandId: uniqueId("ack"),
          artifactDigest: FAKE_ARTIFACT_DIGEST_B, // the WRONG one -- the handoff authorized digest A
          restoredCommit: h.resultCommit,
          restoredTree: h.resultTree,
        }),
      ),
    /artifactDigest/i,
  );
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 0);
});

test("recordRestoreAcknowledgement: wrong restored commit/tree is rejected", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-wrong-commit-tree");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b");

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId,
          handoffId: handoffCommandId(taskId, h.rawHandoffId),
          ownerId: "worker-b",
          epoch: h.transfer.toEpoch,
          commandId: uniqueId("ack"),
          artifactDigest: FAKE_ARTIFACT_DIGEST_A,
          restoredCommit: "f".repeat(40), // does not match the checkpoint's real result commit
          restoredTree: h.resultTree,
        }),
      ),
    /do not match the checkpoint/i,
  );
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 0);
});

test("recordRestoreAcknowledgement: a valid acknowledgement succeeds and publishes exactly one event", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-valid");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b");
  const commandId = uniqueId("ack");

  const receipt = await Effect.runPromise(
    store.recordRestoreAcknowledgement({
      taskId,
      handoffId: handoffCommandId(taskId, h.rawHandoffId),
      ownerId: "worker-b",
      epoch: h.transfer.toEpoch,
      commandId,
      artifactDigest: FAKE_ARTIFACT_DIGEST_A,
      restoredCommit: h.resultCommit,
      restoredTree: h.resultTree,
    }),
  );
  assert.equal(receipt.replay, false);
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 1);

  const task = await Effect.runPromise(store.getTask(taskId));
  assert.equal(task.checkpointDigest, h.cp.checkpointDigest, "provenance preserved -- the ORIGINAL checkpoint digest, never rewritten to pretend B created it");
});

test("recordRestoreAcknowledgement: duplicate form 1 -- SAME commandId, SAME payload -- replays without a new event", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-dup-same-payload");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b");
  const commandId = uniqueId("ack");
  const args = {
    taskId,
    handoffId: handoffCommandId(taskId, h.rawHandoffId),
    ownerId: "worker-b",
    epoch: h.transfer.toEpoch,
    commandId,
    artifactDigest: FAKE_ARTIFACT_DIGEST_A,
    restoredCommit: h.resultCommit,
    restoredTree: h.resultTree,
  };

  const first = await Effect.runPromise(store.recordRestoreAcknowledgement(args));
  assert.equal(first.replay, false);
  const second = await Effect.runPromise(store.recordRestoreAcknowledgement(args));
  assert.equal(second.replay, true);
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 1, "no second event from an identical replay");
});

test("recordRestoreAcknowledgement: duplicate form 2 -- SAME commandId, CHANGED payload -- rejected as a conflict", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-dup-changed-payload");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b");
  const commandId = uniqueId("ack");

  await Effect.runPromise(
    store.recordRestoreAcknowledgement({
      taskId,
      handoffId: handoffCommandId(taskId, h.rawHandoffId),
      ownerId: "worker-b",
      epoch: h.transfer.toEpoch,
      commandId,
      artifactDigest: FAKE_ARTIFACT_DIGEST_A,
      restoredCommit: h.resultCommit,
      restoredTree: h.resultTree,
    }),
  );

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId,
          handoffId: handoffCommandId(taskId, h.rawHandoffId),
          ownerId: "worker-b",
          epoch: h.transfer.toEpoch,
          commandId, // SAME commandId
          artifactDigest: FAKE_ARTIFACT_DIGEST_A,
          restoredCommit: h.resultCommit,
          restoredTree: "d".repeat(40), // CHANGED payload
        }),
      ),
    /already used with a different payload/i,
  );
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 1);
});

test("recordRestoreAcknowledgement: duplicate form 3 -- SAME handoff acknowledged through a DIFFERENT commandId -- rejected, no second completion event", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-dup-different-commandid");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b");

  const first = await Effect.runPromise(
    store.recordRestoreAcknowledgement({
      taskId,
      handoffId: handoffCommandId(taskId, h.rawHandoffId),
      ownerId: "worker-b",
      epoch: h.transfer.toEpoch,
      commandId: uniqueId("ack-first"),
      artifactDigest: FAKE_ARTIFACT_DIGEST_A,
      restoredCommit: h.resultCommit,
      restoredTree: h.resultTree,
    }),
  );
  assert.equal(first.replay, false);

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId,
          handoffId: handoffCommandId(taskId, h.rawHandoffId),
          ownerId: "worker-b",
          epoch: h.transfer.toEpoch,
          commandId: uniqueId("ack-second"), // a GENUINELY NEW commandId, never seen before
          artifactDigest: FAKE_ARTIFACT_DIGEST_A,
          restoredCommit: h.resultCommit,
          restoredTree: h.resultTree,
        }),
      ),
    /already acknowledged via a different commandId/i,
  );
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 1, "exactly one completion event -- the second commandId must not create a second one");
});

test("recordRestoreAcknowledgement: a NEW acknowledgement after destination authority has EXPIRED is rejected", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-authority-expired");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b", { leaseMs: 1_000 });

  await new Promise((r) => setTimeout(r, 1_200));

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId,
          handoffId: handoffCommandId(taskId, h.rawHandoffId),
          ownerId: "worker-b",
          epoch: h.transfer.toEpoch,
          commandId: uniqueId("ack"),
          artifactDigest: FAKE_ARTIFACT_DIGEST_A,
          restoredCommit: h.resultCommit,
          restoredTree: h.resultTree,
        }),
      ),
    /owned by another worker|lease/i,
  );
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 0);
});

test("recordRestoreAcknowledgement: a NEW acknowledgement after destination authority has CHANGED (B's lease expired, C took over) is rejected", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-authority-changed");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b", { leaseMs: 1_000 });
  await new Promise((r) => setTimeout(r, 1_200));
  const claimC = await Effect.runPromise(store.claimTask(taskId, "worker-c", 120_000));
  assert.notEqual(claimC.epoch, h.transfer.toEpoch);

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId,
          handoffId: handoffCommandId(taskId, h.rawHandoffId),
          ownerId: "worker-b", // B no longer owns the task
          epoch: h.transfer.toEpoch,
          commandId: uniqueId("ack"),
          artifactDigest: FAKE_ARTIFACT_DIGEST_A,
          restoredCommit: h.resultCommit,
          restoredTree: h.resultTree,
        }),
      ),
    /stale/i,
  );
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 0);
});

test("recordRestoreAcknowledgement: historical replay still succeeds after ownership has since changed", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-replay-after-ownership-change");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b", { leaseMs: 1_000 });
  const commandId = uniqueId("ack");
  const args = {
    taskId,
    handoffId: handoffCommandId(taskId, h.rawHandoffId),
    ownerId: "worker-b",
    epoch: h.transfer.toEpoch,
    commandId,
    artifactDigest: FAKE_ARTIFACT_DIGEST_A,
    restoredCommit: h.resultCommit,
    restoredTree: h.resultTree,
  };

  const first = await Effect.runPromise(store.recordRestoreAcknowledgement(args));
  assert.equal(first.replay, false);

  // B's lease expires, C takes over.
  await new Promise((r) => setTimeout(r, 1_200));
  const claimC = await Effect.runPromise(store.claimTask(taskId, "worker-c", 120_000));
  assert.notEqual(claimC.epoch, h.transfer.toEpoch);

  // B retries its OWN already-committed acknowledgement (same commandId) --
  // must still succeed as a replay despite no longer holding authority.
  const second = await Effect.runPromise(store.recordRestoreAcknowledgement(args));
  assert.equal(second.replay, true);
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 1);
});

// ---------------------------------------------------------------------
// Phase 2B.2, Part 1: the generic applyTaskCommand route must not be
// usable to bypass recordRestoreAcknowledgement's validation.
// ---------------------------------------------------------------------

test("Phase 2B.2: CONFIRMED bypass closed -- applyTaskCommand refuses a RestoreAcknowledged command outright, even with a valid current owner/epoch/lease and an INVALID (unvalidated-against-the-handoff) payload", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-bypass-closed");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b");

  const before = await rowCounts(taskId);
  await assert.rejects(
    () =>
      Effect.runPromise(
        store.applyTaskCommand({
          taskId,
          ownerId: "worker-b", // the VALID current owner
          epoch: h.transfer.toEpoch, // the VALID current epoch
          commandId: uniqueId("bypass-attempt"),
          command: {
            type: "RestoreAcknowledged",
            handoffId: h.rawHandoffId,
            artifactDigest: "ff".repeat(32), // arbitrary -- never checked against the handoff if this path worked
            restoredCommit: "e".repeat(40), // arbitrary, does NOT match the real checkpoint
            restoredTree: "f".repeat(40),
          },
        }),
      ),
    /must be submitted through TaskStore\.recordRestoreAcknowledgement/i,
  );

  const after = await rowCounts(taskId);
  assert.equal(after.receipts, before.receipts, "no new receipt from the refused generic call");
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 0, "no RestoreAcknowledged event -- the bypass must not be able to emit a success event with unvalidated content");

  // The SUPPORTED valid path (through the specialized method) must
  // still work for the exact same handoff, proving this is a routing
  // refusal, not a general breakage.
  const valid = await Effect.runPromise(
    store.recordRestoreAcknowledgement({
      taskId,
      handoffId: handoffCommandId(taskId, h.rawHandoffId),
      ownerId: "worker-b",
      epoch: h.transfer.toEpoch,
      commandId: uniqueId("valid-ack"),
      artifactDigest: FAKE_ARTIFACT_DIGEST_A,
      restoredCommit: h.resultCommit,
      restoredTree: h.resultTree,
    }),
  );
  assert.equal(valid.replay, false);
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 1);
});

test("Phase 2B.2: a DIFFERENT commandId acknowledging an already-acknowledged handoff is rejected EVEN WHEN its artifact/commit/tree content is IDENTICAL to the original -- never silently accepted because the content happens to match", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-ack-dup-different-id-identical-content");
  const h = await setUpHandoff(taskId, "worker-a", "worker-b");

  const first = await Effect.runPromise(
    store.recordRestoreAcknowledgement({
      taskId,
      handoffId: handoffCommandId(taskId, h.rawHandoffId),
      ownerId: "worker-b",
      epoch: h.transfer.toEpoch,
      commandId: uniqueId("ack-first"),
      artifactDigest: FAKE_ARTIFACT_DIGEST_A,
      restoredCommit: h.resultCommit,
      restoredTree: h.resultTree,
    }),
  );
  assert.equal(first.replay, false);

  // A SECOND, never-before-seen commandId, with EXACTLY the same
  // artifactDigest/restoredCommit/restoredTree as the first -- content
  // equality must NOT be treated as grounds for silent acceptance; the
  // single-completion-event guarantee is keyed to ONE commandId per
  // handoff, not to content equality.
  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordRestoreAcknowledgement({
          taskId,
          handoffId: handoffCommandId(taskId, h.rawHandoffId),
          ownerId: "worker-b",
          epoch: h.transfer.toEpoch,
          commandId: uniqueId("ack-second-identical-content"),
          artifactDigest: FAKE_ARTIFACT_DIGEST_A, // IDENTICAL to the first call
          restoredCommit: h.resultCommit, // IDENTICAL
          restoredTree: h.resultTree, // IDENTICAL
        }),
      ),
    /already acknowledged via a different commandId/i,
  );
  assert.equal(await outboxCount(taskId, "RestoreAcknowledged"), 1, "still exactly one completion event -- identical content under a new commandId must not create a second one");
});
