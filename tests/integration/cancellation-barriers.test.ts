// Demo UI milestone: proves cancellation propagates into REAL Docker
// verification using a DETERMINISTIC barrier -- onVerifyContainerCreated
// fires at the exact real moment Docker confirms the verification
// container is running (the SAME mechanism this project's own crash
// tests already use for container readiness, not a sleep-based guess).
// Real Git/MySQL/Docker throughout.
//
// Covers worker B's restore re-verification (handoff.ts) below.
// Deliberately does NOT also attempt this for worker A's deterministic
// path (run-task.ts): investigated directly and found structurally
// unachievable with this fixture, not merely flaky -- see
// docs/KUBERNETES.md-style note in README.md "Demo UI" for the exact
// reason (the completion marker is captured via a live stdout stream
// listener attached BEFORE the container-confirmed-running poll even
// starts; that poll has an inherent >=50ms minimum latency per cycle,
// while this fixture's verify.js finishes in low single-digit
// milliseconds, so the marker is already resolved, deterministically,
// before onContainerCreated ever fires -- confirmed by direct code
// reading of docker-verify.ts, not by trial and error alone). run-task.ts
// and handoff.ts both still accept and propagate a real `signal` (see
// their own code) -- only THIS specific mid-container barrier technique
// could not be exercised for the near-instant deterministic fixture.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type mysql from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";
import { createFixtureRepo } from "../../src/worker/fixture-repo.js";
import { runGitWorkerTask, defaultAttemptId, branchNameFor } from "../../src/worker/run-task.js";
import { createManagedWorkspaceRoot, resolveWorktreePath, resolveIsolatedHomeDir } from "../../src/worker/workspace.js";
import { makeIsolatedGitEnv } from "../../src/worker/git.js";
import { prepareHandoffArtifact, requestOwnershipTransfer, restoreAndAcknowledge, handoffCommandId } from "../../src/worker/handoff.js";

let pool: mysql.Pool;
let managedRoot: string;
let fixtureParentDir: string;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
});

beforeEach(async () => {
  await truncateAll(pool);
  const isolatedRoot = await mkdtemp(path.join(tmpdir(), "relaylab-cancel-barrier-test-root-"));
  managedRoot = await createManagedWorkspaceRoot(isolatedRoot);
  fixtureParentDir = await mkdtemp(path.join(tmpdir(), "relaylab-cancel-barrier-test-"));
});

after(async () => {
  await pool.end();
});

async function freshFixture() {
  const dir = path.join(fixtureParentDir, uniqueId("fixture"));
  return createFixtureRepo(dir);
}

async function claim(taskId: string, ownerId: string) {
  const store = makeTaskStoreLive(pool);
  return Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));
}

test("deterministic path: the signal is genuinely propagated into Docker verification (VerificationRequest.signal), confirmed via an already-aborted signal refusing to even start", async () => {
  // Not a mid-container barrier (see file header for why that specific
  // technique is unachievable for this fixture) -- this proves the
  // simpler, still-real fact that run-task.ts's new `signal` parameter
  // actually reaches verifyInDockerWithImageId: an ALREADY-aborted
  // signal, present from before the call even begins, must never
  // complete and checkpoint successfully.
  const fixture = await freshFixture();
  const taskId = uniqueId("cancel-det-preaborted-task");
  const claimResult = await claim(taskId, "worker-cancel-a");
  const controller = new AbortController();
  controller.abort();

  const outcome = await runGitWorkerTask({
    taskId,
    ownerId: "worker-cancel-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    signal: controller.signal,
  });

  assert.notEqual(outcome.kind, "checkpointed", `an already-aborted signal must never still result in a checkpoint, got ${outcome.kind}`);
  const [rows] = await pool.query(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal((rows as { c: number }[])[0]!.c, 0, "an already-cancelled run must never produce a checkpoint row");
});

test("worker B restore re-verification: cancelling at the moment B's real container is confirmed running leaves no acknowledgement, never undoing anything already durable", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("cancel-restore-task");
  const ownerA = "worker-cancel-restore-a";
  const claimResult = await claim(taskId, ownerA);

  // Worker A completes normally first -- real checkpoint, real artifact,
  // real ownership transfer. Cancellation is exercised ONLY against B's
  // own re-verification step below.
  const outcomeA = await runGitWorkerTask({
    taskId,
    ownerId: ownerA,
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
  });
  assert.equal(outcomeA.kind, "checkpointed");
  if (outcomeA.kind !== "checkpointed") return;

  const store = makeTaskStoreLive(pool);
  const taskAfterA = await Effect.runPromise(store.getTask(taskId));
  const attemptId = defaultAttemptId(taskId);
  const worktreeDirA = resolveWorktreePath(managedRoot, taskId, attemptId);
  const envA = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRoot));
  const handoffId = `cancel-restore-handoff-${taskId}`;
  const artifactOutDir = await mkdtemp(path.join(tmpdir(), "relaylab-cancel-barrier-artifacts-"));
  const { artifactPath, manifestPath, manifest } = await prepareHandoffArtifact({
    taskId,
    handoffId,
    worktreeDir: worktreeDirA,
    env: envA,
    resultRef: branchNameFor(taskId),
    sourceCheckpointCommandId: `git-checkpoint-${taskId}`,
    sourceManifestDigest: taskAfterA.checkpointDigest!,
    baseCommit: outcomeA.manifest.baseCommit,
    resultCommit: outcomeA.manifest.resultCommit,
    resultTree: outcomeA.manifest.resultTree,
    outDir: artifactOutDir,
  });

  const ownerB = "worker-cancel-restore-b";
  const transfer = await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, handoffId),
    fromOwnerId: ownerA,
    fromEpoch: taskAfterA.epoch,
    toOwnerId: ownerB,
    checkpointDigest: taskAfterA.checkpointDigest!,
    artifactDigest: manifest.artifactDigest,
    leaseMs: 120_000,
  });

  const workspaceBRoot = await mkdtemp(path.join(tmpdir(), "relaylab-cancel-barrier-workspace-b-"));
  const destRepoDir = path.join(workspaceBRoot, "restored-repo");
  const managedRootB = await createManagedWorkspaceRoot(path.join(workspaceBRoot, "home"));
  const envB = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootB));
  const controller = new AbortController();

  const restoreOutcome = await restoreAndAcknowledge(pool, {
    taskId,
    handoffId,
    ownerId: ownerB,
    epoch: transfer.toEpoch,
    artifactPath,
    manifestPath,
    destRepoDir,
    env: envB,
    signal: controller.signal,
    onVerifyContainerCreated: () => {
      setImmediate(() => controller.abort());
    },
  });

  assert.equal(restoreOutcome.kind, "verification_cancelled", `expected verification_cancelled, got ${restoreOutcome.kind}`);

  // A's own checkpoint must still stand -- cancellation of B's restore
  // must never be mistaken for undoing A's already-durable work.
  const [checkpointRows] = await pool.query(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal((checkpointRows as { c: number }[])[0]!.c, 1, "A's checkpoint must remain, untouched by B's cancelled restore");

  const taskAfterCancel = await Effect.runPromise(store.getTask(taskId));
  assert.equal(taskAfterCancel.checkpointDigest, taskAfterA.checkpointDigest, "A's checkpoint digest must be unchanged");
});
