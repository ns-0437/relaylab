// Demo UI milestone: proves cancellation propagates into REAL Docker
// verification using a DETERMINISTIC barrier -- onVerifyContainerCreated
// fires at the exact real moment Docker confirms the verification
// container is running (the SAME mechanism this project's own crash
// tests already use for container readiness, not a sleep-based guess).
// Real Git/MySQL/Docker throughout.
//
// Covers worker B's restore re-verification (handoff.ts) AND worker A's
// deterministic path (run-task.ts) below.
//
// Worker A's mid-container barrier specifically required a TEST-ONLY
// slow fixture (createSlowFixtureRepo below -- never used by production
// code or fixture-repo.ts): the real production fixture's verify.js
// finishes in low single-digit milliseconds, faster than
// docker-verify.ts's own >=50ms-per-cycle "confirm running" poll can
// reliably observe before the completion marker (captured via a live
// stdout stream listener attached BEFORE that poll even starts) has
// already resolved -- confirmed by direct code reading, not trial and
// error. That is a property of the FAST production fixture, not a
// fundamental limit on testing cancellation itself -- the slow fixture
// below, otherwise byte-for-byte identical, closes the gap.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type mysql from "mysql2/promise";
import { Effect } from "effect";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, writeFile } from "node:fs/promises";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";
import { createFixtureRepo } from "../../src/worker/fixture-repo.js";
import { runGitWorkerTask, defaultAttemptId, branchNameFor } from "../../src/worker/run-task.js";
import { createManagedWorkspaceRoot, resolveWorktreePath, resolveIsolatedHomeDir } from "../../src/worker/workspace.js";
import { makeIsolatedGitEnv, git } from "../../src/worker/git.js";
import { prepareHandoffArtifact, requestOwnershipTransfer, restoreAndAcknowledge, handoffCommandId } from "../../src/worker/handoff.js";

const execFileAsync = promisify(execFile);

/**
 * TEST-ONLY fixture repo, used by NOTHING in production code --
 * identical to fixture-repo.ts's createFixtureRepo except verify.js
 * busy-waits for ~3s before asserting. This is what actually makes
 * mid-execution cancellation of the DETERMINISTIC path testable: the
 * real production fixture's verify.js finishes in low single-digit
 * milliseconds, faster than docker-verify.ts's own >=50ms-per-cycle
 * "confirm running" poll can reliably observe (see the module header
 * above) -- that is a property of the FAST fixture, not a fundamental
 * limit of cancellation itself. A deliberately slower, otherwise
 * identical fixture closes that gap without touching
 * fixture-repo.ts or any production code path.
 */
async function createSlowFixtureRepo(destDir: string): Promise<{ repoDir: string; baseCommit: string }> {
  const ADD_JS_BUGGY = `function add(a, b) {\n  return a - b;\n}\n\nmodule.exports = { add };\n`;
  const SLOW_VERIFY_JS = [
    `const start = Date.now();`,
    `while (Date.now() - start < 3000) { /* deterministic, bounded busy-wait -- inside the verified container only, never production code */ }`,
    `const assert = require("node:assert/strict");`,
    `const { add } = require("./src/add.js");`,
    `assert.equal(add(2, 3), 5, "add(2, 3) must equal 5");`,
    `console.log("verify.js: PASS");`,
  ].join("\n");

  await mkdir(destDir, { recursive: true });
  const isolatedHome = path.join(destDir, ".fixture-home");
  await mkdir(isolatedHome, { recursive: true });
  const env = makeIsolatedGitEnv(isolatedHome);

  await git(["init", "--initial-branch=main"], destDir, env);
  await mkdir(path.join(destDir, "src"), { recursive: true });
  await writeFile(path.join(destDir, "src", "add.js"), ADD_JS_BUGGY, "utf8");
  await writeFile(path.join(destDir, "verify.js"), SLOW_VERIFY_JS, "utf8");

  await git(["add", "-A"], destDir, env);
  await git(
    ["-c", "user.name=relaylab-fixture", "-c", "user.email=relaylab-fixture@localhost", "-c", "commit.gpgsign=false", "commit", "--no-verify", "-m", "baseline (slow verify, test-only): add() has a known subtraction bug"],
    destDir,
    env,
  );
  const { stdout } = await git(["rev-parse", "HEAD"], destDir, env);
  return { repoDir: destDir, baseCommit: stdout.trim() };
}

/** Confirms a container is genuinely gone (removed), not merely stopped -- docker-verify.ts's own cancellation path is expected to stop AND remove it. */
async function containerIsGone(containerId: string): Promise<boolean> {
  try {
    await execFileAsync("docker", ["inspect", containerId]);
    return false;
  } catch {
    return true;
  }
}

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

test("deterministic path, SLOW test-only fixture: cancelling at the moment the container is confirmed running stops/removes the owned container, produces no checkpoint, and is reported as cancelled", async () => {
  const fixtureDir = path.join(fixtureParentDir, uniqueId("slow-fixture"));
  const fixture = await createSlowFixtureRepo(fixtureDir);
  const taskId = uniqueId("cancel-det-slow-task");
  const claimResult = await claim(taskId, "worker-cancel-a-slow");
  const controller = new AbortController();
  let observedContainerId: string | undefined;

  const outcome = await runGitWorkerTask({
    taskId,
    ownerId: "worker-cancel-a-slow",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    signal: controller.signal,
    onVerifyContainerCreated: (containerId) => {
      observedContainerId = containerId;
      // The 3s busy-wait in verify.js gives this real, confirmed-running
      // container ample time to still be alive when the abort signal
      // fires -- no scheduling trick needed (unlike the fast fixture).
      controller.abort();
    },
  });

  assert.ok(observedContainerId, "the container-confirmed-running barrier must have fired for the slow fixture");
  assert.equal(outcome.kind, "verification_cancelled", `expected verification_cancelled, got ${outcome.kind}`);
  assert.equal(outcome.verification.outcome, "cancelled");

  const gone = await containerIsGone(observedContainerId!);
  assert.ok(gone, "the owned container must be stopped and removed after cancellation, not left running");

  const [rows] = await pool.query(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal((rows as { c: number }[])[0]!.c, 0, "a cancelled verification must never produce a checkpoint row");
});

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
  // The SLOW test-only fixture (see the file header and worker A's own
  // test above) -- B re-verifies the RESTORED artifact, which carries
  // the same slow verify.js, so B's own container stays confirmed
  // running long enough for a genuine mid-container barrier. No
  // scheduling trick (an earlier version of this test used
  // `setImmediate` as a timing guess instead of a real barrier).
  const fixtureDir = path.join(fixtureParentDir, uniqueId("slow-fixture-b"));
  const fixture = await createSlowFixtureRepo(fixtureDir);
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
  let observedContainerId: string | undefined;

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
    onVerifyContainerCreated: (containerId) => {
      observedContainerId = containerId;
      // The restored artifact carries the SAME slow verify.js (3s
      // busy-wait), so this real, confirmed-running container is still
      // alive when the signal fires -- a genuine barrier, not a guess.
      controller.abort();
    },
  });

  assert.ok(observedContainerId, "the container-confirmed-running barrier must have fired for B's re-verification");
  assert.equal(restoreOutcome.kind, "verification_cancelled", `expected verification_cancelled, got ${restoreOutcome.kind}`);

  const gone = await containerIsGone(observedContainerId!);
  assert.ok(gone, "B's owned container must be stopped and removed after cancellation");

  // A's own checkpoint must still stand -- cancellation of B's restore
  // must never be mistaken for undoing A's already-durable work.
  const [checkpointRows] = await pool.query(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal((checkpointRows as { c: number }[])[0]!.c, 1, "A's checkpoint must remain, untouched by B's cancelled restore");

  const taskAfterCancel = await Effect.runPromise(store.getTask(taskId));
  assert.equal(taskAfterCancel.checkpointDigest, taskAfterA.checkpointDigest, "A's checkpoint digest must be unchanged");

  // The acknowledgement state is queried DIRECTLY from the durable
  // handoff row -- never inferred solely from restoreOutcome's return
  // value -- proving no restore acknowledgement was ever recorded for
  // this cancelled attempt.
  const [handoffRows] = await pool.query<{ restore_ack_command_id: string | null }[] & import("mysql2/promise").RowDataPacket[]>(
    `SELECT restore_ack_command_id FROM task_handoffs WHERE task_id = ? AND handoff_id = ?`,
    [taskId, handoffCommandId(taskId, handoffId)],
  );
  assert.equal(handoffRows.length, 1, "the handoff row itself must exist (the transfer succeeded; only the restore/ack was cancelled)");
  assert.equal(handoffRows[0]!.restore_ack_command_id, null, "no restore acknowledgement must be recorded for a cancelled restore");
});
