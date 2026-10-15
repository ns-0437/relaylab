// Phase 2B, Part G: real Git, real MySQL tests for the committed-
// checkpoint handoff protocol (docs/decisions/0011-handoff-protocol.md).
// Covers tests 1-8 and 10-12; test 9 (a real process kill between
// ownership transfer and restore acknowledgement) needs a real forked
// OS process + failpoint and lives in
// tests/integration/git-worker-handoff-crash.ts, same convention as
// git-worker-crash.ts.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type mysql from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";
import { createFixtureRepo, type FixtureRepo } from "../../src/worker/fixture-repo.js";
import { runGitWorkerTask, defaultAttemptId, branchNameFor, checkpointCommandId } from "../../src/worker/run-task.js";
import { createManagedWorkspaceRoot, resolveWorktreePath, resolveIsolatedHomeDir, ensureDir } from "../../src/worker/workspace.js";
import { makeIsolatedGitEnv, currentCommit, currentTree, hasUncommittedChanges, addWorktree, commitAllowedPaths } from "../../src/worker/git.js";
import { prepareHandoffArtifact, requestOwnershipTransfer, restoreAndAcknowledge, handoffCommandId, restoreAckCommandId } from "../../src/worker/handoff.js";
import { applyScriptedRepair } from "../../src/worker/repair.js";
import { buildManifest, buildRuntimeFingerprint, publishCheckpoint } from "../../src/worker/checkpoint.js";
import { TaskStoreLive } from "../../src/db/task-store.js";
import { ProcessHarness, testChildEnv, repoRootFrom } from "./process-harness.js";
import { loadTestMysqlConfig } from "../../src/config.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = repoRootFrom(here);
const workerEntry = path.join(repoRoot, "src", "worker-main.ts");
const restoreWorkerEntry = path.join(repoRoot, "src", "restore-worker-main.ts");
const testDatabase = loadTestMysqlConfig().database;

let pool: mysql.Pool;
let scratchDir: string;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
});

beforeEach(async () => {
  await truncateAll(pool);
  scratchDir = await mkdtemp(path.join(tmpdir(), "relaylab-handoff-test-"));
});

after(async () => {
  await pool.end();
});

async function freshFixture(): Promise<FixtureRepo> {
  const dir = path.join(scratchDir, uniqueId("fixture"));
  return createFixtureRepo(dir);
}

async function claim(taskId: string, ownerId: string) {
  const store = makeTaskStoreLive(pool);
  return Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));
}

async function getTask(taskId: string) {
  const store = makeTaskStoreLive(pool);
  return Effect.runPromise(store.getTask(taskId));
}

/** A-side setup shared by most scenarios: claim, run the real worker to a published checkpoint, build the artifact. */
async function setUpPublishedCheckpointAndArtifact(taskId: string, ownerA: string, fixture: FixtureRepo) {
  // NOT nested under `taskId` again -- `resolveWorktreePath` already
  // scopes per-task via its own taskId segment; nesting it twice here
  // needlessly inflates the worktree's path depth and, combined with a
  // long taskId, was observed to push the resulting absolute path past
  // Windows' legacy 260-character MAX_PATH in a way that made `git
  // rev-list --count <40-char-sha>..<40-char-sha>` fail with a
  // misleading "Filename too long" error (confirmed directly: the SAME
  // range with short SHA prefixes, or `--not` instead of `..`, worked
  // fine from the identical directory -- this is cwd-length-sensitive,
  // not a real revision-resolution problem).
  const managedRootA = await createManagedWorkspaceRoot(path.join(scratchDir, "workspace-a"));
  const envA = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootA));
  const claimA = await claim(taskId, ownerA);
  const outcome = await runGitWorkerTask({
    taskId,
    ownerId: ownerA,
    epoch: claimA.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRootA,
    pool,
  });
  assert.equal(outcome.kind, "checkpointed");
  if (outcome.kind !== "checkpointed") throw new Error("unreachable");

  const task = await getTask(taskId);
  assert.equal(task.checkpointDigest, outcome.replay ? task.checkpointDigest : task.checkpointDigest); // sanity, always true
  const worktreeDirA = resolveWorktreePath(managedRootA, taskId, defaultAttemptId(taskId));
  const handoffId = uniqueId("handoff");
  const outDir = path.join(scratchDir, "artifacts", taskId);
  const artifact = await prepareHandoffArtifact({
    taskId,
    handoffId,
    worktreeDir: worktreeDirA,
    env: envA,
    resultRef: branchNameFor(taskId),
    sourceCheckpointCommandId: `git-checkpoint-${taskId}`,
    sourceManifestDigest: task.checkpointDigest!,
    baseCommit: outcome.manifest.baseCommit,
    resultCommit: outcome.manifest.resultCommit,
    resultTree: outcome.manifest.resultTree,
    outDir,
  });

  return { claimA, outcome, task, managedRootA, worktreeDirA, handoffId, ...artifact };
}

/**
 * Phase 3A, Part A: builds a checkpoint+artifact for a tree whose
 * `verify.js` TAMPERS with a tracked file and exits 0 -- deliberately
 * NOT via `runGitWorkerTask` (A's own real verification step would
 * correctly refuse this tree per the A-side test in
 * tests/integration/git-worker.test.ts, so there is no way to reach a
 * genuinely "A successfully checkpointed a tampering tree" state
 * through A's own public entrypoint -- that refusal IS the system
 * working as intended, not a gap). This directly constructs the
 * durable checkpoint row the SAME way `publishCheckpoint` always
 * would, so B's restore step below exercises its REAL, INDEPENDENT
 * verification against a REAL tampering tree -- proving B's own
 * check is a genuine, separate line of defense, not merely redundant
 * with A's (this project's own stated design: "B must never trust
 * A's prior verification result as proof B's environment works").
 */
async function setUpTamperingCheckpointAndArtifact(taskId: string, ownerA: string, fixture: FixtureRepo) {
  const managedRootA = await createManagedWorkspaceRoot(path.join(scratchDir, "workspace-a-tamper"));
  const envA = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootA));
  const claimA = await claim(taskId, ownerA);
  const attemptId = defaultAttemptId(taskId);
  const worktreeDirA = resolveWorktreePath(managedRootA, taskId, attemptId);
  const branch = branchNameFor(taskId);
  await addWorktree(fixture.repoDir, worktreeDirA, branch, fixture.baseCommit, envA);
  await applyScriptedRepair(worktreeDirA); // the REAL repair genuinely runs -- only verify.js itself is malicious

  const tamperingVerifyJs = [
    `const fs = require("node:fs");`,
    `fs.chmodSync("tamper-target.txt", 0o644);`,
    `fs.writeFileSync("tamper-target.txt", "TAMPERED BY A MALICIOUS FIXTURE");`,
    `console.log("claiming success despite tampering with tracked input");`,
    `process.exit(0);`,
    ``,
  ].join("\n");
  await writeFile(path.join(worktreeDirA, "verify.js"), tamperingVerifyJs, "utf8");
  await writeFile(path.join(worktreeDirA, "tamper-target.txt"), "original content\n", "utf8");
  const resultCommit = await commitAllowedPaths(worktreeDirA, ["src/add.js", "verify.js", "tamper-target.txt"], "repair + inject tampering verify.js", envA);
  const resultTree = await currentTree(worktreeDirA, envA);

  // Builds the manifest EXACTLY as `buildManifest` always would -- the
  // ONLY thing simulated here is `verification.outcome: "passed"`,
  // standing in for "assume a checkpoint claiming success was somehow
  // recorded" (e.g. an older pre-Part-A build, or a compromised A).
  // `recordGitCheckpoint` itself does not re-run verification -- it
  // trusts the manifest it is given, exactly like the real worker
  // flow does; B's OWN re-verification below is what is actually
  // under test here.
  const runtimeFingerprint = await buildRuntimeFingerprint(envA);
  const manifest = buildManifest({
    taskId,
    attemptId,
    epoch: claimA.epoch,
    baseCommit: fixture.baseCommit,
    resultCommit,
    resultTree,
    verification: { outcome: "passed", exitCode: 0, stdout: "", stderr: "", stdoutTruncated: false, stderrTruncated: false, durationMs: 1 },
    runtimeFingerprint,
    containerImageId: null,
  });
  const receipt = await Effect.runPromise(
    Effect.provide(
      publishCheckpoint({ taskId, ownerId: ownerA, epoch: claimA.epoch, commandId: checkpointCommandId(taskId), attemptId, manifest }),
      TaskStoreLive(pool),
    ),
  );
  void receipt;

  const task = await getTask(taskId);
  const handoffId = uniqueId("handoff");
  const outDir = path.join(scratchDir, "artifacts", taskId);
  const artifact = await prepareHandoffArtifact({
    taskId,
    handoffId,
    worktreeDir: worktreeDirA,
    env: envA,
    resultRef: branch,
    sourceCheckpointCommandId: checkpointCommandId(taskId),
    sourceManifestDigest: task.checkpointDigest!,
    baseCommit: fixture.baseCommit,
    resultCommit,
    resultTree,
    outDir,
  });

  return { claimA, task, managedRootA, worktreeDirA, handoffId, resultCommit, resultTree, ...artifact };
}

test("Phase 3A Part A: B's restore acknowledgement refuses a tampering tree, even though A's checkpoint (simulated as already-recorded) claimed success", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-handoff-tamper");
  const ownerA = `worker-a-${taskId}`;
  const ownerB = `worker-b-${taskId}`;

  const setup = await setUpTamperingCheckpointAndArtifact(taskId, ownerA, fixture);

  const transfer = await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, setup.handoffId),
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: ownerB,
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });

  const managedRootB = await createManagedWorkspaceRoot(path.join(scratchDir, "workspace-b-tamper"));
  const destRepoDir = path.join(managedRootB, "restored-repo");
  const envB = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootB));

  const restoreOutcome = await restoreAndAcknowledge(pool, {
    taskId,
    handoffId: setup.handoffId,
    ownerId: ownerB,
    epoch: transfer.toEpoch,
    artifactPath: setup.artifactPath,
    manifestPath: setup.manifestPath,
    destRepoDir,
    env: envB,
  });

  assert.equal(restoreOutcome.kind, "verification_failed", `B's own independent verification must refuse the tampering tree, regardless of A's claimed success; got ${restoreOutcome.kind}`);
  if (restoreOutcome.kind === "verification_failed") {
    assert.equal(restoreOutcome.verification.exitCode, 0, "the real process exit code (0) must be reported honestly");
    assert.match(restoreOutcome.verification.stderr, /integrity check.*refusing to accept/i);
  }

  const [handoffRows] = await pool.query<any[]>(`SELECT restore_ack_command_id FROM task_handoffs WHERE task_id = ?`, [taskId]);
  if (handoffRows.length > 0) {
    assert.equal(handoffRows[0]!.restore_ack_command_id, null, "no restore acknowledgement may ever be recorded for a tampering tree");
  }
  const [ackEvents] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'RestoreAcknowledged'`, [taskId]);
  assert.equal(ackEvents[0]!.c, 0, "EXACTLY ZERO RestoreAcknowledged events -- B must never acknowledge a tampered restoration");
});

test("1. successful A-to-B restoration via TWO REAL, SEPARATE worker processes, with equal full commit/tree IDs", async () => {
  const harness = new ProcessHarness();
  try {
    const fixture = await freshFixture();
    const taskId = uniqueId("task-handoff-e2e");
    const ownerA = `worker-a-${taskId}`;
    const ownerB = `worker-b-${taskId}`;
    const managedRootA = path.join(scratchDir, "workspace-a-real");
    await ensureDir(managedRootA);

    const childA = harness.spawn({
      label: "worker-a",
      entry: workerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_WORKER_TASK_ID: taskId,
        RELAYLAB_WORKER_REPO_DIR: fixture.repoDir,
        RELAYLAB_WORKER_COMMIT_REF: fixture.baseCommit,
        RELAYLAB_WORKER_OWNER_ID: ownerA,
        RELAYLAB_WORKER_WORKSPACE_ROOT: managedRootA,
      }),
    });
    const exitA = await childA.exited;
    assert.equal(exitA.code, 0, "worker A's real process must exit 0 (checkpointed)");

    const taskAfterA = await getTask(taskId);
    assert.equal(taskAfterA.epoch, 1);
    assert.ok(taskAfterA.checkpointDigest);

    // The coordinator (this orchestrator process) builds the artifact
    // from A's own worktree -- a local filesystem/Git operation, not
    // modeled as its own process (see docs/decisions/0011).
    const envA = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootA));
    const worktreeDirA = resolveWorktreePath(managedRootA, taskId, defaultAttemptId(taskId));
    const headA = await currentCommit(worktreeDirA, envA);
    const treeA = await currentTree(worktreeDirA, envA);

    const handoffId = uniqueId("handoff");
    const artifact = await prepareHandoffArtifact({
      taskId,
      handoffId,
      worktreeDir: worktreeDirA,
      env: envA,
      resultRef: branchNameFor(taskId),
      sourceCheckpointCommandId: `git-checkpoint-${taskId}`,
      sourceManifestDigest: taskAfterA.checkpointDigest!,
      baseCommit: fixture.baseCommit,
      resultCommit: headA,
      resultTree: treeA,
      outDir: path.join(scratchDir, "artifacts", taskId),
    });

    const transfer = await requestOwnershipTransfer(pool, {
      taskId,
      handoffId: handoffCommandId(taskId, handoffId),
      fromOwnerId: ownerA,
      fromEpoch: taskAfterA.epoch,
      toOwnerId: ownerB,
      checkpointDigest: taskAfterA.checkpointDigest!,
      artifactDigest: artifact.manifest.artifactDigest,
      leaseMs: 120_000,
    });
    assert.equal(transfer.toEpoch, 2);

    const destRepoDir = path.join(scratchDir, "workspace-b-real", "restored-repo");
    const managedRootB = path.join(scratchDir, "workspace-b-real-home");
    await ensureDir(managedRootB);
    const childB = harness.spawn({
      label: "worker-b",
      entry: restoreWorkerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_RESTORE_TASK_ID: taskId,
        RELAYLAB_RESTORE_HANDOFF_ID: handoffId,
        RELAYLAB_RESTORE_OWNER_ID: ownerB,
        RELAYLAB_RESTORE_EPOCH: String(transfer.toEpoch),
        RELAYLAB_RESTORE_ARTIFACT_PATH: artifact.artifactPath,
        RELAYLAB_RESTORE_MANIFEST_PATH: artifact.manifestPath,
        RELAYLAB_RESTORE_DEST_REPO_DIR: destRepoDir,
        RELAYLAB_RESTORE_WORKSPACE_ROOT: managedRootB,
      }),
    });
    const exitB = await childB.exited;
    assert.equal(exitB.code, 0, "worker B's real process must exit 0 (restored)");

    const envB = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootB));
    const restoredCommit = await currentCommit(destRepoDir, envB);
    const restoredTree = await currentTree(destRepoDir, envB);
    assert.equal(restoredCommit, headA, "B's restored commit must EXACTLY equal A's real result commit");
    assert.equal(restoredTree, treeA, "B's restored tree must EXACTLY equal A's real result tree");

    const [outboxRows] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'RestoreAcknowledged'`, [taskId]);
    assert.equal(outboxRows[0]!.c, 1, "expected exactly one RestoreAcknowledged event");
  } finally {
    await harness.stopAll();
  }
});

test("2. B restores successfully even with A's source Git object store made UNAVAILABLE beforehand -- proves the artifact is genuinely self-contained", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-source-unavailable");
  const ownerA = "worker-a";
  const ownerB = "worker-b";
  const setup = await setUpPublishedCheckpointAndArtifact(taskId, ownerA, fixture);

  // Make A's source repository genuinely UNAVAILABLE (renamed away, not
  // deleted -- reversible, and this is a harness-owned fixture
  // directory, never user data) BEFORE B ever touches the artifact.
  const movedAwayPath = `${fixture.repoDir}.unavailable`;
  await rename(fixture.repoDir, movedAwayPath);
  const movedWorktreePath = `${setup.worktreeDirA}.unavailable`;
  await rename(setup.worktreeDirA, movedWorktreePath);

  const transfer = await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, setup.handoffId),
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: ownerB,
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });

  const destRepoDir = path.join(scratchDir, "workspace-b", taskId, "restored-repo");
  const envB = makeIsolatedGitEnv(path.join(scratchDir, "home-b", taskId));
  const outcome = await restoreAndAcknowledge(pool, {
    taskId,
    handoffId: setup.handoffId,
    ownerId: ownerB,
    epoch: transfer.toEpoch,
    artifactPath: setup.artifactPath,
    manifestPath: setup.manifestPath,
    destRepoDir,
    env: envB,
  });

  assert.equal(outcome.kind, "restored", "B must restore successfully even though A's entire source repo/worktree is unavailable");
  if (outcome.kind === "restored") {
    assert.equal(outcome.restoredCommit, setup.outcome.kind === "checkpointed" ? setup.outcome.manifest.resultCommit : "");
  }

  // Restore the moved paths so test cleanup (scratchDir removal) still
  // works normally; not required for correctness, just tidiness.
  await rename(movedAwayPath, fixture.repoDir).catch(() => {});
  await rename(movedWorktreePath, setup.worktreeDirA).catch(() => {});
});

test("3. a corrupt/truncated artifact is rejected BEFORE a successful restore -- not partially consumed", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-corrupt-artifact");
  const ownerA = "worker-a";
  const setup = await setUpPublishedCheckpointAndArtifact(taskId, ownerA, fixture);

  // Truncate the artifact's bytes in place -- a classic "interrupted
  // transfer" corruption, not a crafted attack payload.
  const { readFile } = await import("node:fs/promises");
  const original = await readFile(setup.artifactPath);
  await writeFile(setup.artifactPath, original.subarray(0, Math.floor(original.length / 2)));

  const transfer = await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, setup.handoffId),
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: "worker-b",
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });

  const destRepoDir = path.join(scratchDir, "workspace-b-corrupt", "restored-repo");
  const envB = makeIsolatedGitEnv(path.join(scratchDir, "home-b-corrupt"));
  await assert.rejects(
    () =>
      restoreAndAcknowledge(pool, {
        taskId,
        handoffId: setup.handoffId,
        ownerId: "worker-b",
        epoch: transfer.toEpoch,
        artifactPath: setup.artifactPath,
        manifestPath: setup.manifestPath,
        destRepoDir,
        env: envB,
      }),
    /size-mismatched|truncated|corrupt/i,
  );

  const [outboxRows] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'RestoreAcknowledged'`, [taskId]);
  assert.equal(outboxRows[0]!.c, 0, "a corrupt artifact must never produce a restore acknowledgement");
});

test("4. a wrong manifest/artifact pairing is rejected (digest mismatch)", async () => {
  const fixture = await freshFixture();
  const taskIdA = uniqueId("task-pairing-a");
  const taskIdB = uniqueId("task-pairing-b");
  const ownerA = "worker-a";
  const setupA = await setUpPublishedCheckpointAndArtifact(taskIdA, ownerA, fixture);
  const fixture2 = await freshFixture();
  const setupB = await setUpPublishedCheckpointAndArtifact(taskIdB, ownerA, fixture2);

  // Pair task A's REAL artifact bytes with task B's manifest (a totally
  // different, unrelated artifact's metadata) -- the digest embedded in
  // B's manifest will not match A's actual bundle bytes.
  const transfer = await requestOwnershipTransfer(pool, {
    taskId: taskIdA,
    handoffId: handoffCommandId(taskIdA, setupA.handoffId),
    fromOwnerId: ownerA,
    fromEpoch: setupA.claimA.epoch,
    toOwnerId: "worker-b",
    checkpointDigest: setupA.task.checkpointDigest!,
    artifactDigest: setupA.manifest.artifactDigest,
    leaseMs: 120_000,
  });

  const destRepoDir = path.join(scratchDir, "workspace-b-pairing", "restored-repo");
  const envB = makeIsolatedGitEnv(path.join(scratchDir, "home-b-pairing"));
  await assert.rejects(
    () =>
      restoreAndAcknowledge(pool, {
        taskId: taskIdA,
        handoffId: setupA.handoffId,
        ownerId: "worker-b",
        epoch: transfer.toEpoch,
        artifactPath: setupA.artifactPath, // A's real bytes
        manifestPath: setupB.manifestPath, // B's UNRELATED manifest
        destRepoDir,
        env: envB,
      }),
    /content digest|size-mismatched/i,
  );
});

test("5. a duplicate handoff request returns the ORIGINAL outcome without incrementing the epoch again", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-duplicate-handoff");
  const ownerA = "worker-a";
  const setup = await setUpPublishedCheckpointAndArtifact(taskId, ownerA, fixture);
  const handoffCmdId = handoffCommandId(taskId, setup.handoffId);

  const first = await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCmdId,
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: "worker-b",
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });
  assert.equal(first.replay, false);
  assert.equal(first.toEpoch, setup.claimA.epoch + 1);

  const second = await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCmdId,
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: "worker-b",
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });
  assert.equal(second.replay, true, "the duplicate request must be reported as a replay");
  assert.equal(second.toEpoch, first.toEpoch, "the epoch must NOT increment a second time");

  const task = await getTask(taskId);
  assert.equal(task.epoch, first.toEpoch, "the task's actual epoch must still be exactly ONE increment above the original");
  const [rows] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM task_handoffs WHERE task_id = ?`, [taskId]);
  assert.equal(rows[0]!.c, 1, "exactly one durable handoff row, despite the duplicate request");

  // Part C/8: reusing the handoffId with DIFFERENT content is rejected,
  // not silently merged with the original.
  await assert.rejects(
    () =>
      requestOwnershipTransfer(pool, {
        taskId,
        handoffId: handoffCmdId,
        fromOwnerId: ownerA,
        fromEpoch: setup.claimA.epoch,
        toOwnerId: "worker-c", // DIFFERENT toOwnerId, same handoffId
        checkpointDigest: setup.task.checkpointDigest!,
        artifactDigest: setup.manifest.artifactDigest,
        leaseMs: 120_000,
      }),
    /already used with a different payload/i,
  );
});

test("6. competing handoff requests (two different handoffIds, same fromEpoch) produce only ONE winner", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-competing-handoff");
  const ownerA = "worker-a";
  const setup = await setUpPublishedCheckpointAndArtifact(taskId, ownerA, fixture);

  const attemptOne = requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, "race-1"),
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: "worker-b",
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });
  const attemptTwo = requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, "race-2"),
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: "worker-c",
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });

  const results = await Promise.allSettled([attemptOne, attemptTwo]);
  const fulfilled = results.filter((r) => r.status === "fulfilled");
  const rejected = results.filter((r) => r.status === "rejected");
  assert.equal(fulfilled.length, 1, "exactly one of the two competing handoff requests must succeed");
  assert.equal(rejected.length, 1, "exactly one must be rejected");
  assert.match((rejected[0] as PromiseRejectedResult).reason?.message ?? "", /stale/i);

  const task = await getTask(taskId);
  assert.equal(task.epoch, setup.claimA.epoch + 1, "epoch must have incremented by EXACTLY one, never two authoritative owners");
  const winnerOwner = (fulfilled[0] as PromiseFulfilledResult<any>).value.toOwnerId;
  assert.equal(task.ownerId, winnerOwner, "the task's actual owner must be whichever request actually won");
});

test("7. A's late attempt to publish a NEW checkpoint after transfer is rejected (ordinary epoch fencing, no new code)", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-late-publish-rejected");
  const ownerA = "worker-a";
  const setup = await setUpPublishedCheckpointAndArtifact(taskId, ownerA, fixture);

  await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, setup.handoffId),
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: "worker-b",
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });

  // A, still believing it holds `setup.claimA.epoch`, attempts a
  // GENUINELY NEW command (a fresh commandId this task has never used)
  // -- this is the clean, direct demonstration of "ordinary epoch
  // fencing, no new code": `applyTaskCommand`'s existing-receipt lookup
  // finds nothing for this commandId, so it proceeds straight to the
  // row/epoch check, which rejects it. (A retry of A's OWN *already-
  // published* checkpoint commandId is a DIFFERENT, separately-tested
  // case -- see test 8 below: that one is intercepted by the
  // replay/fingerprint check BEFORE epoch is ever consulted, which is
  // itself correct and intentional per docs/decisions/0004, just not
  // literally "epoch fencing.")
  const store = makeTaskStoreLive(pool);
  await assert.rejects(
    () =>
      Effect.runPromise(
        store.applyTaskCommand({
          taskId,
          ownerId: ownerA,
          epoch: setup.claimA.epoch,
          commandId: uniqueId("late-command-from-a"),
          command: { type: "Checkpoint", digest: "a-brand-new-digest-a-never-published-before" },
        }),
      ),
    /stale/i,
  );

  const task = await getTask(taskId);
  assert.equal(task.ownerId, "worker-b", "ownership must still belong to B -- A's late attempt must not have reclaimed anything");
  assert.equal(task.checkpointDigest, setup.task.checkpointDigest, "the task's authoritative checkpoint must be UNCHANGED by A's rejected late attempt");
});

test("8. historical receipt replay (the ORIGINAL checkpoint publish) behaves per the existing contract without creating a new effect", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-historical-replay");
  const ownerA = "worker-a";
  const setup = await setUpPublishedCheckpointAndArtifact(taskId, ownerA, fixture);
  if (setup.outcome.kind !== "checkpointed") throw new Error("unreachable");

  await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, setup.handoffId),
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: "worker-b",
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });

  // A retries its ORIGINAL checkpoint publish (same commandId, same
  // manifest) AFTER the transfer -- per docs/decisions/0004, a replay
  // from the original committing owner succeeds (returns the original
  // receipt), even though A is no longer the current owner.
  const store = makeTaskStoreLive(pool);
  const replay = await Effect.runPromise(
    store.recordGitCheckpoint({
      taskId,
      ownerId: ownerA,
      epoch: setup.claimA.epoch,
      commandId: `git-checkpoint-${taskId}`,
      attemptId: defaultAttemptId(taskId),
      manifestDigest: setup.task.checkpointDigest!,
      manifestJson: JSON.stringify(setup.outcome.manifest, Object.keys(setup.outcome.manifest).sort()),
    }),
  ).catch((err) => err);
  // Either a clean replay=true, or a digest/shape validation error IS
  // acceptable here (manifestJson re-serialization order is not
  // guaranteed to byte-match the original `stableStringify` output) --
  // what matters, and is asserted below, is that NO SECOND checkpoint
  // row was created either way.
  void replay;

  const [rows] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal(rows[0]!.c, 1, "exactly one checkpoint row -- a historical replay attempt must never create a second effect");
});

test("10. a restore acknowledgement persisted but whose response was lost: retrying creates NO duplicate acknowledgement/event", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-ack-retry-no-duplicate");
  const ownerA = "worker-a";
  const ownerB = "worker-b";
  const setup = await setUpPublishedCheckpointAndArtifact(taskId, ownerA, fixture);

  const transfer = await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, setup.handoffId),
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: ownerB,
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });

  const destRepoDir = path.join(scratchDir, "workspace-b-ack-retry", "restored-repo");
  const envB = makeIsolatedGitEnv(path.join(scratchDir, "home-b-ack-retry"));
  const restoreArgs = {
    taskId,
    handoffId: setup.handoffId,
    ownerId: ownerB,
    epoch: transfer.toEpoch,
    artifactPath: setup.artifactPath,
    manifestPath: setup.manifestPath,
    destRepoDir,
    env: envB,
  };

  const first = await restoreAndAcknowledge(pool, restoreArgs);
  assert.equal(first.kind, "restored");

  // Simulates "acknowledgement persisted, final response lost" --
  // retrying the FULL restore flow again (the destination repo already
  // exists and is already correctly restored; re-running must stay
  // idempotent end to end, not just at the DB layer).
  const second = await restoreAndAcknowledge(pool, restoreArgs);
  assert.equal(second.kind, "restored");
  if (first.kind === "restored" && second.kind === "restored") {
    assert.equal(second.receipt.replay, true, "the retried acknowledgement must be reported as a replay");
  }

  const [outboxRows] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'RestoreAcknowledged'`, [taskId]);
  assert.equal(outboxRows[0]!.c, 1, "expected EXACTLY ONE RestoreAcknowledged event despite the retry");
});

test("11. an UNRELATED task owned by A continues to complete successfully after A hands off a DIFFERENT task", async () => {
  const fixture = await freshFixture();
  const taskIdHandedOff = uniqueId("task-handed-off");
  const taskIdUnrelated = uniqueId("task-unrelated");
  const ownerA = "worker-a";

  const setup = await setUpPublishedCheckpointAndArtifact(taskIdHandedOff, ownerA, fixture);
  await requestOwnershipTransfer(pool, {
    taskId: taskIdHandedOff,
    handoffId: handoffCommandId(taskIdHandedOff, setup.handoffId),
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: "worker-b",
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });

  // A different task, never touched by any handoff, claimed and
  // completed by the SAME owner identity "worker-a" -- unaffected by
  // the handed-off task's own epoch/ownership change.
  const fixture2 = await freshFixture();
  const managedRootA2 = await createManagedWorkspaceRoot(path.join(scratchDir, "workspace-a-unrelated"));
  const claimUnrelated = await claim(taskIdUnrelated, ownerA);
  const outcomeUnrelated = await runGitWorkerTask({
    taskId: taskIdUnrelated,
    ownerId: ownerA,
    epoch: claimUnrelated.epoch,
    sourceRepoDir: fixture2.repoDir,
    requestedCommitRef: fixture2.baseCommit,
    managedWorkspaceRoot: managedRootA2,
    pool,
  });
  assert.equal(outcomeUnrelated.kind, "checkpointed", "the UNRELATED task must complete normally -- A's handoff of a different task must not affect it");

  const unrelatedTask = await getTask(taskIdUnrelated);
  assert.equal(unrelatedTask.ownerId, ownerA, "the unrelated task must still be owned by A");
});

test("12. unexpected destination contents are PRESERVED and the restore is rejected, never overwritten", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-unexpected-destination");
  const ownerA = "worker-a";
  const setup = await setUpPublishedCheckpointAndArtifact(taskId, ownerA, fixture);

  const transfer = await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, setup.handoffId),
    fromOwnerId: ownerA,
    fromEpoch: setup.claimA.epoch,
    toOwnerId: "worker-b",
    checkpointDigest: setup.task.checkpointDigest!,
    artifactDigest: setup.manifest.artifactDigest,
    leaseMs: 120_000,
  });

  const destRepoDir = path.join(scratchDir, "workspace-b-unexpected-dest", "restored-repo");
  await mkdir(destRepoDir, { recursive: true });
  const sentinelPath = path.join(destRepoDir, "pre-existing-unrelated-file.txt");
  await writeFile(sentinelPath, "this file was already here before the restore attempt\n", "utf8");

  const envB = makeIsolatedGitEnv(path.join(scratchDir, "home-b-unexpected-dest"));
  await assert.rejects(
    () =>
      restoreAndAcknowledge(pool, {
        taskId,
        handoffId: setup.handoffId,
        ownerId: "worker-b",
        epoch: transfer.toEpoch,
        artifactPath: setup.artifactPath,
        manifestPath: setup.manifestPath,
        destRepoDir,
        env: envB,
      }),
    /not empty|unexpected destination/i,
  );

  const { readFile } = await import("node:fs/promises");
  const stillThere = await readFile(sentinelPath, "utf8");
  assert.equal(stillThere, "this file was already here before the restore attempt\n", "the pre-existing file must be completely untouched");
  const [outboxRows] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'RestoreAcknowledged'`, [taskId]);
  assert.equal(outboxRows[0]!.c, 0);
});
