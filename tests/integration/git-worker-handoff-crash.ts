// Phase 2B, Part G test 9: a real SIGKILL of B's real worker process
// between "ownership transferred" and "restore acknowledgement
// persisted," then a real restart (no failpoint) against the SAME
// task/handoff/artifact, proving the documented recovery path
// (docs/decisions/0011-handoff-protocol.md "Recovery when B fails
// after ownership transfer, before acknowledgement") actually works --
// same IPC failpoint-readiness handshake convention as
// tests/integration/crash-windows.ts / git-worker-crash.ts, never an
// arbitrary sleep.
//
// Usage: npm run test:handoff-crash
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import mysql from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { loadTestMysqlConfig } from "../../src/config.js";
import { ProcessHarness, testChildEnv, repoRootFrom, type ManagedChild } from "./process-harness.js";
import { createFixtureRepo } from "../../src/worker/fixture-repo.js";
import { createManagedWorkspaceRoot, removeManagedWorkspaceRoot, resolveWorktreePath, resolveIsolatedHomeDir, ensureDir } from "../../src/worker/workspace.js";
import { runGitWorkerTask, defaultAttemptId, branchNameFor } from "../../src/worker/run-task.js";
import { makeIsolatedGitEnv, currentCommit, currentTree, hasUncommittedChanges } from "../../src/worker/git.js";
import { prepareHandoffArtifact, requestOwnershipTransfer, handoffCommandId } from "../../src/worker/handoff.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = repoRootFrom(here);
const restoreWorkerEntry = path.join(repoRoot, "src", "restore-worker-main.ts");
const testDatabase = loadTestMysqlConfig().database;

function log(scenario: string, msg: string) {
  console.log(`[handoff-crash ${scenario}] ${msg}`);
}
function logEvidence(scenario: string, label: string, data: unknown) {
  console.log(`[handoff-crash ${scenario}] EVIDENCE ${label}: ${JSON.stringify(data)}`);
}

interface FailpointMessage {
  type?: string;
  name?: string;
  pid?: number;
  taskId?: string;
  handoffId?: string;
  ownerId?: string;
  epoch?: number;
  restoredCommit?: string;
  restoredTree?: string;
}

function waitForFailpointReady(child: ManagedChild, failpointName: string, timeoutMs: number): Promise<FailpointMessage> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      child.process.off("message", onMessage);
      reject(new Error(`timed out after ${timeoutMs}ms waiting for failpoint "${failpointName}" to report ready`));
    }, timeoutMs);
    function onMessage(msg: unknown) {
      const m = msg as FailpointMessage;
      if (m?.type === "failpoint-ready" && m.name === failpointName) {
        clearTimeout(timer);
        child.process.off("message", onMessage);
        resolve(m);
      }
    }
    child.process.on("message", onMessage);
  });
}

/**
 * Phase 2B.1: an INDEPENDENT liveness probe, separate from Node's own
 * `ChildProcess.exitCode`/`signalCode` bookkeeping -- `process.kill(pid,
 * 0)` is documented, cross-platform (including Windows), or-nothing
 * syscall-level existence check: it sends no actual signal, only tests
 * whether the OS still has a live process at that pid, throwing if not.
 * Used to positively confirm the barrier was reached by a STILL-LIVE
 * process, not inferred from the IPC message having arrived at all
 * (which could, in principle, arrive and then the process could exit on
 * its own before this check runs -- this check is what rules that out).
 */
function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function killAndWaitExit(child: ManagedChild): Promise<{ killReturned: boolean; exitCode: number | null; signalCode: string | null }> {
  const killReturned = child.process.kill("SIGKILL");
  const { code, signal } = await child.exited;
  // Same honest caveat as git-worker-crash.ts: on this Windows machine,
  // this is Node's own TerminateProcess-backed implementation of
  // kill("SIGKILL"), never a real POSIX signal delivery.
  return { killReturned, exitCode: code, signalCode: signal };
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs: number, label: string): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 150));
  }
  throw new Error(`timed out waiting for: ${label}`);
}

async function scenario_restoreKilledBeforeAck(pool: mysql.Pool, demoRoot: string): Promise<void> {
  const scenario = "restore-killed-before-ack";
  const harness = new ProcessHarness();
  try {
    const fixtureDir = path.join(demoRoot, "fixture");
    const fixture = await createFixtureRepo(fixtureDir);
    const taskId = `handoff-crash-${Date.now()}`;
    const ownerA = `worker-a-${taskId}`;
    const ownerB = `worker-b-${taskId}`;
    const managedRootA = await createManagedWorkspaceRoot(path.join(demoRoot, "workspace-a"));
    const envA = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootA));
    logEvidence(scenario, "task_id", taskId);

    const store = makeTaskStoreLive(pool);
    const claimA = await Effect.runPromise(store.claimTask(taskId, ownerA, 120_000));
    const outcomeA = await runGitWorkerTask({
      taskId,
      ownerId: ownerA,
      epoch: claimA.epoch,
      sourceRepoDir: fixture.repoDir,
      requestedCommitRef: fixture.baseCommit,
      managedWorkspaceRoot: managedRootA,
      pool,
    });
    assert.equal(outcomeA.kind, "checkpointed");
    if (outcomeA.kind !== "checkpointed") return;
    logEvidence(scenario, "A_checkpoint", { resultCommit: outcomeA.manifest.resultCommit, resultTree: outcomeA.manifest.resultTree });

    const worktreeDirA = resolveWorktreePath(managedRootA, taskId, defaultAttemptId(taskId));
    const handoffId = `handoff-${taskId}`;
    const artifact = await prepareHandoffArtifact({
      taskId,
      handoffId,
      worktreeDir: worktreeDirA,
      env: envA,
      resultRef: branchNameFor(taskId),
      sourceCheckpointCommandId: `git-checkpoint-${taskId}`,
      sourceManifestDigest: (await Effect.runPromise(store.getTask(taskId))).checkpointDigest!,
      baseCommit: outcomeA.manifest.baseCommit,
      resultCommit: outcomeA.manifest.resultCommit,
      resultTree: outcomeA.manifest.resultTree,
      outDir: path.join(demoRoot, "artifacts"),
    });
    logEvidence(scenario, "artifact", { digest: artifact.manifest.artifactDigest, size: artifact.manifest.artifactSizeBytes });

    const transfer = await requestOwnershipTransfer(pool, {
      taskId,
      handoffId: handoffCommandId(taskId, handoffId),
      fromOwnerId: ownerA,
      fromEpoch: claimA.epoch,
      toOwnerId: ownerB,
      checkpointDigest: (await Effect.runPromise(store.getTask(taskId))).checkpointDigest!,
      artifactDigest: artifact.manifest.artifactDigest,
      leaseMs: 120_000,
    });
    logEvidence(scenario, "transfer", { fromEpoch: transfer.fromEpoch, toEpoch: transfer.toEpoch });

    const destRepoDir = path.join(demoRoot, "workspace-b", "restored-repo");
    const managedRootB = path.join(demoRoot, "workspace-b-home");
    await ensureDir(managedRootB);

    const child = harness.spawn({
      label: "restore-worker-killed",
      entry: restoreWorkerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_FAILPOINT: "restore-after-checkout-before-ack",
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
    const ready = await waitForFailpointReady(child, "restore-after-checkout-before-ack", 15_000);
    logEvidence(scenario, "failpoint_ipc_payload", ready);
    assert.ok(ready.restoredCommit && /^[0-9a-f]{40}$/.test(ready.restoredCommit));
    assert.equal(ready.restoredCommit, outcomeA.manifest.resultCommit, "the failpoint's reported restored commit must already equal A's real result commit -- checkout happened before this failpoint");
    assert.equal(ready.taskId, taskId, "the failpoint's reported identity must match this scenario's actual task");
    assert.equal(ready.handoffId, handoffId);
    assert.equal(ready.ownerId, ownerB);
    assert.equal(ready.epoch, transfer.toEpoch);

    // Independent liveness confirmation, per-PID, BEFORE asserting
    // anything about "zero acknowledgements" or killing anything --
    // this is what proves the barrier was reached by a process that
    // was STILL ALIVE at the moment we observed it, not merely that an
    // IPC message arrived at some point.
    assert.ok(typeof ready.pid === "number" && ready.pid > 0, "failpoint payload must report a real pid");
    assert.equal(ready.pid, child.process.pid, "the failpoint's reported pid must match the ACTUAL spawned child's pid");
    const aliveAtBarrier = isProcessAlive(ready.pid);
    logEvidence(scenario, "pid_liveness_at_barrier", { pid: ready.pid, alive: aliveAtBarrier });
    assert.equal(aliveAtBarrier, true, "the restore-worker process must still be alive (confirmed via an independent OS-level probe) at the moment we observed it parked at the barrier");

    const [acksBeforeKill] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'RestoreAcknowledged'`, [taskId]);
    assert.equal(acksBeforeKill[0]!.c, 0, "expected ZERO RestoreAcknowledged events at the failpoint -- the failpoint is in the wrong place otherwise");

    const envBForInspection = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootB));
    const preKillCommit = await currentCommit(destRepoDir, envBForInspection);
    const preKillTree = await currentTree(destRepoDir, envBForInspection);
    const preKillDirty = await hasUncommittedChanges(destRepoDir, envBForInspection);
    logEvidence(scenario, "orchestrator_observed_pre_kill", { commit: preKillCommit, tree: preKillTree, dirty: preKillDirty });
    assert.equal(preKillCommit, outcomeA.manifest.resultCommit);
    assert.equal(preKillTree, outcomeA.manifest.resultTree);
    assert.equal(preKillDirty, false);

    // Re-confirm liveness IMMEDIATELY before sending the kill -- closes
    // the window between the barrier observation above and the actual
    // termination request below.
    assert.equal(isProcessAlive(ready.pid), true, "the process must still be alive immediately before the kill is sent");

    const exitInfo = await killAndWaitExit(child);
    logEvidence(scenario, "child_exit_outcome", exitInfo);
    assert.equal(exitInfo.killReturned, true, "the kill() syscall request itself must have been accepted by the OS");
    // Phase 2B.1: with the failpoint's own explicit active handle fix
    // (src/testing/failpoint.ts) now in place, an exit code of exactly
    // 0 here would mean the process reached its OWN normal completion
    // path instead of being interrupted -- that is NOT an artifact to
    // explain away, it is this test failing to prove what it claims to
    // prove, so it is asserted against directly, not merely logged.
    assert.notEqual(
      exitInfo.exitCode,
      0,
      `the killed restore-worker exited with code 0 (its OWN normal completion), not via the forced termination -- the active-handle fix did not prevent a natural exit; this crash scenario did NOT actually interrupt a live worker`,
    );
    assert.equal(isProcessAlive(ready.pid), false, "the process must actually be gone from the OS process table after the kill");

    log(scenario, "restarting a fresh restore-worker (no failpoint), SAME task/handoff/artifact/epoch, to follow the documented recovery path");
    harness.spawn({
      label: "restore-worker-recovered",
      entry: restoreWorkerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_RESTORE_TASK_ID: taskId,
        RELAYLAB_RESTORE_HANDOFF_ID: handoffId,
        RELAYLAB_RESTORE_OWNER_ID: ownerB, // SAME owner identity -- idempotent retry, not a takeover
        RELAYLAB_RESTORE_EPOCH: String(transfer.toEpoch), // SAME epoch -- never rolled back
        RELAYLAB_RESTORE_ARTIFACT_PATH: artifact.artifactPath,
        RELAYLAB_RESTORE_MANIFEST_PATH: artifact.manifestPath,
        RELAYLAB_RESTORE_DEST_REPO_DIR: destRepoDir, // SAME destination -- must be recognized as already-correctly-restored
        RELAYLAB_RESTORE_WORKSPACE_ROOT: managedRootB,
      }),
    });

    await waitFor(
      async () => {
        const [rows] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'RestoreAcknowledged'`, [taskId]);
        return (rows[0]?.c ?? 0) >= 1;
      },
      20_000,
      "the restarted restore-worker to publish exactly one RestoreAcknowledged event",
    );

    const [acksAfter] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'RestoreAcknowledged'`, [taskId]);
    assert.equal(acksAfter[0]!.c, 1, "expected EXACTLY ONE RestoreAcknowledged event after recovery");

    const task = await Effect.runPromise(store.getTask(taskId));
    assert.equal(task.epoch, transfer.toEpoch, "epoch must be UNCHANGED by the recovery retry -- never rolled back, never incremented again");
    assert.equal(task.ownerId, ownerB);

    const postRestartCommit = await currentCommit(destRepoDir, envBForInspection);
    const postRestartTree = await currentTree(destRepoDir, envBForInspection);
    logEvidence(scenario, "orchestrator_observed_post_restart", { commit: postRestartCommit, tree: postRestartTree });
    assert.equal(postRestartCommit, preKillCommit, "post-restart restored commit must EXACTLY equal the pre-kill restored commit");
    assert.equal(postRestartTree, preKillTree);

    log(scenario, "PASS: a real kill between ownership transfer and restore acknowledgement recovers via the documented retry path -- same destination, same epoch, exactly one acknowledgement");
  } finally {
    await harness.stopAll();
  }
}

async function main() {
  console.log(`[handoff-crash] runtime identity (orchestrator): node ${process.version} at ${process.execPath}`);
  const pool = mysql.createPool({ ...loadTestMysqlConfig(), waitForConnections: true, connectionLimit: 10 });
  const { applyMigrations } = await import("../../src/db/migrate.js");
  await applyMigrations(loadTestMysqlConfig());
  const demoRoot = await mkdtemp(path.join(tmpdir(), "relaylab-handoff-crash-"));
  try {
    await scenario_restoreKilledBeforeAck(pool, demoRoot);
    console.log("[handoff-crash] ALL SCENARIOS PASSED");
  } finally {
    await pool.end();
    await removeManagedWorkspaceRoot(demoRoot);
  }
}

main().catch((err) => {
  console.error("[handoff-crash] FAILED:", err);
  process.exitCode = 1;
});
