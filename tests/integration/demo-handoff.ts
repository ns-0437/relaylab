// Phase 2B: reproducible demo -- `npm run demo:handoff`. Runs the full
// committed-checkpoint handoff end to end against the dev database:
// worker A repairs the fixture and publishes a checkpoint, the
// coordinator builds a self-contained artifact and atomically transfers
// authoritative ownership to worker B, and B restores the artifact into
// its OWN independent repository, re-verifies it, and acknowledges.
// Prints actual commit/tree/digest/epoch values, not placeholders. Runs
// in-process (same convention as demo-git-worker.ts); the AUTOMATED test
// suite (tests/integration/git-worker-handoff.test.ts) is what proves
// the "separate worker processes" requirement with real forked
// processes.
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createPool } from "../../src/db/pool.js";
import { loadMysqlConfig } from "../../src/config.js";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { createFixtureRepo } from "../../src/worker/fixture-repo.js";
import { runOnce } from "../../src/worker-main.js";
import { defaultAttemptId, branchNameFor } from "../../src/worker/run-task.js";
import { createManagedWorkspaceRoot, resolveWorktreePath, resolveIsolatedHomeDir } from "../../src/worker/workspace.js";
import { makeIsolatedGitEnv } from "../../src/worker/git.js";
import { prepareHandoffArtifact, requestOwnershipTransfer, restoreAndAcknowledge, handoffCommandId } from "../../src/worker/handoff.js";
import { uniqueId } from "./helpers.js";

async function main() {
  const pool = createPool(loadMysqlConfig());
  const store = makeTaskStoreLive(pool);
  try {
    const demoRoot = await mkdtemp(path.join(tmpdir(), "relaylab-handoff-demo-"));
    const fixtureDir = path.join(demoRoot, "fixture-repo");
    const fixture = await createFixtureRepo(fixtureDir);
    console.log(`[demo] fixture repository created at ${fixtureDir}`);
    console.log(`[demo] baseline (buggy) commit: ${fixture.baseCommit}`);

    const taskId = uniqueId("handoff-demo-task");
    const ownerA = "worker-a-demo";
    // `runOnce` (src/worker-main.ts) always resolves its OWN managed
    // workspace root via `createManagedWorkspaceRoot()` with NO
    // argument -- it has no override parameter, only the
    // `RELAYLAB_WORKER_WORKSPACE_ROOT` env var (falling back to a
    // stable, SHARED OS-temp default otherwise -- see
    // src/worker/workspace.ts). Set it explicitly to a demo-local path
    // BEFORE calling `runOnce`, so this demo never collides with the
    // shared default root some other concurrent run might also be using.
    process.env.RELAYLAB_WORKER_WORKSPACE_ROOT = path.join(demoRoot, "workspace-a");
    const managedRootA = await createManagedWorkspaceRoot();

    console.log(`[demo] === step 1: worker A repairs and publishes a checkpoint ===`);
    const outcomeA = await runOnce({ taskId, sourceRepoDir: fixtureDir, requestedCommitRef: fixture.baseCommit, ownerId: ownerA });
    if (outcomeA.kind !== "checkpointed") {
      console.error(`[demo] FAILED: worker A did not checkpoint (${outcomeA.kind})`);
      process.exitCode = 1;
      return;
    }
    const { Effect } = await import("effect");
    const taskAfterA = await Effect.runPromise(store.getTask(taskId));
    console.log(`[demo] A published checkpoint digest=${taskAfterA.checkpointDigest} at epoch=${taskAfterA.epoch}`);
    console.log(`[demo] A verified in container: image=${outcomeA.imageId} executionId(container)=${outcomeA.verification.executionId} outcome=${outcomeA.verification.outcome}`);

    console.log(`[demo] === step 2: build a self-contained transferable artifact ===`);
    const handoffId = `demo-handoff-${taskId}`;
    const worktreeDirA = resolveWorktreePath(managedRootA, taskId, defaultAttemptId(taskId));
    const envA = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootA));
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
      outDir: path.join(demoRoot, "artifacts"),
    });
    console.log(`[demo] artifact created: ${artifactPath}`);
    console.log(`[demo] artifact digest=${manifest.artifactDigest} size=${manifest.artifactSizeBytes} bytes`);

    console.log(`[demo] === step 3: coordinator transfers authoritative ownership A -> B ===`);
    const ownerB = "worker-b-demo";
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
    console.log(`[demo] transferred: epoch ${transfer.fromEpoch} -> ${transfer.toEpoch}, owner ${transfer.fromOwnerId} -> ${transfer.toOwnerId}`);

    console.log(`[demo] === step 4-6: worker B restores into an INDEPENDENT repository and acknowledges ===`);
    const destRepoDir = path.join(demoRoot, "workspace-b", "restored-repo");
    const managedRootB = await createManagedWorkspaceRoot(path.join(demoRoot, "workspace-b-home"));
    const envB = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootB));
    const restoreOutcome = await restoreAndAcknowledge(pool, {
      taskId,
      handoffId,
      ownerId: ownerB,
      epoch: transfer.toEpoch,
      artifactPath,
      manifestPath,
      destRepoDir,
      env: envB,
    });

    if (restoreOutcome.kind !== "restored") {
      console.error(`[demo] FAILED: worker B did not restore successfully (${restoreOutcome.kind})`);
      process.exitCode = 1;
      return;
    }
    console.log(`[demo] B restored commit=${restoreOutcome.restoredCommit}`);
    console.log(`[demo] B restored tree=${restoreOutcome.restoredTree}`);
    console.log(`[demo] B verified in container: image=${restoreOutcome.imageId} executionId(container)=${restoreOutcome.verification.executionId} outcome=${restoreOutcome.verification.outcome} (exit ${restoreOutcome.verification.exitCode})`);
    console.log(`[demo] B restore acknowledgement replay=${restoreOutcome.receipt.replay}`);

    console.log(`[demo] === identity/evidence summary ===`);
    const commitsMatch = outcomeA.manifest.resultCommit === restoreOutcome.restoredCommit;
    const treesMatch = outcomeA.manifest.resultTree === restoreOutcome.restoredTree;
    const distinctExecutions = outcomeA.verification.executionId !== restoreOutcome.verification.executionId;
    console.log(`[demo] A.resultCommit=${outcomeA.manifest.resultCommit} B.restoredCommit=${restoreOutcome.restoredCommit} IDENTICAL=${commitsMatch}`);
    console.log(`[demo] A.resultTree=${outcomeA.manifest.resultTree} B.restoredTree=${restoreOutcome.restoredTree} IDENTICAL=${treesMatch}`);
    console.log(`[demo] A.verification.executionId=${outcomeA.verification.executionId} B.verification.executionId=${restoreOutcome.verification.executionId} DISTINCT=${distinctExecutions}`);
    if (!commitsMatch || !treesMatch || !distinctExecutions) {
      console.error("[demo] FAILED: identity/evidence summary did not hold");
      process.exitCode = 1;
      return;
    }
    console.log(
      "[demo] PASS: real checkpoint verified in its OWN container, real self-contained artifact, real atomic ownership transfer, " +
        "real independent restore verified in a SEPARATE container, real idempotent acknowledgement -- identical Git identities, distinct verification executions.",
    );
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error("[demo] fatal:", err);
  process.exitCode = 1;
});
