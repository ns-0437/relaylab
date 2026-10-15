// Phase 2A: a real worker process entrypoint. Deliberately a ONE-SHOT
// CLI, not a polling loop like publisher-main.ts/consumer-main.ts --
// this milestone has no task QUEUE to poll (a single task is specified
// explicitly), and the "smallest real workflow" instruction this
// milestone implements does not call for worker-side task discovery.
// See docs/ARCHITECTURE.md "Phase 2A worker" for the full lifecycle.
import { startTelemetry, shutdownTelemetry } from "./telemetry/init.js";
startTelemetry("relaylab-git-worker");

import { randomUUID } from "node:crypto";
import { createPool } from "./db/pool.js";
import { loadMysqlConfig } from "./config.js";
import { makeTaskStoreLive } from "./db/task-store.js";
import { Effect } from "effect";
import { runGitWorkerTask } from "./worker/run-task.js";
import { createManagedWorkspaceRoot } from "./worker/workspace.js";

export interface WorkerCliArgs {
  taskId: string;
  sourceRepoDir: string;
  requestedCommitRef: string;
  ownerId?: string;
}

export async function runOnce(cliArgs: WorkerCliArgs) {
  const pool = createPool(loadMysqlConfig());
  const store = makeTaskStoreLive(pool);
  const ownerId = cliArgs.ownerId ?? `git-worker-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
  const managedWorkspaceRoot = await createManagedWorkspaceRoot();

  console.log(`[git-worker] runtime identity: node ${process.version} at ${process.execPath}, pid ${process.pid}`);
  console.log(`[git-worker] claiming task "${cliArgs.taskId}" as "${ownerId}"`);

  try {
    const claim = await Effect.runPromise(store.claimTask(cliArgs.taskId, ownerId, 120_000));
    console.log(`[git-worker] claimed epoch ${claim.epoch}`);

    const outcome = await runGitWorkerTask({
      taskId: cliArgs.taskId,
      ownerId,
      epoch: claim.epoch,
      sourceRepoDir: cliArgs.sourceRepoDir,
      requestedCommitRef: cliArgs.requestedCommitRef,
      managedWorkspaceRoot,
      pool,
    });

    if (outcome.kind === "checkpointed") {
      console.log(`[git-worker] checkpoint ${outcome.replay ? "already existed (replay)" : "published"}`);
      console.log(`[git-worker] baseCommit=${outcome.manifest.baseCommit}`);
      console.log(`[git-worker] resultCommit=${outcome.manifest.resultCommit}`);
      console.log(`[git-worker] resultTree=${outcome.manifest.resultTree}`);
      console.log(`[git-worker] verification=${outcome.manifest.verification.outcome} (exit ${outcome.manifest.verification.exitCode}, ${outcome.manifest.verification.durationMs.toFixed(1)}ms)`);
      console.log(`[git-worker] runtime: node ${outcome.manifest.runtimeFingerprint.nodeVersion} at ${outcome.manifest.runtimeFingerprint.execPath}`);
    } else {
      console.error(`[git-worker] did NOT checkpoint: ${outcome.kind}`);
      if ("verification" in outcome) {
        console.error(`[git-worker] verification exit=${outcome.verification.exitCode} outcome=${outcome.verification.outcome}`);
      }
    }
    return outcome;
  } finally {
    await pool.end();
    // Deliberately NOT removing `managedWorkspaceRoot` here: it is a
    // STABLE, SHARED directory across worker restarts and across
    // DIFFERENT tasks (see src/worker/workspace.ts's
    // `createManagedWorkspaceRoot` -- a real bug this patch fixed: an
    // earlier version randomized this root per process, which silently
    // broke restart reconciliation entirely). Deleting the whole root on
    // every single run would destroy OTHER tasks' worktrees and defeat
    // the entire point of this being stable. This task's own worktree is
    // also the evidence backing its checkpoint (Part F: "local Git
    // objects must remain available for this checkpoint to be usable")
    // -- removing it right after a successful run would be actively
    // counterproductive. Workspace cleanup is therefore an explicit,
    // separate, OPERATOR-invoked concern (not implemented in this
    // milestone), not an automatic side effect of one task's run.
    await shutdownTelemetry();
  }
}

function parseArgsFromEnv(): WorkerCliArgs {
  const taskId = process.env.RELAYLAB_WORKER_TASK_ID ?? randomUUID();
  const sourceRepoDir = process.env.RELAYLAB_WORKER_REPO_DIR;
  const requestedCommitRef = process.env.RELAYLAB_WORKER_COMMIT_REF ?? "HEAD";
  if (!sourceRepoDir) {
    throw new Error("RELAYLAB_WORKER_REPO_DIR must be set (the fixture repository to work against)");
  }
  const ownerId = process.env.RELAYLAB_WORKER_OWNER_ID;
  return ownerId !== undefined ? { taskId, sourceRepoDir, requestedCommitRef, ownerId } : { taskId, sourceRepoDir, requestedCommitRef };
}

// Only runs the CLI path when invoked directly (not when imported by tests).
if (process.argv[1] && process.argv[1].endsWith("worker-main.ts")) {
  runOnce(parseArgsFromEnv())
    .then((outcome) => {
      process.exitCode = outcome.kind === "checkpointed" ? 0 : 1;
    })
    .catch((err) => {
      console.error("[git-worker] fatal:", err instanceof Error ? err.message : err);
      process.exitCode = 1;
    });
}
