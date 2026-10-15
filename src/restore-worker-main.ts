// Phase 2B: B's real worker process entrypoint -- a one-shot CLI, same
// convention as src/worker-main.ts (A's entrypoint): no polling loop,
// a single restore is specified explicitly via env vars. See
// docs/decisions/0011-handoff-protocol.md for the full protocol this
// implements steps 4-6 of.
import { startTelemetry, shutdownTelemetry } from "./telemetry/init.js";
startTelemetry("relaylab-restore-worker");

import { createPool } from "./db/pool.js";
import { loadMysqlConfig } from "./config.js";
import { restoreAndAcknowledge } from "./worker/handoff.js";
import { makeIsolatedGitEnv } from "./worker/git.js";
import { resolveIsolatedHomeDir, ensureDir } from "./worker/workspace.js";

export interface RestoreWorkerCliArgs {
  taskId: string;
  handoffId: string;
  ownerId: string;
  epoch: number;
  artifactPath: string;
  manifestPath: string;
  destRepoDir: string;
  managedWorkspaceRoot: string;
}

export async function runRestoreOnce(cliArgs: RestoreWorkerCliArgs) {
  const pool = createPool(loadMysqlConfig());
  console.log(`[restore-worker] runtime identity: node ${process.version} at ${process.execPath}, pid ${process.pid}`);
  console.log(`[restore-worker] restoring task "${cliArgs.taskId}" handoff "${cliArgs.handoffId}" as "${cliArgs.ownerId}" at epoch ${cliArgs.epoch}`);

  try {
    const isolatedHome = resolveIsolatedHomeDir(cliArgs.managedWorkspaceRoot);
    await ensureDir(isolatedHome);
    const env = makeIsolatedGitEnv(isolatedHome);

    const outcome = await restoreAndAcknowledge(pool, {
      taskId: cliArgs.taskId,
      handoffId: cliArgs.handoffId,
      ownerId: cliArgs.ownerId,
      epoch: cliArgs.epoch,
      artifactPath: cliArgs.artifactPath,
      manifestPath: cliArgs.manifestPath,
      destRepoDir: cliArgs.destRepoDir,
      env,
    });

    if (outcome.kind === "restored") {
      console.log(`[restore-worker] restored successfully`);
      console.log(`[restore-worker] restoredCommit=${outcome.restoredCommit}`);
      console.log(`[restore-worker] restoredTree=${outcome.restoredTree}`);
      console.log(`[restore-worker] verification=${outcome.verification.outcome} (exit ${outcome.verification.exitCode})`);
      console.log(`[restore-worker] ack replay=${outcome.receipt.replay}`);
    } else {
      console.error(`[restore-worker] did NOT restore successfully: ${outcome.kind}`);
    }
    return outcome;
  } finally {
    await pool.end();
    await shutdownTelemetry();
  }
}

function parseArgsFromEnv(): RestoreWorkerCliArgs {
  const taskId = process.env.RELAYLAB_RESTORE_TASK_ID;
  const handoffId = process.env.RELAYLAB_RESTORE_HANDOFF_ID;
  const ownerId = process.env.RELAYLAB_RESTORE_OWNER_ID;
  const epochRaw = process.env.RELAYLAB_RESTORE_EPOCH;
  const artifactPath = process.env.RELAYLAB_RESTORE_ARTIFACT_PATH;
  const manifestPath = process.env.RELAYLAB_RESTORE_MANIFEST_PATH;
  const destRepoDir = process.env.RELAYLAB_RESTORE_DEST_REPO_DIR;
  const managedWorkspaceRoot = process.env.RELAYLAB_RESTORE_WORKSPACE_ROOT;
  if (!taskId || !handoffId || !ownerId || !epochRaw || !artifactPath || !manifestPath || !destRepoDir || !managedWorkspaceRoot) {
    throw new Error(
      "RELAYLAB_RESTORE_TASK_ID, RELAYLAB_RESTORE_HANDOFF_ID, RELAYLAB_RESTORE_OWNER_ID, RELAYLAB_RESTORE_EPOCH, RELAYLAB_RESTORE_ARTIFACT_PATH, RELAYLAB_RESTORE_MANIFEST_PATH, RELAYLAB_RESTORE_DEST_REPO_DIR, and RELAYLAB_RESTORE_WORKSPACE_ROOT must all be set",
    );
  }
  const epoch = Number.parseInt(epochRaw, 10);
  if (!Number.isFinite(epoch)) throw new Error(`RELAYLAB_RESTORE_EPOCH must be a number, got "${epochRaw}"`);
  return { taskId, handoffId, ownerId, epoch, artifactPath, manifestPath, destRepoDir, managedWorkspaceRoot };
}

if (process.argv[1] && process.argv[1].endsWith("restore-worker-main.ts")) {
  runRestoreOnce(parseArgsFromEnv())
    .then((outcome) => {
      process.exitCode = outcome.kind === "restored" ? 0 : 1;
    })
    .catch((err) => {
      console.error("[restore-worker] fatal:", err instanceof Error ? err.message : err);
      process.exitCode = 1;
    });
}
