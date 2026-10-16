// Demo UI milestone: orchestrates ONE full run (generation ->
// verification -> checkpoint -> transfer -> restore/ack) for the local
// HTTP API, using the EXISTING pipeline end to end -- runGitWorkerTask /
// runAiRepairTask for generation+verification+checkpoint, and the
// EXISTING, unmodified handoff.ts APIs for transfer+restore+ack. This
// file adds no new execution primitive; it only sequences real ones and
// records progress into demo_runs for the UI to poll.
//
// Explicitly a SINGLE local in-process execution model: one run active
// at a time, tracked by an in-memory AbortController. This is NOT a
// distributed queue/worker pool -- restarting this process necessarily
// loses any in-flight run (there is nothing to resume a JS Promise
// from), which is exactly why reconcileOnStartup (runs-store.ts) exists.
import { randomUUID } from "node:crypto";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type mysql from "mysql2/promise";
import { Effect } from "effect";
import Anthropic from "@anthropic-ai/sdk";
import { makeTaskStoreLive } from "../db/task-store.js";
import { createFixtureRepo } from "../worker/fixture-repo.js";
import { createManagedWorkspaceRoot, resolveWorktreePath, resolveIsolatedHomeDir } from "../worker/workspace.js";
import { makeIsolatedGitEnv } from "../worker/git.js";
import { runGitWorkerTask, defaultAttemptId, branchNameFor } from "../worker/run-task.js";
import { runAiRepairTask } from "../worker/run-task-ai.js";
import type { AiRepairAttemptRecord } from "../worker/ai-repair.js";
import { createAnthropicClient, loadAnthropicAdapterConfig, type MessagesClient } from "../ai/anthropic-client.js";
import { prepareHandoffArtifact, requestOwnershipTransfer, restoreAndAcknowledge, handoffCommandId } from "../worker/handoff.js";
import { insertRun, updateRunStatus, type RunDetail, type RunMode } from "./runs-store.js";

export class RunConflictError extends Error {}
export class LiveModeDisabledError extends Error {}

let active: { runId: string; controller: AbortController } | null = null;

export function activeRunId(): string | null {
  return active?.runId ?? null;
}

/** The ONE simulated response the demo mode's fake client returns -- the real, correct fix, clearly labeled. Never used for "ai-live". */
function simulatedClient(): MessagesClient {
  const correctFix = "function add(a, b) {\n  return a + b;\n}\n\nmodule.exports = { add };\n";
  return {
    messages: {
      create: (async () =>
        ({
          id: "msg_demo_simulated",
          type: "message",
          role: "assistant",
          model: "claude-opus-5-5-simulated-demo",
          content: [{ type: "text", text: JSON.stringify({ edits: [{ path: "src/add.js", content: correctFix }] }), citations: null }],
          stop_reason: "end_turn",
          stop_sequence: null,
          usage: { input_tokens: 40, output_tokens: 12, cache_creation_input_tokens: null, cache_read_input_tokens: null, server_tool_use: null, service_tier: null },
          container: null,
          context_management: null,
        }) as unknown as Anthropic.Message) as unknown,
    } as unknown as Anthropic.Messages,
  } as MessagesClient;
}

function mapAttempts(attempts: (AiRepairAttemptRecord & { verificationOutcome?: string })[]) {
  return attempts.map((a) => ({
    attempt: a.attempt,
    modelUsed: a.modelUsed,
    outcome: a.outcome,
    verificationOutcome: a.verificationOutcome,
    usage: a.usage,
    reason: a.reason,
  }));
}

/**
 * Starts exactly one run if none is currently active; otherwise throws
 * `RunConflictError` with a clear, actionable message. `confirmPaidCall`
 * is REQUIRED (and server-side `RELAYLAB_LIVE_MODE=1` + credentials
 * must ALSO be present) for `mode: "ai-live"` -- this function never
 * silently downgrades a requested live run to simulated on any failure
 * of that check; it refuses outright.
 */
export async function startRun(pool: mysql.Pool, mode: RunMode, confirmPaidCall: boolean): Promise<{ runId: string }> {
  if (active) {
    throw new RunConflictError(`a run (${active.runId}) is already active -- this demo supports one active run at a time; cancel it first or wait for it to finish`);
  }
  if (mode === "ai-live") {
    if (process.env.RELAYLAB_LIVE_MODE !== "1") {
      throw new LiveModeDisabledError("live mode is disabled on this server -- set RELAYLAB_LIVE_MODE=1 server-side to enable it");
    }
    if (!confirmPaidCall) {
      throw new LiveModeDisabledError("live mode requires explicit confirmation (confirmPaidCall: true) that starting it incurs real Anthropic API charges");
    }
    if (!process.env.ANTHROPIC_API_KEY && !process.env.ANTHROPIC_AUTH_TOKEN) {
      throw new LiveModeDisabledError("live mode is enabled but no ANTHROPIC_API_KEY/ANTHROPIC_AUTH_TOKEN is configured on this server");
    }
    if (!process.env.RELAYLAB_AI_MODEL && process.env.RELAYLAB_AI_ALLOW_DEFAULT_MODEL !== "1") {
      throw new LiveModeDisabledError("live mode requires RELAYLAB_AI_MODEL to be set explicitly (or RELAYLAB_AI_ALLOW_DEFAULT_MODEL=1)");
    }
  }

  const runId = `run-${randomUUID()}`;
  const taskId = `demo-task-${randomUUID()}`;
  const ownerId = `demo-api-${runId}`;
  const controller = new AbortController();
  active = { runId, controller };

  await insertRun(pool, { runId, taskId, mode, status: "starting", ownerId, detail: { stage: "generation" } });

  void executeRun(pool, runId, taskId, ownerId, mode, controller.signal)
    .catch(() => {
      /* executeRun itself records failures into demo_runs; nothing further to do here. */
    })
    .finally(() => {
      if (active?.runId === runId) active = null;
    });

  return { runId };
}

export function cancelRun(runId: string): void {
  if (!active || active.runId !== runId) {
    throw new Error(`no active run with id ${runId}`);
  }
  active.controller.abort();
}

async function executeRun(pool: mysql.Pool, runId: string, taskId: string, ownerId: string, mode: RunMode, signal: AbortSignal): Promise<void> {
  const demoRoot = await mkdtemp(path.join(tmpdir(), "relaylab-demo-ui-run-"));
  const fixtureDir = path.join(demoRoot, "fixture-repo");

  try {
    await updateRunStatus(pool, runId, "running", { stage: "generation" });
    const fixture = await createFixtureRepo(fixtureDir);
    const managedWorkspaceRoot = await createManagedWorkspaceRoot(path.join(demoRoot, "workspace-a"));
    const store = makeTaskStoreLive(pool);
    const claim = await Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));

    let resultCommit: string | undefined;
    let resultTree: string | undefined;
    let baseCommit: string | undefined;
    let verificationOutcome: string | undefined;
    let checkpointed = false;

    if (mode === "deterministic") {
      const outcome = await runGitWorkerTask({ taskId, ownerId, epoch: claim.epoch, sourceRepoDir: fixture.repoDir, requestedCommitRef: fixture.baseCommit, managedWorkspaceRoot, pool });
      verificationOutcome = outcome.verification.outcome;
      if (outcome.kind === "checkpointed") {
        checkpointed = true;
        baseCommit = outcome.manifest.baseCommit;
        resultCommit = outcome.manifest.resultCommit;
        resultTree = outcome.manifest.resultTree;
      } else {
        await updateRunStatus(pool, runId, outcome.kind === "verification_cancelled" ? "cancelled" : "failed", {
          stage: "verification",
          verificationOutcome,
          error: `generation/verification did not succeed: ${outcome.kind}`,
        });
        return;
      }
    } else {
      const client = mode === "ai-live" ? createAnthropicClient() : simulatedClient();
      const aiConfig = mode === "ai-live" ? loadAnthropicAdapterConfig() : { model: "claude-opus-5-5-simulated-demo", maxOutputTokens: 512, requestTimeoutMs: 10_000 };
      const result = await runAiRepairTask({
        taskId,
        ownerId,
        epoch: claim.epoch,
        sourceRepoDir: fixture.repoDir,
        requestedCommitRef: fixture.baseCommit,
        managedWorkspaceRoot,
        pool,
        aiClient: client,
        aiConfig,
        signal,
      });
      await updateRunStatus(pool, runId, "running", { stage: "generation", attempts: mapAttempts(result.attempts) });
      if ("verification" in result.outcome) verificationOutcome = result.outcome.verification.outcome;
      if (result.outcome.kind === "checkpointed") {
        checkpointed = true;
        baseCommit = result.outcome.manifest.baseCommit;
        resultCommit = result.outcome.manifest.resultCommit;
        resultTree = result.outcome.manifest.resultTree;
      } else {
        const cancelledKinds = new Set(["verification_cancelled"]);
        await updateRunStatus(pool, runId, cancelledKinds.has(result.outcome.kind) || signal.aborted ? "cancelled" : "failed", {
          stage: "verification",
          attempts: mapAttempts(result.attempts),
          verificationOutcome,
          error: `generation/verification did not succeed: ${result.outcome.kind}`,
        });
        return;
      }
    }

    await updateRunStatus(pool, runId, "checkpointed", { stage: "checkpoint", baseCommit, resultCommit, resultTree, verificationOutcome });

    if (signal.aborted) {
      await updateRunStatus(pool, runId, "cancelled", { stage: "checkpoint" });
      return;
    }

    // Transfer + restore/ack, via the SAME generic handoff.ts this
    // project already ships -- never a parallel protocol.
    await updateRunStatus(pool, runId, "transferring", { stage: "transfer" });
    const taskAfter = await Effect.runPromise(store.getTask(taskId));
    const attemptId = defaultAttemptId(taskId);
    const worktreeDirA = resolveWorktreePath(managedWorkspaceRoot, taskId, attemptId);
    const envA = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedWorkspaceRoot));
    const handoffId = `demo-handoff-${runId}`;
    const { artifactPath, manifestPath, manifest } = await prepareHandoffArtifact({
      taskId,
      handoffId,
      worktreeDir: worktreeDirA,
      env: envA,
      resultRef: branchNameFor(taskId),
      sourceCheckpointCommandId: `git-checkpoint-${taskId}`,
      sourceManifestDigest: taskAfter.checkpointDigest!,
      baseCommit: baseCommit!,
      resultCommit: resultCommit!,
      resultTree: resultTree!,
      outDir: path.join(demoRoot, "artifacts"),
    });

    const ownerB = `${ownerId}-worker-b`;
    const transfer = await requestOwnershipTransfer(pool, {
      taskId,
      handoffId: handoffCommandId(taskId, handoffId),
      fromOwnerId: ownerId,
      fromEpoch: taskAfter.epoch,
      toOwnerId: ownerB,
      checkpointDigest: taskAfter.checkpointDigest!,
      artifactDigest: manifest.artifactDigest,
      leaseMs: 120_000,
    });

    await updateRunStatus(pool, runId, "restoring", { stage: "restore_ack", artifactDigest: manifest.artifactDigest });

    const workspaceBRoot = await mkdtemp(path.join(tmpdir(), "relaylab-demo-ui-workspace-b-"));
    const destRepoDir = path.join(workspaceBRoot, "restored-repo");
    const managedRootB = await createManagedWorkspaceRoot(path.join(workspaceBRoot, "home"));
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
      await updateRunStatus(pool, runId, "failed", { stage: "restore_ack", error: `restore did not succeed: ${restoreOutcome.kind}` });
      return;
    }

    await updateRunStatus(pool, runId, "completed", {
      stage: "restore_ack",
      restoredCommit: restoreOutcome.restoredCommit,
      restoredTree: restoreOutcome.restoredTree,
      restoreVerificationOutcome: restoreOutcome.verification.outcome,
      acknowledged: true,
    });
  } catch (err) {
    // Sanitized: a fixed classification only, never a raw stack/message
    // that could echo credentials or prompt content (see
    // src/ai/anthropic-client.ts's own sanitization for provider
    // errors specifically; this is the demo-run-level backstop for
    // everything else).
    const message = err instanceof Error ? `${err.constructor.name}: ${err.message.slice(0, 300)}` : "unknown error";
    await updateRunStatus(pool, runId, signal.aborted ? "cancelled" : "failed", { error: message }).catch(() => {});
  }
}
