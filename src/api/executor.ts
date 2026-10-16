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
import { createAnthropicClient, loadAnthropicAdapterConfig, AnthropicAdapterError, type MessagesClient } from "../ai/anthropic-client.js";
import { prepareHandoffArtifact, requestOwnershipTransfer, restoreAndAcknowledge, handoffCommandId } from "../worker/handoff.js";
import { insertRun, updateRunStatus, type RunDetail, type RunMode } from "./runs-store.js";

export class RunConflictError extends Error {}
export class AdmissionBlockedError extends Error {}
export class LiveModeDisabledError extends Error {}
/** Carries a SANITIZED, fixed error code -- never a raw message built from an arbitrary caught error. See classifyError below. */
export class RunStartError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
  }
}

interface ActiveRun {
  runId: string;
  controller: AbortController;
  /** Awaited by graceful shutdown (server.ts) before the process exits -- "await execution cleanup before releasing the active slot" applies here too: shutdown never exits while this is still settling. */
  promise: Promise<void>;
}

let active: ActiveRun | null = null;

export function activeRunId(): string | null {
  return active?.runId ?? null;
}

/**
 * Blocked permanently once set -- used when this process can no longer
 * establish ownership of the singleton lock (confirmed loss, or the
 * conservative expiry watchdog), and during graceful shutdown. A
 * STALE instance that has lost the lock must never admit a new run
 * (which would let it overwrite a SUCCESSOR instance's lifecycle
 * state in demo_runs) -- there is no "unblock" function; a new process
 * is required.
 */
let admissionBlocked = false;
export function blockAdmission(): void {
  admissionBlocked = true;
}

/** Aborts the currently active run's controller, if any -- used when the singleton lock is confirmed lost, so a stale instance's execution stops promptly rather than running to completion and overwriting a successor's view of demo_runs. */
export function cancelActiveRunForLockLoss(): void {
  active?.controller.abort();
}

/** Awaited by graceful shutdown. Resolves immediately if nothing is active. */
export async function awaitActiveRun(): Promise<void> {
  if (active) await active.promise.catch(() => {});
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
 * Sanitizes ANY caught error into a fixed {code, message} pair -- never
 * echoes arbitrary `err.message` content (which can carry filesystem
 * paths, Git/Docker CLI output, or in rare cases provider response
 * fragments) into a message this module persists or returns. Known,
 * self-authored error types (whose message text this module itself
 * wrote, never derived from external input) pass their message through
 * unchanged; everything else maps to one of a small, fixed set of
 * actionable, generic messages by broad category.
 */
function classifyError(err: unknown): { code: string; message: string } {
  if (err instanceof RunConflictError) return { code: "run_conflict", message: err.message };
  if (err instanceof LiveModeDisabledError) return { code: "live_mode_disabled", message: err.message };
  if (err instanceof AnthropicAdapterError) return { code: "provider_error", message: err.message }; // already sanitized at the source
  const mysqlCode = err && typeof err === "object" && "code" in err ? String((err as { code: unknown }).code) : undefined;
  if (mysqlCode?.startsWith("ER_") || mysqlCode === "ECONNREFUSED" || mysqlCode === "PROTOCOL_CONNECTION_LOST" || mysqlCode === "ETIMEDOUT") {
    return { code: "storage_error", message: "a database error occurred while recording this run -- the database may be unreachable or misconfigured" };
  }
  if (mysqlCode === "ENOENT" || mysqlCode === "EACCES" || mysqlCode === "EPERM") {
    return { code: "filesystem_error", message: "a filesystem error occurred preparing this run's working directory" };
  }
  return { code: "internal_error", message: "an internal error occurred running this task" };
}

/**
 * Starts exactly one run if none is currently active; otherwise throws
 * `RunConflictError`. `confirmPaidCall` is REQUIRED (and server-side
 * `RELAYLAB_LIVE_MODE=1` + credentials must ALSO be present) for
 * `mode: "ai-live"` -- this function never silently downgrades a
 * requested live run to simulated on any failure of that check; it
 * refuses outright.
 *
 * CONFIRMED real defect, fixed here: `active` used to be set only AFTER
 * `await insertRun(...)` resolved, leaving a window where two
 * concurrent calls could both pass the `if (active)` check above and
 * both proceed to insert/execute. `active` is now reserved
 * SYNCHRONOUSLY, as the very next statement after that check and
 * BEFORE this function's first `await` -- a second concurrent call
 * cannot observe `active` as unset once the first call's synchronous
 * prefix has run, which JavaScript's own run-to-completion semantics
 * guarantee happens before the second call's own synchronous prefix
 * ever starts. The reservation's own `promise` field covers BOTH the
 * pending-insertion window and (once insertion succeeds) real
 * execution, so a failed insertion still releases it correctly, and
 * `awaitActiveRun` (graceful shutdown) can await either phase.
 */
export async function startRun(pool: mysql.Pool, mode: RunMode, confirmPaidCall: boolean): Promise<{ runId: string }> {
  if (admissionBlocked) {
    throw new AdmissionBlockedError("this server instance can no longer establish ownership of the singleton lock (or is shutting down) -- refusing to start a new run");
  }
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

  // One stable promise covers insertion AND execution. Shutdown may already
  // be awaiting this promise while insertion is pending; never replace it.
  let releasePending: () => void;
  const reservation: ActiveRun = { runId, controller, promise: new Promise<void>((resolve) => (releasePending = resolve)) };
  active = reservation;

  try {
    await insertRun(pool, { runId, taskId, mode, status: "starting", ownerId, detail: { stage: "generation" } });
  } catch (err) {
    // Release ONLY this reservation -- and only if it is STILL the
    // active one (defensive; nothing else can legitimately have
    // replaced it mid-insert, but never clear a DIFFERENT reservation
    // by accident).
    if (active === reservation) active = null;
    releasePending!();
    const classified = classifyError(err);
    throw new RunStartError(classified.code, classified.message);
  }

  const execPromise = executeRun(pool, runId, taskId, ownerId, mode, controller.signal).catch(() => {
    /* executeRun itself records failures into demo_runs; nothing further to do here. */
  });
  void execPromise.finally(() => {
    if (active === reservation) active = null;
    releasePending!();
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
  try {
    if (signal.aborted) {
      await updateRunStatus(pool, runId, "cancelled", { stage: "generation" });
      return;
    }
    await updateRunStatus(pool, runId, "running", { stage: "generation" });

    // Temporary-directory setup is now INSIDE the guarded lifecycle --
    // a failure here (disk full, permissions) is caught below and
    // recorded as a real "failed" status, never left silently stuck at
    // "starting"/"running" forever with no explanation.
    const demoRoot = await mkdtemp(path.join(tmpdir(), "relaylab-demo-ui-run-"));
    const fixtureDir = path.join(demoRoot, "fixture-repo");

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
      const outcome = await runGitWorkerTask({ taskId, ownerId, epoch: claim.epoch, sourceRepoDir: fixture.repoDir, requestedCommitRef: fixture.baseCommit, managedWorkspaceRoot, pool, signal });
      verificationOutcome = outcome.verification.outcome;
      if (outcome.kind === "checkpointed") {
        checkpointed = true;
        baseCommit = outcome.manifest.baseCommit;
        resultCommit = outcome.manifest.resultCommit;
        resultTree = outcome.manifest.resultTree;
      } else {
        // The deterministic repair step itself cannot fail (it is a
        // fixed, local string substitution) -- every non-checkpointed
        // outcome here genuinely reached real verification, so "stage:
        // verification" is factual, never assumed.
        await updateRunStatus(pool, runId, outcome.kind === "verification_cancelled" ? "cancelled" : "failed", {
          stage: "verification",
          verificationOutcome,
          error: `verification did not succeed: ${outcome.kind}`,
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
      if ("verification" in result.outcome) verificationOutcome = result.outcome.verification.outcome;
      if (result.outcome.kind === "checkpointed") {
        checkpointed = true;
        baseCommit = result.outcome.manifest.baseCommit;
        resultCommit = result.outcome.manifest.resultCommit;
        resultTree = result.outcome.manifest.resultTree;
        await updateRunStatus(pool, runId, "running", { stage: "checkpoint", attempts: mapAttempts(result.attempts) });
      } else {
        // FACTUAL stage, derived from what actually happened: if no
        // attempt ever reached a real verification run (every attempt
        // was rejected/no_change/a provider error before a commit ever
        // existed to verify), this never ran verification at all --
        // "stage: generation" stays accurate. Only an attempt that
        // actually produced a verificationOutcome proves real
        // verification occurred.
        const anyVerified = result.attempts.some((a) => a.verificationOutcome !== undefined);
        const cancelledKinds = new Set(["verification_cancelled"]);
        await updateRunStatus(pool, runId, cancelledKinds.has(result.outcome.kind) || signal.aborted ? "cancelled" : "failed", {
          stage: anyVerified ? "verification" : "generation",
          attempts: mapAttempts(result.attempts),
          verificationOutcome,
          error: `${anyVerified ? "verification" : "generation"} did not succeed: ${result.outcome.kind}`,
        });
        return;
      }
    }

    await updateRunStatus(pool, runId, "checkpointed", { stage: "checkpoint", baseCommit, resultCommit, resultTree, verificationOutcome });

    // Checked before starting the NEXT stage (transfer) -- a checkpoint
    // that already published is real and stays reported; this only
    // stops the run from proceeding further.
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

    // Checked again before requesting ownership transfer.
    if (signal.aborted) {
      await updateRunStatus(pool, runId, "cancelled", { stage: "transfer", artifactDigest: manifest.artifactDigest });
      return;
    }

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

    // Checked before worker B's restore/re-verify/acknowledge begins.
    if (signal.aborted) {
      await updateRunStatus(pool, runId, "cancelled", { stage: "restore_ack", artifactDigest: manifest.artifactDigest });
      return;
    }

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
      signal,
    });

    if (restoreOutcome.kind !== "restored") {
      // restoreAndAcknowledge itself NEVER writes the acknowledgement
      // unless it fully succeeds -- any non-"restored" outcome here
      // (including one caused by cancellation during B's
      // re-verification, which restoreAndAcknowledge checks explicitly
      // before ever writing the ack) means no durable acknowledgement
      // exists, so reporting this as cancelled/failed (never
      // "completed") is accurate, not a guess.
      const cancelledKinds = new Set(["verification_cancelled"]);
      await updateRunStatus(pool, runId, cancelledKinds.has(restoreOutcome.kind) || signal.aborted ? "cancelled" : "failed", {
        stage: "restore_ack",
        error: `restore/acknowledge did not succeed: ${restoreOutcome.kind}`,
      });
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
    const classified = classifyError(err);
    try {
      await updateRunStatus(pool, runId, signal.aborted ? "cancelled" : "failed", { error: `${classified.code}: ${classified.message}` });
    } catch {
      // A failure to even PERSIST the terminal status must not throw
      // out of this catch block uncaught (that would be an unhandled
      // rejection in the detached executeRun promise) -- but it is also
      // never silently treated as success. Recorded in-memory as a
      // SANITIZED degraded marker (never the raw error) so GET
      // /api/runs/:id can surface "this run's true final status could
      // not be durably recorded" instead of showing a stale
      // "running"/"transferring" row with no explanation. Cleared once
      // storage recovers and reconcileOnStartup runs on the NEXT
      // restart (the authoritative recovery path); this marker is only
      // ever a same-process, same-run, best-effort annotation.
      degradedRuns.set(runId, "this run's terminal status could not be durably recorded -- the database write itself failed after the run finished");
    }
  }
}

/** Sanitized, in-memory ONLY -- never a substitute for the durable demo_runs row, which remains the authoritative source once storage recovers. See the catch block above. */
const degradedRuns = new Map<string, string>();
export function degradedRunNote(runId: string): string | undefined {
  return degradedRuns.get(runId);
}
