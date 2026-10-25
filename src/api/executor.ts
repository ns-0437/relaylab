// Demo UI milestone: orchestrates ONE full run (generation ->
// verification -> checkpoint -> transfer -> restore/ack) for the local
// HTTP API, using the EXISTING pipeline end to end -- runGitWorkerTask /
// runAiRepairTask for generation+verification+checkpoint, and the
// EXISTING, unmodified handoff.ts APIs for transfer+restore+ack. This
// file adds no new execution primitive; it only sequences real ones and
// records progress into demo_runs for the UI to poll.
//
// Explicitly a SINGLE local in-process execution model: one run active
// per executor, tracked by an in-memory AbortController. This is NOT a
// distributed queue/worker pool -- restarting this process necessarily
// loses any in-flight run (there is nothing to resume a JS Promise
// from), which is exactly why reconcileOnStartup (runs-store.ts) exists.
//
// All mutable lifecycle state (the active reservation, the admission
// block, pending degraded terminal results) lives inside ONE executor
// instance created per API server by `createExecutor()` -- never in
// module globals -- so a second server lifecycle in the same process
// (tests do this; a future embedding could too) never inherits a
// previous lifecycle's permanent admission block or active slot.
import { randomUUID } from "node:crypto";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { Effect } from "effect";
import Anthropic from "@anthropic-ai/sdk";
import { makeTaskStoreLive } from "../db/task-store.js";
import { createFixtureRepo } from "../worker/fixture-repo.js";
import { createManagedWorkspaceRoot, resolveWorktreePath, resolveIsolatedHomeDir } from "../worker/workspace.js";
import { makeIsolatedGitEnv } from "../worker/git.js";
import { runGitWorkerTask, defaultAttemptId, branchNameFor } from "../worker/run-task.js";
import { runAiRepairTask, type AiProgressEvent } from "../worker/run-task-ai.js";
import type { AiRepairAttemptRecord } from "../worker/ai-repair.js";
import { createAnthropicClient, loadAnthropicAdapterConfig, AnthropicAdapterError, type MessagesClient } from "../ai/anthropic-client.js";
import { prepareHandoffArtifact, requestOwnershipTransfer, restoreAndAcknowledge, handoffCommandId } from "../worker/handoff.js";
import { insertRun, updateRunStatus, type RunDetail, type RunMode, type RunStatus } from "./runs-store.js";

export class RunConflictError extends Error {}
export class AdmissionBlockedError extends Error {}
export class LiveModeDisabledError extends Error {}
/** Carries a SANITIZED, fixed error code -- never a raw message built from an arbitrary caught error. See classifyError below. */
export class RunStartError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
  }
}
/** A progress write was refused because the row is already terminal -- a successor (or reconciliation) has decided this run's outcome; this executor must stop, never overwrite it. */
class StaleRunError extends Error {}

export type StageHookName = "generation" | "checkpoint" | "transfer" | "restore_ack";

/**
 * TEST-ONLY seams, reachable solely through `createApiServer(options)`
 * in-process -- never from an HTTP request. Production passes none.
 */
export interface ExecutorTestOptions {
  /** Replaces the simulated provider (never the live one) -- lets a test drive a deterministic failing response without any network call. */
  simulatedClient?: () => MessagesClient;
  /** Awaited immediately before each named stage begins; lets a test hold a run at a known point instead of guessing with sleeps. */
  beforeStage?: (stage: StageHookName, runId: string) => Promise<void>;
}

interface ActiveRun {
  runId: string;
  controller: AbortController;
  /** The ONE lifecycle promise for this reservation -- see startRun. */
  promise: Promise<void>;
}

interface PendingTerminal {
  status: RunStatus;
  patch: Partial<RunDetail>;
}

export interface Executor {
  startRun(pool: mysql.Pool, mode: RunMode, confirmPaidCall: boolean): Promise<{ runId: string }>;
  cancelRun(runId: string): void;
  activeRunId(): string | null;
  /** Awaited by graceful shutdown. Resolves immediately if nothing is active. */
  awaitActiveRun(): Promise<void>;
  /** Permanent for THIS executor -- used on confirmed lock loss and during shutdown. */
  blockAdmission(): void;
  cancelActiveRunForLockLoss(): void;
  /** Sanitized note for a run whose terminal outcome is known in memory but not yet durably recorded. */
  degradedRunNote(runId: string): string | undefined;
  /** Bounded, non-overlapping retry of pending terminal writes; safe to call often (throttled internally). */
  reconcileDegraded(pool: mysql.Pool): Promise<void>;
}

const DEGRADED_NOTE = "outcome not yet durably recorded -- the database write for this run's final status failed; it will be retried automatically once storage is reachable";
const RECONCILE_MIN_INTERVAL_MS = 1_000;

/** The ONE simulated response the demo mode's fake client returns -- the real, correct fix, clearly labeled. Never used for "ai-live". */
function defaultSimulatedClient(): MessagesClient {
  const correctFix = "function add(a, b) {\n  return a + b;\n}\n\nmodule.exports = { add };\n";
  return simulatedClientReturning(correctFix);
}

/** Builds a no-network fake provider that always proposes `fileContent` for src/add.js. */
export function simulatedClientReturning(fileContent: string): MessagesClient {
  return {
    messages: {
      create: (async () =>
        ({
          id: "msg_demo_simulated",
          type: "message",
          role: "assistant",
          model: "claude-opus-5-5-simulated-demo",
          content: [{ type: "text", text: JSON.stringify({ edits: [{ path: "src/add.js", content: fileContent }] }), citations: null }],
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

export function createExecutor(testOptions: ExecutorTestOptions = {}): Executor {
  let active: ActiveRun | null = null;
  let admissionBlocked = false;
  const pendingTerminal = new Map<string, PendingTerminal>();
  let reconciling: Promise<void> | null = null;
  let lastReconcileAt = 0;

  /** Non-terminal transition: refused (row already terminal) means a successor decided this run -- stop. */
  async function write(pool: mysql.Pool, runId: string, status: RunStatus, patch: Partial<RunDetail>): Promise<void> {
    const applied = await updateRunStatus(pool, runId, status, patch);
    if (!applied) throw new StaleRunError(`run ${runId} already has a terminal status recorded elsewhere`);
  }

  /** Best-effort progress (attempt/verification-start): never aborts the run on a storage blip, never overwrites a terminal row. */
  async function progress(pool: mysql.Pool, runId: string, patch: Partial<RunDetail>): Promise<void> {
    await updateRunStatus(pool, runId, "running", patch).catch(() => false);
  }

  /** Terminal transition: never throws. A storage failure keeps the SANITIZED result in memory for later reconciliation. */
  async function writeTerminal(pool: mysql.Pool, runId: string, status: RunStatus, patch: Partial<RunDetail>): Promise<void> {
    try {
      await updateRunStatus(pool, runId, status, patch);
      pendingTerminal.delete(runId);
    } catch {
      pendingTerminal.set(runId, { status, patch });
    }
  }

  async function hold(stage: StageHookName, runId: string): Promise<void> {
    if (testOptions.beforeStage) await testOptions.beforeStage(stage, runId);
  }

  async function executeRun(pool: mysql.Pool, runId: string, taskId: string, ownerId: string, mode: RunMode, signal: AbortSignal): Promise<void> {
    try {
      await hold("generation", runId);
      await write(pool, runId, "running", { stage: "generation" });

      // Temporary-directory setup is INSIDE the guarded lifecycle -- a
      // failure here (disk full, permissions) is caught below and
      // recorded as a real "failed" status, never left silently stuck.
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

      if (mode === "deterministic") {
        const outcome = await runGitWorkerTask({
          taskId,
          ownerId,
          epoch: claim.epoch,
          sourceRepoDir: fixture.repoDir,
          requestedCommitRef: fixture.baseCommit,
          managedWorkspaceRoot,
          pool,
          signal,
          // Recorded the moment Docker confirms the verification
          // container is running -- not after the worker returns.
          onVerifyContainerCreated: () => progress(pool, runId, { stage: "verification" }),
        });
        verificationOutcome = outcome.verification.outcome;
        if (outcome.kind !== "checkpointed") {
          // The deterministic repair step itself cannot fail (a fixed,
          // local substitution) -- every non-checkpointed outcome here
          // genuinely reached real verification.
          await writeTerminal(pool, runId, outcome.kind === "verification_cancelled" || signal.aborted ? "cancelled" : "failed", {
            stage: "verification",
            verificationOutcome,
            error: `verification did not succeed: ${outcome.kind}`,
          });
          return;
        }
        baseCommit = outcome.manifest.baseCommit;
        resultCommit = outcome.manifest.resultCommit;
        resultTree = outcome.manifest.resultTree;
      } else {
        const client = mode === "ai-live" ? createAnthropicClient() : (testOptions.simulatedClient ?? defaultSimulatedClient)();
        const aiConfig = mode === "ai-live" ? loadAnthropicAdapterConfig() : { model: "claude-opus-5-5-simulated-demo", maxOutputTokens: 512, requestTimeoutMs: 10_000 };
        // Each transition is persisted AS IT HAPPENS. A retry after a
        // failed verification moves the stage back to "generation" with
        // the earlier attempt's real "failed" verification outcome still
        // listed -- never presented as a successful stage.
        const onProgress = async (event: AiProgressEvent): Promise<void> => {
          const attempts = mapAttempts(event.attempts);
          if (event.kind === "verification_started" || event.kind === "attempt_verified") {
            await progress(pool, runId, { stage: "verification", attempts });
          } else {
            await progress(pool, runId, { stage: "generation", attempts });
          }
        };
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
          onProgress,
        });
        if ("verification" in result.outcome) verificationOutcome = result.outcome.verification.outcome;
        if (result.outcome.kind !== "checkpointed") {
          // FACTUAL stage: only an attempt that actually produced a
          // verificationOutcome proves real verification occurred.
          const anyVerified = result.attempts.some((a) => a.verificationOutcome !== undefined);
          await writeTerminal(pool, runId, result.outcome.kind === "verification_cancelled" || signal.aborted ? "cancelled" : "failed", {
            stage: anyVerified ? "verification" : "generation",
            attempts: mapAttempts(result.attempts),
            verificationOutcome,
            error: `${anyVerified ? "verification" : "generation"} did not succeed: ${result.outcome.kind}`,
          });
          return;
        }
        baseCommit = result.outcome.manifest.baseCommit;
        resultCommit = result.outcome.manifest.resultCommit;
        resultTree = result.outcome.manifest.resultTree;
        await progress(pool, runId, { attempts: mapAttempts(result.attempts) });
      }

      // Checkpoint success is derived from DURABLE state: the task row's
      // recorded checkpoint digest, not merely the worker's return value.
      const taskAfter = await Effect.runPromise(store.getTask(taskId));
      if (!taskAfter.checkpointDigest) throw new Error("worker reported a checkpoint but no durable checkpoint digest is recorded");
      await hold("checkpoint", runId);
      await write(pool, runId, "checkpointed", { stage: "checkpoint", baseCommit, resultCommit, resultTree, verificationOutcome, checkpointDigest: taskAfter.checkpointDigest });

      // Checked before starting the NEXT stage -- a checkpoint that
      // already published stays reported; this only stops progress.
      await hold("transfer", runId);
      if (signal.aborted) {
        await writeTerminal(pool, runId, "cancelled", { stage: "transfer", error: "cancelled after the checkpoint was published; no transfer was started" });
        return;
      }

      await write(pool, runId, "transferring", { stage: "transfer" });
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
        sourceManifestDigest: taskAfter.checkpointDigest,
        baseCommit: baseCommit!,
        resultCommit: resultCommit!,
        resultTree: resultTree!,
        outDir: path.join(demoRoot, "artifacts"),
      });

      if (signal.aborted) {
        await writeTerminal(pool, runId, "cancelled", { stage: "transfer", artifactDigest: manifest.artifactDigest });
        return;
      }

      const ownerB = `${ownerId}-worker-b`;
      const transfer = await requestOwnershipTransfer(pool, {
        taskId,
        handoffId: handoffCommandId(taskId, handoffId),
        fromOwnerId: ownerId,
        fromEpoch: taskAfter.epoch,
        toOwnerId: ownerB,
        checkpointDigest: taskAfter.checkpointDigest,
        artifactDigest: manifest.artifactDigest,
        leaseMs: 120_000,
      });

      await hold("restore_ack", runId);
      await write(pool, runId, "restoring", { stage: "restore_ack", artifactDigest: manifest.artifactDigest });
      if (signal.aborted) {
        await writeTerminal(pool, runId, "cancelled", { stage: "restore_ack" });
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

      // Acknowledgement is read back from the durable handoff row, never
      // inferred from the function's return value alone.
      const [ackRows] = await pool.query<RowDataPacket[]>(`SELECT restore_ack_command_id FROM task_handoffs WHERE task_id = ?`, [taskId]);
      const acknowledged = ackRows.some((r) => r.restore_ack_command_id !== null);

      if (restoreOutcome.kind !== "restored" || !acknowledged) {
        await writeTerminal(pool, runId, restoreOutcome.kind === "verification_cancelled" || signal.aborted ? "cancelled" : "failed", {
          stage: "restore_ack",
          acknowledged,
          error: `restore/acknowledge did not succeed: ${restoreOutcome.kind}${restoreOutcome.kind === "restored" ? " (no durable acknowledgement found)" : ""}`,
        });
        return;
      }

      await writeTerminal(pool, runId, "completed", {
        stage: "restore_ack",
        restoredCommit: restoreOutcome.restoredCommit,
        restoredTree: restoreOutcome.restoredTree,
        restoreVerificationOutcome: restoreOutcome.verification.outcome,
        acknowledged: true,
      });
    } catch (err) {
      if (err instanceof StaleRunError) return; // a successor owns this row's outcome now -- never overwrite it
      const classified = classifyError(err);
      await writeTerminal(pool, runId, signal.aborted ? "cancelled" : "failed", { error: `${classified.code}: ${classified.message}` });
    }
  }

  return {
    activeRunId: () => active?.runId ?? null,

    blockAdmission: () => {
      admissionBlocked = true;
    },

    cancelActiveRunForLockLoss: () => {
      active?.controller.abort();
    },

    awaitActiveRun: async () => {
      if (active) await active.promise.catch(() => {});
    },

    degradedRunNote: (runId) => (pendingTerminal.has(runId) ? DEGRADED_NOTE : undefined),

    reconcileDegraded: (pool) => {
      if (reconciling) return reconciling;
      if (pendingTerminal.size === 0 || Date.now() - lastReconcileAt < RECONCILE_MIN_INTERVAL_MS) return Promise.resolve();
      lastReconcileAt = Date.now();
      reconciling = (async () => {
        for (const [runId, pending] of [...pendingTerminal]) {
          try {
            // Refused (row already terminal -- e.g. a successor marked it
            // interrupted) counts as settled: never overwrite it.
            await updateRunStatus(pool, runId, pending.status, pending.patch);
            pendingTerminal.delete(runId);
          } catch {
            // storage still unreachable -- keep it for the next bounded attempt
          }
        }
      })().finally(() => {
        reconciling = null;
      });
      return reconciling;
    },

    cancelRun: (runId) => {
      if (!active || active.runId !== runId) {
        throw new Error(`no active run with id ${runId}`);
      }
      active.controller.abort();
    },

    /**
     * Starts exactly one run if none is active; otherwise throws
     * `RunConflictError`. `confirmPaidCall` is REQUIRED (and server-side
     * `RELAYLAB_LIVE_MODE=1` + credentials must ALSO be present) for
     * `mode: "ai-live"` -- never silently downgraded to simulated.
     *
     * The reservation is set SYNCHRONOUSLY, before the first `await`, so
     * two concurrent calls can never both pass the `active` check.
     * `reservation.promise` is ONE deferred promise whose identity never
     * changes; it settles exactly once, when the whole lifecycle
     * (insertion, plus execution and cleanup if insertion succeeded) is
     * finished. `awaitActiveRun` can therefore never observe completion
     * early, whichever phase it happens to be called in.
     */
    startRun: async (pool, mode, confirmPaidCall) => {
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

      let settleReservation!: () => void;
      const reservation: ActiveRun = { runId, controller, promise: new Promise<void>((resolve) => (settleReservation = resolve)) };
      active = reservation;
      const release = () => {
        if (active === reservation) active = null;
        settleReservation();
      };

      try {
        await insertRun(pool, { runId, taskId, mode, status: "starting", ownerId, detail: { stage: "generation" } });
      } catch (err) {
        release();
        const classified = classifyError(err);
        throw new RunStartError(classified.code, classified.message);
      }

      if (controller.signal.aborted) {
        // Cancelled while insertion was still pending: normal execution
        // must never begin. Recording it (and settling the lifecycle)
        // happens in the background, same as the normal execution path
        // below -- startRun itself still returns {runId} promptly; only
        // awaitActiveRun (the SAME lifecycle promise) waits for the
        // write to actually finish.
        void writeTerminal(pool, runId, "cancelled", { stage: "generation", error: "cancelled before this run's own insertion had finished recording it" }).finally(release);
        return { runId };
      }

      void executeRun(pool, runId, taskId, ownerId, mode, controller.signal)
        .catch(() => {
          /* executeRun records its own outcome; nothing further to do here. */
        })
        .finally(release);

      return { runId };
    },
  };
}
