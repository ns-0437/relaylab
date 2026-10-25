// AI repair milestone: an explicit AI repair mode alongside
// run-task.ts's deterministic one. Deliberately a SEPARATE function,
// not a branch inside `runGitWorkerTask` -- the deterministic path's
// already-validated crash-reconciliation state machine
// (workspace-classify.ts) is untouched by this file. This path reuses
// every other piece of the existing pipeline directly: the same
// worktree/identity primitives, the same `commitAllowedPaths` (so the
// path allowlist is enforced a SECOND time at the Git layer,
// independently of ai-repair.ts's own check), the same Docker
// verification, and the same checkpoint manifest/publish code run-task.ts
// uses. Nothing here bypasses verification or weakens an identity check.
//
// Bounded execution: at most MAX_AI_ATTEMPTS (2) end-to-end attempts
// (generate edit -> commit -> verify). A failed attempt's bounded
// verification diagnostics (truncated stdout/stderr, never the full
// untruncated log) are fed into the next attempt's prompt. An overall
// deadline and the caller's own AbortSignal are combined and propagated
// into both the provider call and Docker verification, and the task's
// claim lease is explicitly renewed before each attempt so the SAME
// epoch-fencing TaskStore already enforces (never weakened or bypassed
// here) is what actually stops a would-be checkpoint publish once
// ownership has genuinely moved on.
import { realpath, stat } from "node:fs/promises";
import path from "node:path";
import { Cause, Effect, Exit } from "effect";
import { TaskStore, TaskStoreLive } from "../db/task-store.js";
import { ClaimLost, EpochStale } from "../domain/errors.js";
import {
  addWorktree,
  changedPaths,
  changedPathsBetweenCommits,
  commitAllowedPaths,
  commitCountBetween,
  currentCommit,
  currentTree,
  hasUncommittedChanges,
  isAncestor,
  makeIsolatedGitEnv,
  resetHard,
  resolveCommit,
} from "./git.js";
import { assertNoSymlinkEscapes, assertWithinRoot, ensureDir, resolveIsolatedHomeDir, resolveWorktreePath } from "./workspace.js";
import { TARGET_RELATIVE_PATH } from "./repair.js";
import { attemptAiRepair, AI_REPAIR_NAME, type AiRepairAttemptRecord } from "./ai-repair.js";
import { createAnthropicClient, loadAnthropicAdapterConfig, type AnthropicAdapterConfig, type MessagesClient } from "../ai/anthropic-client.js";
import { buildManifest, buildRuntimeFingerprint, publishCheckpoint } from "./checkpoint.js";
import { assertIdentityMatches, readWorktreeIdentity, writeWorktreeIdentity } from "./worktree-identity.js";
import { getTracer } from "../telemetry/init.js";
import { assertMaterializedTreeMatches, materializeCandidateInput, verifyInDockerWithImageId } from "./docker-verify.js";
import type { RunOutcome, RunTaskArgs } from "./run-task.js";
import { branchNameFor, checkpointCommandId, defaultAttemptId } from "./run-task.js";

export const MAX_AI_ATTEMPTS = 2;
/** Bounded diagnostics fed into a retry prompt -- never the full, untruncated stdout/stderr. */
const MAX_DIAGNOSTICS_CHARS = 1_000;
/**
 * The existing task-claim lease this project's workers use is 120s (see
 * tests/integration/git-worker.test.ts's own `claim(...)` calls). This
 * is comfortably BELOW that, leaving real margin for Docker
 * verification time; it is also explicitly RENEWED (see
 * `renewLeaseOrStop` below) before every attempt, so a slow run is
 * covered by bounded renewal, not merely a generous one-shot budget.
 */
export const DEFAULT_OVERALL_DEADLINE_MS = 100_000;
const LEASE_RENEWAL_MS = 120_000;

export interface AiRunOutcome {
  outcome:
    | RunOutcome
    /** The claim lease could not be renewed (another owner took over, or the epoch moved) -- this is TaskStore's OWN fencing (ClaimLost/EpochStale), surfaced here, never weakened or second-guessed. Never checkpointed. */
    | { kind: "ownership_lost"; reason: string }
    /** The overall deadline elapsed before a result was reached. Never checkpointed. */
    | { kind: "deadline_exceeded" };
  /**
   * Every attempt's identity/model/outcome/usage, PLUS (once known) the
   * verification outcome that attempt's commit actually received --
   * "edit_applied" alone never stands in as the attempt's final result.
   * Never the prompt or response text (see ai-repair.ts).
   */
  attempts: (AiRepairAttemptRecord & { verificationOutcome?: string })[];
}

/** Emitted as each transition happens, carrying a snapshot of every attempt so far. Never carries prompt/response text. */
export interface AiProgressEvent {
  kind: "attempt_started" | "attempt_generated" | "verification_started" | "attempt_verified";
  attempt: number;
  attempts: (AiRepairAttemptRecord & { verificationOutcome?: string })[];
}

export interface RunAiRepairTaskArgs extends RunTaskArgs {
  /** Awaited at each transition; a throwing callback is ignored -- progress reporting never changes the repair outcome. */
  onProgress?: (event: AiProgressEvent) => void | Promise<void>;
  /** Injectable for tests -- defaults to the real `createAnthropicClient()` (reads credentials from the environment only). */
  aiClient?: MessagesClient;
  aiConfig?: AnthropicAdapterConfig;
  /** Lets a caller cancel an in-flight provider call or Docker verification between/during attempts. */
  signal?: AbortSignal;
  /** Overrides `DEFAULT_OVERALL_DEADLINE_MS` -- tests use a short one to exercise the deadline path without a real 100s wait. */
  overallDeadlineMs?: number;
}

function truncateForDiagnostics(text: string): string {
  return text.length > MAX_DIAGNOSTICS_CHARS ? text.slice(0, MAX_DIAGNOSTICS_CHARS) + "\n...[truncated]" : text;
}

/** Combines the caller's own cancellation signal with a per-step deadline-derived timeout, so both reach the provider call / Docker verification identically. */
function stepSignal(callerSignal: AbortSignal | undefined, deadlineAt: number): AbortSignal {
  const remaining = Math.max(0, deadlineAt - Date.now());
  const signals = [AbortSignal.timeout(remaining)];
  if (callerSignal) signals.push(callerSignal);
  return AbortSignal.any(signals);
}

/**
 * A caller-requested cancellation and this function's OWN overall
 * deadline elapsing are distinct causes -- conflating them into one
 * outcome would mislabel an explicit user cancel (e.g. the demo API's
 * "cancel run" button) as a generic "deadline_exceeded" timeout, which
 * is misleading to anything reporting on it. Returns `null` when
 * neither condition holds.
 */
function checkCancelledOrDeadline(callerSignal: AbortSignal | undefined, deadlineAt: number): { kind: "cancelled" } | { kind: "deadline" } | null {
  if (callerSignal?.aborted) return { kind: "cancelled" };
  if (Date.now() >= deadlineAt) return { kind: "deadline" };
  return null;
}

type WorkspaceReuseState =
  | { kind: "ready" }
  | { kind: "edit_pending_commit" }
  | { kind: "committed"; head: string }
  | { kind: "unexpected"; reason: string };

/**
 * Generalized analogue of workspace-classify.ts's A-E state table, but
 * for AI-generated content (which has no single known fixed string to
 * compare against). Reuses the SAME underlying primitives
 * (`changedPaths`, `isAncestor`, `commitCountBetween`) that module
 * does. Positively classifies a reused worktree into one of four named
 * states; anything not matching one of the first three is "unexpected"
 * and is REFUSED, never discarded/reset -- only a state THIS function
 * run (within the SAME call's own retry loop, after a confirmed failed
 * verification) ever calls `resetHard`.
 */
async function classifyAiWorkspace(worktreeDir: string, baseCommit: string, env: NodeJS.ProcessEnv): Promise<WorkspaceReuseState> {
  const changed = await changedPaths(worktreeDir, env);
  const head = await currentCommit(worktreeDir, env);

  if (changed.length === 0) {
    if (head === baseCommit) return { kind: "ready" };
    const descends = await isAncestor(worktreeDir, baseCommit, head, env);
    if (!descends) {
      return { kind: "unexpected", reason: `HEAD ("${head}") is not a descendant of the base commit ("${baseCommit}")` };
    }
    const commitsSinceBase = await commitCountBetween(worktreeDir, baseCommit, head, env);
    if (commitsSinceBase !== 1) {
      return { kind: "unexpected", reason: `expected exactly 1 commit between base and HEAD for a resumable AI attempt, found ${commitsSinceBase}` };
    }
    const diffPaths = await changedPathsBetweenCommits(worktreeDir, baseCommit, head, env);
    const unexpectedDiff = diffPaths.filter((p) => p !== TARGET_RELATIVE_PATH);
    if (diffPaths.length === 0 || unexpectedDiff.length > 0) {
      return { kind: "unexpected", reason: `committed diff between base and HEAD touches path(s) other than the allowlisted target: ${diffPaths.join(", ") || "(none)"}` };
    }
    return { kind: "committed", head };
  }

  if (head !== baseCommit) {
    return { kind: "unexpected", reason: `worktree is dirty AND HEAD ("${head}") is not the base commit ("${baseCommit}") -- refusing to reconcile an ambiguous mixed state` };
  }
  const unexpected = changed.filter((entry) => entry.path !== TARGET_RELATIVE_PATH);
  if (unexpected.length > 0) {
    return { kind: "unexpected", reason: `worktree has changed path(s) outside the allowlisted target: ${unexpected.map((e) => `${e.status} ${e.path}`).join(", ")}` };
  }
  return { kind: "edit_pending_commit" };
}

export async function runAiRepairTask(args: RunAiRepairTaskArgs): Promise<AiRunOutcome> {
  const tracer = getTracer();
  return tracer.startActiveSpan("relaylab.worker.run_ai_repair_task", async (span): Promise<AiRunOutcome> => {
    const attempts: (AiRepairAttemptRecord & { verificationOutcome?: string })[] = [];
    const report = async (kind: AiProgressEvent["kind"], attempt: number): Promise<void> => {
      try {
        await args.onProgress?.({ kind, attempt, attempts: attempts.map((a) => ({ ...a })) });
      } catch {
        // progress reporting is observational only
      }
    };
    try {
      span.setAttribute("relaylab.task_id", args.taskId);
      span.setAttribute("relaylab.repair_mode", "ai");

      const client = args.aiClient ?? createAnthropicClient();
      const config = args.aiConfig ?? loadAnthropicAdapterConfig();
      const deadlineAt = Date.now() + (args.overallDeadlineMs ?? DEFAULT_OVERALL_DEADLINE_MS);

      const attemptId = defaultAttemptId(args.taskId);
      const isolatedHome = resolveIsolatedHomeDir(args.managedWorkspaceRoot);
      await ensureDir(isolatedHome);
      const env = makeIsolatedGitEnv(isolatedHome);

      const baseCommit = await resolveCommit(args.sourceRepoDir, args.requestedCommitRef, env);
      const worktreeDir = resolveWorktreePath(args.managedWorkspaceRoot, args.taskId, attemptId);
      assertWithinRoot(args.managedWorkspaceRoot, worktreeDir);
      await assertNoSymlinkEscapes(args.managedWorkspaceRoot, worktreeDir);

      const branch = branchNameFor(args.taskId);
      const sourceRepoRealPath = await realpath(args.sourceRepoDir);
      const expectedIdentity = { taskId: args.taskId, attemptId, sourceRepoRealPath, baseCommit, branch };

      const exists = await stat(worktreeDir).then(
        () => true,
        () => false,
      );
      let reuse: WorkspaceReuseState = { kind: "ready" };
      if (!exists) {
        await ensureDir(path.dirname(worktreeDir));
        await addWorktree(args.sourceRepoDir, worktreeDir, branch, baseCommit, env);
        await writeWorktreeIdentity(worktreeDir, { ...expectedIdentity, createdAtIso: new Date().toISOString() });
      } else {
        // Bound retries to THIS task/attempt/base/owner state -- identity
        // must match before anything else about a reused worktree is
        // trusted, exactly like the deterministic path.
        const recorded = await readWorktreeIdentity(worktreeDir);
        assertIdentityMatches(recorded, expectedIdentity);
        reuse = await classifyAiWorkspace(worktreeDir, baseCommit, env);
        if (reuse.kind === "unexpected") {
          throw new Error(
            `AI repair mode will not reconcile an unexpected workspace state for task ${args.taskId} attempt ${attemptId}: ${reuse.reason} -- refusing to discard or guess; nothing was modified`,
          );
        }
      }

      let diagnostics: string | undefined;
      for (let attemptNumber = 1; attemptNumber <= MAX_AI_ATTEMPTS; attemptNumber++) {
        if (Date.now() >= deadlineAt) {
          span.setAttribute("relaylab.outcome", "deadline_exceeded");
          return { outcome: { kind: "deadline_exceeded" }, attempts };
        }

        // Renew the claim lease before starting this attempt's work --
        // on ClaimLost/EpochStale, stop immediately WITHOUT publishing
        // anything. This is TaskStore's own fencing surfaced, never
        // weakened: a real takeover is respected exactly as it would be
        // if this function did nothing at all.
        const renewal = await renewLeaseOrStop(args.pool, args.taskId, args.ownerId, args.epoch, LEASE_RENEWAL_MS);
        if (!renewal.ok) {
          span.setAttribute("relaylab.outcome", "ownership_lost");
          return { outcome: { kind: "ownership_lost", reason: renewal.reason }, attempts };
        }

        let resultCommit: string;
        let record: (AiRepairAttemptRecord & { verificationOutcome?: string }) | undefined;

        if (attemptNumber === 1 && reuse.kind === "committed") {
          // Resuming a PRIOR run's already-committed AI edit -- never
          // re-generate, never re-commit; just re-verify it (mirrors the
          // deterministic path's "repair_committed" skip).
          resultCommit = reuse.head;
        } else if (attemptNumber === 1 && reuse.kind === "edit_pending_commit") {
          // Resuming a PRIOR run's validated-but-uncommitted AI edit --
          // commit exactly it, never re-generate.
          resultCommit = await commitAllowedPaths(
            worktreeDir,
            [TARGET_RELATIVE_PATH],
            `relaylab worker: resuming interrupted AI-generated repair ${AI_REPAIR_NAME} for task ${args.taskId} (attempt ${attemptNumber})`,
            env,
          );
        } else {
          if (attemptNumber > 1) {
            // Discard THIS run's own previous attempt's commit, already
            // confirmed failed by a real verification result below --
            // never applied to an "unexpected" state found above.
            await resetHard(worktreeDir, baseCommit, env);
          }

          const boundary1 = checkCancelledOrDeadline(args.signal, deadlineAt);
          if (boundary1) {
            span.setAttribute("relaylab.outcome", boundary1.kind === "cancelled" ? "ai_repair_cancelled" : "deadline_exceeded");
            return { outcome: boundary1.kind === "cancelled" ? { kind: "verification_cancelled", verification: cancelledVerificationPlaceholder() } : { kind: "deadline_exceeded" }, attempts };
          }

          await report("attempt_started", attemptNumber);
          const genSignal = stepSignal(args.signal, deadlineAt);
          record = await attemptAiRepair(worktreeDir, client, config, attemptNumber, diagnostics, genSignal);
          attempts.push(record);
          await report("attempt_generated", attemptNumber);

          if (record.outcome === "cancelled") {
            span.setAttribute("relaylab.outcome", "ai_repair_cancelled");
            return { outcome: { kind: "verification_cancelled", verification: cancelledVerificationPlaceholder() }, attempts };
          }
          if (record.outcome === "provider_error" || record.outcome === "rejected" || record.outcome === "no_change") {
            // No usable edit was produced at all -- nothing to commit or
            // verify. The provider-failure case is kept DISTINGUISHABLE
            // from a failed verification in the record itself
            // (outcome: "provider_error" vs the "edit_applied" records
            // below that later gain a verificationOutcome).
            if (attemptNumber < MAX_AI_ATTEMPTS) continue;
            span.setAttribute("relaylab.outcome", "ai_repair_attempts_exhausted");
            return { outcome: { kind: "verification_failed", verification: rejectedVerificationPlaceholder(record.reason ?? record.outcome) }, attempts };
          }

          resultCommit = await commitAllowedPaths(
            worktreeDir,
            [TARGET_RELATIVE_PATH],
            `relaylab worker: AI-generated repair ${AI_REPAIR_NAME} for task ${args.taskId} (attempt ${attemptNumber})`,
            env,
          );
        }

        const boundary2 = checkCancelledOrDeadline(args.signal, deadlineAt);
        if (boundary2) {
          span.setAttribute("relaylab.outcome", boundary2.kind === "cancelled" ? "ai_repair_cancelled" : "deadline_exceeded");
          return { outcome: boundary2.kind === "cancelled" ? { kind: "verification_cancelled", verification: cancelledVerificationPlaceholder() } : { kind: "deadline_exceeded" }, attempts };
        }

        const pinnedTree = await currentTree(worktreeDir, env);
        await report("verification_started", attemptNumber);
        const verifySignal = stepSignal(args.signal, deadlineAt);
        const { result: verification, imageId } = await (async () => {
          const materialized = await materializeCandidateInput(worktreeDir, resultCommit, env, path.join(args.managedWorkspaceRoot, ".verify-scratch-ai"));
          try {
            assertMaterializedTreeMatches(materialized, pinnedTree, `task ${args.taskId} (ai attempt ${attemptNumber})`);
            return await verifyInDockerWithImageId({ taskId: args.taskId, attemptId, inputDir: materialized.exportDir, signal: verifySignal });
          } finally {
            await materialized.cleanup();
          }
        })();

        if (record) {
          record.verificationOutcome = verification.outcome;
        }
        await report("attempt_verified", attemptNumber);

        if (verification.outcome === "timed_out") {
          span.setAttribute("relaylab.outcome", "verification_timeout");
          return { outcome: { kind: "verification_timeout", verification }, attempts };
        }
        if (verification.outcome === "cancelled") {
          span.setAttribute("relaylab.outcome", "verification_cancelled");
          return { outcome: { kind: "verification_cancelled", verification }, attempts };
        }
        if (verification.outcome === "infrastructure_failure") {
          span.setAttribute("relaylab.outcome", "verification_infrastructure_failure");
          return { outcome: { kind: "verification_infrastructure_failure", verification }, attempts };
        }
        if (verification.outcome !== "passed") {
          if (attemptNumber < MAX_AI_ATTEMPTS) {
            diagnostics = truncateForDiagnostics(`exit code: ${verification.exitCode}\nstdout: ${verification.stdout}\nstderr: ${verification.stderr}`);
            continue;
          }
          span.setAttribute("relaylab.outcome", "verification_failed");
          return { outcome: { kind: "verification_failed", verification }, attempts };
        }

        // Passed -- re-confirm nothing moved between verification and
        // now, identically to the deterministic path.
        const postVerifyDirty = await hasUncommittedChanges(worktreeDir, env);
        const postVerifyTree = await currentTree(worktreeDir, env);
        const postVerifyCommit = await currentCommit(worktreeDir, env);
        if (postVerifyDirty || postVerifyTree !== pinnedTree || postVerifyCommit !== resultCommit) {
          throw new Error(
            `verification-to-result drift detected for AI repair task ${args.taskId}: worktree changed between the pinned pre-verification state and now`,
          );
        }

        // Check cancellation AND ownership one final time before ever
        // publishing -- a signal fired, or a takeover that happened,
        // during Docker verification must still prevent a checkpoint.
        const boundary3 = checkCancelledOrDeadline(args.signal, deadlineAt);
        if (boundary3) {
          span.setAttribute("relaylab.outcome", boundary3.kind === "cancelled" ? "ai_repair_cancelled" : "deadline_exceeded");
          return { outcome: boundary3.kind === "cancelled" ? { kind: "verification_cancelled", verification: cancelledVerificationPlaceholder() } : { kind: "deadline_exceeded" }, attempts };
        }
        const finalRenewal = await renewLeaseOrStop(args.pool, args.taskId, args.ownerId, args.epoch, LEASE_RENEWAL_MS);
        if (!finalRenewal.ok) {
          span.setAttribute("relaylab.outcome", "ownership_lost");
          return { outcome: { kind: "ownership_lost", reason: finalRenewal.reason }, attempts };
        }

        const runtimeFingerprint = await buildRuntimeFingerprint(env);
        const manifest = buildManifest({
          taskId: args.taskId,
          attemptId,
          epoch: args.epoch,
          baseCommit,
          resultCommit,
          resultTree: pinnedTree,
          verification: { ...verification, outcome: "passed" },
          runtimeFingerprint,
          containerImageId: imageId,
        });

        const receipt = await Effect.runPromise(
          Effect.provide(
            publishCheckpoint({
              taskId: args.taskId,
              ownerId: args.ownerId,
              epoch: args.epoch,
              commandId: checkpointCommandId(args.taskId),
              attemptId,
              manifest,
            }),
            TaskStoreLive(args.pool),
          ),
        );

        span.setAttribute("relaylab.outcome", "checkpointed");
        return {
          outcome: { kind: "checkpointed", manifest, replay: (receipt as { replay: boolean }).replay, verification, imageId },
          attempts,
        };
      }

      // Unreachable given MAX_AI_ATTEMPTS >= 1 and the loop's own
      // returns above, but keeps the function's control flow explicit
      // rather than relying on TypeScript's narrowing alone.
      throw new Error("runAiRepairTask: attempt loop exited without returning a result");
    } finally {
      span.end();
    }
  });
}

/**
 * Renews the SAME claim lease every other worker in this project uses
 * (TaskStore.renewLease), via the SAME epoch-fencing mechanism -- this
 * function never bypasses or reinterprets ClaimLost/EpochStale, only
 * reports them. Any other error (a transient DB failure) propagates as
 * a real throw, since it is not a "this task is no longer ours" signal.
 */
async function renewLeaseOrStop(
  pool: Parameters<typeof TaskStoreLive>[0],
  taskId: string,
  ownerId: string,
  epoch: number,
  leaseMs: number,
): Promise<{ ok: true } | { ok: false; reason: string }> {
  const program = Effect.gen(function* () {
    const store = yield* TaskStore;
    return yield* store.renewLease(taskId, ownerId, epoch, leaseMs);
  });
  const exit = await Effect.runPromiseExit(Effect.provide(program, TaskStoreLive(pool)));
  if (Exit.isSuccess(exit)) return { ok: true };
  const failure = Cause.failureOption(exit.cause);
  if (failure._tag === "Some" && (failure.value instanceof ClaimLost || failure.value instanceof EpochStale)) {
    return { ok: false, reason: failure.value.message };
  }
  // Anything else (a transient store error, a defect) is a genuine
  // infrastructure problem, not an ownership signal -- let it propagate.
  throw Cause.squash(exit.cause);
}

/**
 * A placeholder `VerificationResult` for the case where an AI attempt
 * never reached real verification at all (cancelled before any provider
 * call completed, or every attempt was rejected/errored before a commit
 * ever existed to verify). `executionId`/`adapterName` name this
 * module, not a real Docker execution, so this is never mistaken for
 * evidence of an actual verification run.
 */
function cancelledVerificationPlaceholder(): import("./verification.js").VerificationResult {
  return { outcome: "cancelled", executionId: AI_REPAIR_NAME, adapterName: "ai-repair", exitCode: null, durationMs: 0, stdout: "", stderr: "", stdoutTruncated: false, stderrTruncated: false, cleanupError: null };
}
function rejectedVerificationPlaceholder(reason: string): import("./verification.js").VerificationResult {
  return {
    outcome: "verification_failed",
    executionId: AI_REPAIR_NAME,
    adapterName: "ai-repair",
    exitCode: null,
    durationMs: 0,
    stdout: "",
    stderr: `AI repair attempts exhausted without producing a usable edit: ${reason}`,
    stdoutTruncated: false,
    stderrTruncated: false,
    cleanupError: null,
  };
}
