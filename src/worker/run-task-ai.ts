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
// untruncated log) are fed into the next attempt's prompt. Exhausting
// all attempts, or any attempt failing for a reason other than a normal
// failing verification (timeout/cancelled/infrastructure failure),
// returns the same `RunOutcome` shapes run-task.ts already defines --
// a checkpoint is NEVER published for anything but a genuinely passed
// verification.
import { realpath, stat } from "node:fs/promises";
import path from "node:path";
import { Effect } from "effect";
import { TaskStoreLive } from "../db/task-store.js";
import { addWorktree, commitAllowedPaths, currentCommit, currentTree, hasUncommittedChanges, makeIsolatedGitEnv, resetHard, resolveCommit } from "./git.js";
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

export interface AiRunOutcome {
  outcome: RunOutcome;
  /** Every attempt's identity/model/outcome/usage -- never the prompt or response text (see ai-repair.ts). This is what satisfies "record attempt identity, model, outcome, and reported token usage" for the final report; it is never logged wholesale, only returned to the caller. */
  attempts: AiRepairAttemptRecord[];
}

export interface RunAiRepairTaskArgs extends RunTaskArgs {
  /** Injectable for tests -- defaults to the real `createAnthropicClient()` (reads credentials from the environment only). */
  aiClient?: MessagesClient;
  aiConfig?: AnthropicAdapterConfig;
  /** Lets a caller cancel an in-flight provider call between attempts. */
  signal?: AbortSignal;
}

function truncateForDiagnostics(text: string): string {
  return text.length > MAX_DIAGNOSTICS_CHARS ? text.slice(0, MAX_DIAGNOSTICS_CHARS) + "\n...[truncated]" : text;
}

export async function runAiRepairTask(args: RunAiRepairTaskArgs): Promise<AiRunOutcome> {
  const tracer = getTracer();
  return tracer.startActiveSpan("relaylab.worker.run_ai_repair_task", async (span): Promise<AiRunOutcome> => {
    const attempts: AiRepairAttemptRecord[] = [];
    try {
      span.setAttribute("relaylab.task_id", args.taskId);
      span.setAttribute("relaylab.repair_mode", "ai");

      const client = args.aiClient ?? createAnthropicClient();
      const config = args.aiConfig ?? loadAnthropicAdapterConfig();

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
      if (!exists) {
        await ensureDir(path.dirname(worktreeDir));
        await addWorktree(args.sourceRepoDir, worktreeDir, branch, baseCommit, env);
        await writeWorktreeIdentity(worktreeDir, { ...expectedIdentity, createdAtIso: new Date().toISOString() });
      } else {
        // AI repair mode does not implement the deterministic path's
        // full crash-reconciliation state machine (workspace-classify.ts)
        // -- it supports exactly one supported reused state: identity
        // matches AND the worktree is clean at baseCommit. Anything else
        // (a genuinely interrupted AI attempt) is refused explicitly
        // rather than guessed at.
        const recorded = await readWorktreeIdentity(worktreeDir);
        assertIdentityMatches(recorded, expectedIdentity);
        const dirty = await hasUncommittedChanges(worktreeDir, env);
        const head = await currentCommit(worktreeDir, env);
        if (dirty || head !== baseCommit) {
          throw new Error(
            `AI repair mode does not support reconciling a reused worktree that is not clean at the base commit (dirty=${dirty}, head=${head}, base=${baseCommit}) -- refusing to guess; use a fresh managed workspace root for a retried run`,
          );
        }
      }

      let diagnostics: string | undefined;
      for (let attemptNumber = 1; attemptNumber <= MAX_AI_ATTEMPTS; attemptNumber++) {
        if (attemptNumber > 1) {
          // Discard the previous attempt's rejected/failed commit and
          // return to a clean base before generating a fresh edit.
          await resetHard(worktreeDir, baseCommit, env);
        }

        const record = await attemptAiRepair(worktreeDir, client, config, attemptNumber, diagnostics, args.signal);
        attempts.push(record);

        if (record.outcome === "cancelled") {
          span.setAttribute("relaylab.outcome", "ai_repair_cancelled");
          return { outcome: { kind: "verification_cancelled", verification: cancelledVerificationPlaceholder() }, attempts };
        }
        if (record.outcome === "provider_error" || record.outcome === "rejected") {
          // This attempt never produced a usable edit at all -- nothing
          // to commit or verify. If attempts remain, loop; if not,
          // report failure honestly (never checkpointed).
          if (attemptNumber < MAX_AI_ATTEMPTS) continue;
          span.setAttribute("relaylab.outcome", "ai_repair_attempts_exhausted");
          return { outcome: { kind: "verification_failed", verification: rejectedVerificationPlaceholder(record.reason ?? record.outcome) }, attempts };
        }

        // record.outcome === "edit_applied": commit via the SAME
        // allowlist-enforcing primitive the deterministic path uses.
        const resultCommit = await commitAllowedPaths(
          worktreeDir,
          [TARGET_RELATIVE_PATH],
          `relaylab worker: AI-generated repair ${AI_REPAIR_NAME} for task ${args.taskId} (attempt ${attemptNumber})`,
          env,
        );
        const pinnedTree = await currentTree(worktreeDir, env);

        const { result: verification, imageId } = await (async () => {
          const materialized = await materializeCandidateInput(worktreeDir, resultCommit, env, path.join(args.managedWorkspaceRoot, ".verify-scratch-ai"));
          try {
            assertMaterializedTreeMatches(materialized, pinnedTree, `task ${args.taskId} (ai attempt ${attemptNumber})`);
            return await verifyInDockerWithImageId({ taskId: args.taskId, attemptId, inputDir: materialized.exportDir });
          } finally {
            await materialized.cleanup();
          }
        })();

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
