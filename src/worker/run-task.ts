// Phase 2A: the worker's core orchestration -- claims are the caller's
// job (see src/worker-main.ts); this module resolves the starting
// commit, creates/reuses a task-specific worktree, applies the one
// scripted repair, runs the fixed verification command, commits on
// success, and publishes a durable checkpoint. Every step is written to
// be safely RE-ENTRANT after a crash at any point (see "reconciliation"
// below) -- this is deliberately a single-attempt model (ONE
// deterministic `attemptId` per task; see `defaultAttemptId`), NOT a
// multi-worker handoff/election system -- that is explicitly out of
// scope for this milestone (see docs/ROADMAP.md Phase 2).
import { realpath, stat } from "node:fs/promises";
import path from "node:path";
import { Effect } from "effect";
import type mysql from "mysql2/promise";
import { TaskStore, TaskStoreLive } from "../db/task-store.js";
import {
  git,
  addWorktree,
  commitAllowedPaths,
  currentCommit,
  currentTree,
  hasUncommittedChanges,
  makeIsolatedGitEnv,
  removeWorktree,
  resolveCommit,
} from "./git.js";
import {
  assertNoSymlinkEscapes,
  assertSafeForDeletion,
  assertWithinRoot,
  ensureDir,
  resolveIsolatedHomeDir,
  resolveWorktreePath,
} from "./workspace.js";
import { applyScriptedRepair, REPAIR_NAME, TARGET_RELATIVE_PATH } from "./repair.js";
import { classifyWorkspace, checkCommittedRepairState } from "./workspace-classify.js";
import { buildManifest, buildRuntimeFingerprint, publishCheckpoint, type CheckpointManifest } from "./checkpoint.js";
import { assertIdentityMatches, readWorktreeIdentity, writeWorktreeIdentity } from "./worktree-identity.js";
import { getTracer } from "../telemetry/init.js";
import { hitFailpoint } from "../testing/failpoint.js";
import { assertMaterializedTreeMatches, materializeCandidateInput, verifyInDockerWithImageId } from "./docker-verify.js";
import type { VerificationResult } from "./verification.js";

/**
 * Single-attempt model for this milestone: every (re)start of work on a
 * given task uses the SAME, deterministic `attemptId` -- NOT a fresh
 * random one per process restart. This is what makes restart
 * reconciliation tractable without a "search across every possible
 * attempt" mechanism: there is only ever ONE worktree path for a given
 * task, so "does a prior attempt's work already exist" is a single,
 * deterministic filesystem check (see `runGitWorkerTask` below), not a
 * distributed search. A real multi-attempt/handoff model (a different
 * worker instance picking up a DIFFERENT attempt after the first is
 * presumed dead) is explicitly Phase 2's later, NOT-yet-implemented
 * scope (docs/ROADMAP.md).
 */
export function defaultAttemptId(_taskId: string): string {
  return "attempt-1";
}

export function branchNameFor(taskId: string): string {
  return `relaylab/${taskId}`;
}

/** The deterministic idempotency key this task's ONE checkpoint publish uses -- task-scoped (not attempt-scoped), so a retry from a DIFFERENT attemptId after a crash still collides correctly with an already-published checkpoint (see docs/ARCHITECTURE.md "Phase 2A worker" for why task-scoped, not attempt-scoped, is the right choice here). */
export function checkpointCommandId(taskId: string): string {
  return `git-checkpoint-${taskId}`;
}

export type RunOutcome =
  | { kind: "checkpointed"; manifest: CheckpointManifest; replay: boolean; verification: VerificationResult; imageId: string | null }
  | { kind: "verification_failed"; verification: VerificationResult }
  | { kind: "verification_timeout"; verification: VerificationResult }
  | { kind: "verification_cancelled"; verification: VerificationResult }
  /**
   * Phase 2C: the Docker daemon/image/container itself could not be
   * launched -- NEVER treated as proof the candidate is bad, and NEVER
   * checkpointed, exactly like the other three non-"passed" outcomes
   * above. Kept as its OWN `kind` (not folded into
   * "verification_failed") so a caller/log reader is never misled into
   * thinking the candidate's code was actually exercised.
   */
  | { kind: "verification_infrastructure_failure"; verification: VerificationResult };

export interface RunTaskArgs {
  taskId: string;
  ownerId: string;
  epoch: number;
  sourceRepoDir: string;
  requestedCommitRef: string;
  managedWorkspaceRoot: string;
  pool: mysql.Pool;
  /** Test-only escape hatch: skip the scripted repair so verification genuinely fails against the unrepaired fixture -- see tests/integration/git-worker.test.ts "verification failure prevents successful completion." Never used by src/worker-main.ts's real path. */
  skipRepairForTesting?: boolean;
  /** Demo UI milestone: propagated into Docker verification (VerificationRequest.signal, already supported there) so a caller (e.g. the local demo API's cancel button) can cancel an in-flight deterministic verification, not only the AI path's. Optional -- every existing caller that omits it is unaffected. */
  signal?: AbortSignal;
  /** Test-only deterministic barrier: invoked at the exact moment the verification container is confirmed running (the SAME real signal process-ownership-style failpoints already use), so a cancellation test can abort precisely then -- never a sleep-based guess. Never used by any real caller. */
  onVerifyContainerCreated?: (containerId: string) => void | Promise<void>;
}

/**
 * Reconciliation (Part G), stated per real crash point:
 *   - "Worktree created, acknowledgement lost": the worktree directory
 *     exists on disk; this function detects it via `stat` and reuses it
 *     rather than calling `git worktree add` again (which would fail --
 *     and reusing is correct, since the original worktree's base commit
 *     is exactly what a fresh `addWorktree` call would have produced).
 *   - "Repair applied, worker interrupted": the worktree exists with
 *     uncommitted changes (`hasUncommittedChanges` true). The repair is
 *     RE-APPLIED (idempotent by construction -- see repair.ts) rather
 *     than assumed-correct, then committed.
 *   - "Local commit created, checkpoint not yet persisted": the
 *     worktree's HEAD is already past `baseCommit` with a clean working
 *     tree. That existing commit is REUSED as `resultCommit` -- no new
 *     commit is created. Verification is still RE-RUN (cheap,
 *     deterministic, and the actual proof this checkpoint needs) before
 *     publishing.
 *   - "Checkpoint persisted, final acknowledgement lost": handled
 *     entirely by `TaskStore.recordGitCheckpoint`'s own idempotency
 *     ledger (the SAME `command_receipts` mechanism every other durable
 *     mutation in this project uses) -- a retried publish with the same
 *     `checkpointCommandId` returns `replay: true`, never a duplicate row.
 */
export async function runGitWorkerTask(args: RunTaskArgs): Promise<RunOutcome> {
  const tracer = getTracer();
  return tracer.startActiveSpan("relaylab.worker.run_task", async (span): Promise<RunOutcome> => {
    try {
      span.setAttribute("relaylab.task_id", args.taskId);
      const attemptId = defaultAttemptId(args.taskId);
      const isolatedHome = resolveIsolatedHomeDir(args.managedWorkspaceRoot);
      await ensureDir(isolatedHome);
      const env = makeIsolatedGitEnv(isolatedHome);

      const baseCommit = await tracer.startActiveSpan("relaylab.worker.resolve_commit", async (s) => {
        try {
          return await resolveCommit(args.sourceRepoDir, args.requestedCommitRef, env);
        } finally {
          s.end();
        }
      });

      const worktreeDir = resolveWorktreePath(args.managedWorkspaceRoot, args.taskId, attemptId);
      // Defense in depth: resolveWorktreePath already validates this,
      // but re-asserted here at the point of actual use per Part D
      // ("verify resolved paths remain within the managed workspace").
      assertWithinRoot(args.managedWorkspaceRoot, worktreeDir);
      // Phase 2A.1, section 6: the above is lexical only -- also reject
      // any symlink/junction/reparse point anywhere between the managed
      // root and this worktree path before touching it.
      await assertNoSymlinkEscapes(args.managedWorkspaceRoot, worktreeDir);

      const { resultCommit, repairChanged } = await tracer.startActiveSpan("relaylab.worker.create_workspace", async (s) => {
        try {
          return await ensureWorktreeAndCommit(args, env, worktreeDir, baseCommit, attemptId);
        } finally {
          s.end();
        }
      });
      void repairChanged;

      // Phase 2A.1, section 4: bind verification to the ACTUAL result by
      // pinning the tree BEFORE verification runs, not after. Everything
      // downstream (the manifest, the checkpoint) is checked against
      // THIS captured value, never a freshly-re-read one that could have
      // silently drifted.
      const pinnedTree = await currentTree(worktreeDir, env);

      await hitFailpoint("worker-after-commit-before-verify", { resultCommit, resultTree: pinnedTree });

      // Phase 2C: verify in an isolated Docker container, never the
      // worktree itself. Materializes EXACTLY the pinned result tree
      // (by commit SHA, via `git archive`) into a fresh, disposable
      // export -- the worktree on disk is never touched by this step at
      // all, which is STRONGER than merely "read-only mounted."
      const { result: verification, imageId } = await tracer.startActiveSpan("relaylab.worker.verify", async (s) => {
        try {
          const materialized = await materializeCandidateInput(worktreeDir, resultCommit, env, path.join(args.managedWorkspaceRoot, ".verify-scratch"));
          try {
            // Phase 2C.1, Part 1: an identity-mismatch regression --
            // `materializeCandidateInput`'s OWN report of what it
            // actually archived (`treeSha`, from a fresh `git
            // rev-parse ref^{tree}`) must equal the tree this worker
            // already pinned BEFORE calling it. This is independent of
            // (and strictly stronger than) `resultCommit` merely being
            // the SAME STRING passed in as `ref` -- it catches a tree
            // that was archived under a different identity than
            // intended (e.g. an ambiguous/stale ref resolution), rather
            // than trusting "the ref string matched" as proof of "the
            // right tree was verified." A mismatch here must NEVER
            // reach the checkpoint step below.
            assertMaterializedTreeMatches(materialized, pinnedTree, `task ${args.taskId}`);
            return await verifyInDockerWithImageId(
              { taskId: args.taskId, attemptId, inputDir: materialized.exportDir, ...(args.signal ? { signal: args.signal } : {}) },
              {
                onContainerCreated: async (containerId) => {
                  await hitFailpoint("worker-verify-container-running", { pid: process.pid, containerId, taskId: args.taskId, attemptId, epoch: args.epoch });
                  // Test-only deterministic barrier: fires at the REAL
                  // moment Docker confirms the verification container is
                  // running, never a sleep -- see
                  // tests/integration/cancellation-barriers.test.ts.
                  await args.onVerifyContainerCreated?.(containerId);
                },
              },
            );
          } finally {
            await materialized.cleanup();
          }
        } finally {
          s.end();
        }
      });

      if (verification.outcome === "timed_out") {
        span.setAttribute("relaylab.outcome", "verification_timeout");
        return { kind: "verification_timeout", verification };
      }
      if (verification.outcome === "cancelled") {
        span.setAttribute("relaylab.outcome", "verification_cancelled");
        return { kind: "verification_cancelled", verification };
      }
      if (verification.outcome === "infrastructure_failure") {
        // NEVER proof the candidate is bad -- the Docker daemon/image
        // itself could not be launched. Never checkpointed, same as
        // every other non-"passed" outcome here.
        span.setAttribute("relaylab.outcome", "verification_infrastructure_failure");
        return { kind: "verification_infrastructure_failure", verification };
      }
      if (verification.outcome !== "passed") {
        // Explicit, honest non-success: a failing verification NEVER
        // publishes a checkpoint. The real local commit still exists in
        // the worktree (it is not undone), but no durable, authoritative
        // "this is good" record is created for it.
        span.setAttribute("relaylab.outcome", "verification_failed");
        return { kind: "verification_failed", verification };
      }

      // Phase 2A.1, section 4 (now stronger since Phase 2C -- verification
      // runs against a DISPOSABLE EXPORT, src/worker/docker-verify.ts's
      // `materializeCandidateInput`, never the worktree itself, so this
      // check is not expected to ever trip): confirm the code state
      // verification just exercised is STILL the code state about to be
      // checkpointed. If anything altered the worktree during
      // verification (a concurrent process, etc.), this is caught and
      // refused explicitly rather than silently publishing a manifest
      // describing a tree that was never actually verified.
      const postVerifyDirty = await hasUncommittedChanges(worktreeDir, env);
      const postVerifyTree = await currentTree(worktreeDir, env);
      const postVerifyCommit = await currentCommit(worktreeDir, env);
      if (postVerifyDirty || postVerifyTree !== pinnedTree || postVerifyCommit !== resultCommit) {
        throw new Error(
          `verification-to-result drift detected for task ${args.taskId}: worktree changed between the pinned pre-verification state (commit ${resultCommit}, tree ${pinnedTree}) and now (commit ${postVerifyCommit}, tree ${postVerifyTree}, dirty=${postVerifyDirty}) -- refusing to checkpoint a result that does not match what was actually verified`,
        );
      }

      await hitFailpoint("worker-after-verify-before-checkpoint", { resultCommit, resultTree: pinnedTree });

      const resultTree = pinnedTree;
      const runtimeFingerprint = await buildRuntimeFingerprint(env);
      const manifest = buildManifest({
        taskId: args.taskId,
        attemptId,
        epoch: args.epoch,
        baseCommit,
        resultCommit,
        resultTree,
        // Narrow, explicit mapping into buildManifest's own (unchanged,
        // Phase 2A) verification shape -- `verification.outcome` is
        // already statically known "passed" here (every other outcome
        // returned above), so this is a plain re-shape, never a lossy
        // cast.
        verification: {
          outcome: "passed",
          exitCode: verification.exitCode,
          stdout: verification.stdout,
          stderr: verification.stderr,
          stdoutTruncated: verification.stdoutTruncated,
          stderrTruncated: verification.stderrTruncated,
          durationMs: verification.durationMs,
        },
        runtimeFingerprint,
        containerImageId: imageId,
      });

      const receipt = await tracer.startActiveSpan("relaylab.worker.publish_checkpoint", async (s) => {
        try {
          return await Effect.runPromise(
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
        } finally {
          s.end();
        }
      });

      span.setAttribute("relaylab.outcome", "checkpointed");
      // `verification` (raw, with its real `executionId`/`adapterName`)
      // is returned ALONGSIDE the durable `manifest` -- the manifest's
      // own narrow-mapped verification block deliberately omits
      // `executionId` (an evidence-scoping choice, not an oversight; see
      // CheckpointManifest's doc comment), but callers that need to tell
      // apart two DIFFERENT executions of the same candidate (e.g. this
      // milestone's own end-to-end demo, matching B's `RestoreOutcome`
      // which already exposes this) still need it.
      return { kind: "checkpointed", manifest, replay: (receipt as { replay: boolean }).replay, verification, imageId };
    } finally {
      span.end();
    }
  });
}

/**
 * Phase 2A.2: explicit, named workspace states, now determined by
 * CLASSIFYING the worktree's actual Git state (HEAD plus every tracked/
 * staged/untracked changed path -- see workspace-classify.ts)
 * BEFORE anything is touched, replacing Phase 2A.1's version, which
 * called `applyScriptedRepair` unconditionally before ever checking
 * whether a REUSED worktree's dirty/staged/untracked state was
 * consistent with the one supported repair. That let an unrelated
 * tracked-file edit, a stray staged change, an untracked file, or a
 * clean-but-unexpected HEAD either be silently absorbed into the next
 * `git add -A` commit or overwritten/hidden by the unconditional repair
 * write -- exactly the "unconditional repair hides evidence of an
 * inconsistent workspace" gap this patch closes. See
 * `classifyWorkspace`'s own doc comment for the full state table (A-E).
 *
 *   1. Worktree does not exist yet -- created fresh at `baseCommit`; a
 *      signed identity sidecar is written so a later reuse can verify
 *      this same worktree was never silently handed to a different
 *      task/attempt/base.
 *   2. Worktree exists -- its recorded identity (see
 *      worktree-identity.ts) must match this call's expected identity
 *      exactly (`assertIdentityMatches`) before anything else about it
 *      is trusted. A matching identity says only "this worktree belongs
 *      to the requested repo/task/attempt/base," NOT "nothing else has
 *      touched it since" -- that is what classification (next) checks.
 *   3. `classifyWorkspace` positively classifies the resulting worktree
 *      into "ready_for_repair" (A), "repair_pending_commit" (B, the
 *      known interrupted-repair state), "repair_committed" (C -- D,
 *      "checkpoint already persisted," is indistinguishable from C at
 *      the Git level and is handled downstream by
 *      `TaskStore.recordGitCheckpoint`'s own idempotency ledger), or
 *      "unexpected" (E) -- an explicit, named rejection, with the
 *      offending path(s) or HEAD state named in the error, and NOTHING
 *      staged, committed, or deleted.
 */
/**
 * Exported (Phase 2A.1/2A.2) so tests/integration/git-worker-crash.ts can
 * directly exercise the explicit workspace states listed above --
 * including the REJECTION states -- without needing to construct a
 * contrived full-process run to reach them. Never imported by production
 * code paths other than `runGitWorkerTask` itself.
 */
export async function ensureWorktreeAndCommit(
  args: RunTaskArgs,
  env: NodeJS.ProcessEnv,
  worktreeDir: string,
  baseCommit: string,
  attemptId: string,
): Promise<{ resultCommit: string; repairChanged: boolean }> {
  const branch = branchNameFor(args.taskId);
  const exists = await stat(worktreeDir).then(
    () => true,
    () => false,
  );
  const sourceRepoRealPath = await realpath(args.sourceRepoDir);
  const expectedIdentity = { taskId: args.taskId, attemptId, sourceRepoRealPath, baseCommit, branch };

  if (!exists) {
    // Fresh checkout.
    await ensureDir(path.dirname(worktreeDir));
    await addWorktree(args.sourceRepoDir, worktreeDir, branch, baseCommit, env);
    await writeWorktreeIdentity(worktreeDir, { ...expectedIdentity, createdAtIso: new Date().toISOString() });
  } else {
    // Reused -- must prove identity before trusting anything else about
    // this worktree.
    const recorded = await readWorktreeIdentity(worktreeDir);
    assertIdentityMatches(recorded, expectedIdentity);
  }

  // Phase 2A.2: classify BEFORE modifying anything -- read-only (see
  // workspace-classify.ts), never stages/writes/commits/deletes.
  const classification = await classifyWorkspace(worktreeDir, baseCommit, env);

  await hitFailpoint("worker-after-worktree-before-repair", { worktreeExisted: exists, workspaceState: classification.kind });

  if (classification.kind === "unexpected") {
    throw new Error(
      `unexpected workspace state for task ${args.taskId} attempt ${attemptId}: ${classification.reason} -- refusing to modify, stage, commit, or delete anything in this worktree`,
    );
  }

  if (args.skipRepairForTesting) {
    // Test-only escape hatch: deliberately leave the worktree exactly as
    // classified (never applies or commits the repair), so verification
    // genuinely fails against the unrepaired fixture -- see
    // tests/integration/git-worker.test.ts test 6. The caller (a test)
    // owns what this state means; this function does nothing further.
    const head = await currentCommit(worktreeDir, env);
    return { resultCommit: head, repairChanged: false };
  }

  if (classification.kind === "repair_committed") {
    return { resultCommit: classification.head, repairChanged: false };
  }

  if (classification.kind === "ready_for_repair") {
    await applyScriptedRepair(worktreeDir);
    await hitFailpoint("worker-after-repair-before-commit");
    const sha = await commitAllowedPaths(
      worktreeDir,
      [TARGET_RELATIVE_PATH],
      `relaylab worker: scripted repair ${REPAIR_NAME} for task ${args.taskId} (attempt ${attemptId})`,
      env,
    );
    return { resultCommit: sha, repairChanged: true };
  }

  // classification.kind === "repair_pending_commit": the known
  // interrupted-repair state -- `classifyWorkspace` already confirmed
  // the fix is written to disk at EXACTLY the one supported path and
  // nowhere else. Commit exactly that; never re-run `applyScriptedRepair`
  // against content already confirmed correct.
  await hitFailpoint("worker-after-repair-before-commit");
  const sha = await commitAllowedPaths(
    worktreeDir,
    [TARGET_RELATIVE_PATH],
    `relaylab worker: resuming interrupted scripted repair ${REPAIR_NAME} for task ${args.taskId} (attempt ${attemptId})`,
    env,
  );
  return { resultCommit: sha, repairChanged: true };
}

/**
 * Phase 2A.1 (kept as its own throwing entry point in Phase 2A.2, now
 * delegating to the SAME `checkCommittedRepairState` helper
 * `classifyWorkspace` uses, rather than duplicating the ancestry/repair-
 * presence/commit-count logic): directly testable against a
 * DELIBERATELY constructed inconsistent worktree (see
 * tests/integration/git-worker-crash.ts
 * `scenario_cleanAtBuggyBaseRejected`) without needing a contrived
 * full-process run. Never trust a clean status alone -- positively
 * confirms the repair is present in HEAD's actual tree, that HEAD is a
 * real descendant of baseCommit, and that the exact expected commit-
 * count delta (one, for this fixture) holds.
 */
export async function assertCleanWorktreeIsValidResult(
  worktreeDir: string,
  taskId: string,
  attemptId: string,
  baseCommit: string,
  env: NodeJS.ProcessEnv,
): Promise<string> {
  const head = await currentCommit(worktreeDir, env);
  const verdict = await checkCommittedRepairState(worktreeDir, baseCommit, head, env);
  if (!verdict.ok) {
    throw new Error(`unexpected workspace state for task ${taskId} attempt ${attemptId}: ${verdict.reason}`);
  }
  return head;
}

/** Exposed so a caller (worker-main.ts, or a test) can pre-flight-reject an out-of-root path WITHOUT creating anything -- see Part H test 7. */
export function assertRequestedPathsAreSafe(managedWorkspaceRoot: string, taskId: string, attemptId: string): void {
  resolveWorktreePath(managedWorkspaceRoot, taskId, attemptId);
}

/** Best-effort worktree cleanup -- never called automatically on success in this milestone (the committed-state checkpoint's whole point is that the underlying Git OBJECTS, not the worktree checkout, are what must remain available -- see docs/OBSERVABILITY.md "checkpoint data-handling"). Exposed for test/demo teardown. */
/**
 * Phase 2A.1, section 6: deletion is treated MORE STRICTLY than mere
 * inspection -- `assertSafeForDeletion` (not just `assertWithinRoot`)
 * must pass before anything is removed, so a cleanup call can never
 * target the managed root itself or a path that isn't EXACTLY one
 * task's one attempt worktree, and can never follow a symlink/junction
 * out of the managed root.
 */
export async function cleanupWorktree(
  managedWorkspaceRoot: string,
  sourceRepoDir: string,
  worktreeDir: string,
  env: NodeJS.ProcessEnv,
): Promise<void> {
  await assertSafeForDeletion(managedWorkspaceRoot, worktreeDir);
  await removeWorktree(sourceRepoDir, worktreeDir, env);
}

export { git as runGit };
