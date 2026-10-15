// Phase 2A.2: classifies a worktree's ACTUAL current state (HEAD plus
// every tracked/staged/untracked changed path) BEFORE anything is
// mutated. Closes the gap Phase 2A.1 left open: that patch proved a
// CLEAN result worktree's HEAD actually contains the repair (ancestry +
// commit-count-delta checks), but `ensureWorktreeAndCommit` still called
// `applyScriptedRepair` unconditionally -- before ever inspecting
// whether a REUSED worktree's identity match said anything about its
// actual dirty/staged/untracked state. A worktree whose identity sidecar
// matches (same task/attempt/base/branch) but whose real Git state has
// since been touched by something OTHER than this worker's own repair
// (an unrelated tracked-file edit, a stray staged change, an untracked
// file, a modified verify.js, or HEAD moved to an unrelated commit) was
// previously either silently absorbed into the next commit (via
// `git add -A`) or overwritten/hidden by the unconditional repair step.
// This module positively confirms the workspace is in one of the
// supported states below, reads but never writes/stages/deletes
// anything, and REJECTS everything else with the unexpected paths named
// explicitly, leaving them untouched for inspection.
import { currentCommit, isAncestor, commitCountBetween, changedPaths } from "./git.js";
import { isRepairPresent, REPAIR_NAME, TARGET_RELATIVE_PATH } from "./repair.js";

export type WorkspaceClassification =
  | { kind: "ready_for_repair" }
  | { kind: "repair_pending_commit" }
  | { kind: "repair_committed"; head: string }
  | { kind: "unexpected"; reason: string };

/**
 * The shared "is this clean HEAD a genuinely completed, verifiable
 * repair result" check -- used by BOTH `classifyWorkspace` (for a clean
 * worktree whose HEAD has moved past `baseCommit`) and
 * `assertCleanWorktreeIsValidResult` (src/worker/run-task.ts, kept as its
 * own independently-testable, throwing entry point -- see
 * tests/integration/git-worker-crash.ts `scenario_cleanAtBuggyBaseRejected`).
 * Factored out here so the two can never silently drift apart. Never
 * trusts a clean `git status` alone: positively confirms the repair is
 * present in HEAD's actual tree, that HEAD is a real descendant of
 * `baseCommit`, and that the exact expected commit-count delta (one, for
 * this fixture) holds.
 */
export async function checkCommittedRepairState(
  worktreeDir: string,
  baseCommit: string,
  head: string,
  env: NodeJS.ProcessEnv,
): Promise<{ ok: true } | { ok: false; reason: string }> {
  const [repairPresent, headDescendsFromBase] = await Promise.all([isRepairPresent(worktreeDir), isAncestor(worktreeDir, baseCommit, head, env)]);

  if (!headDescendsFromBase) {
    return { ok: false, reason: `HEAD ("${head}") is not a descendant of the requested base commit ("${baseCommit}")` };
  }
  if (!repairPresent) {
    // Covers BOTH "clean at the unmodified buggy base" (handled before
    // this function is reached in `classifyWorkspace`'s own flow, but
    // still a real possibility for a direct caller like
    // `assertCleanWorktreeIsValidResult`) and "HEAD moved to some other
    // commit that still lacks the repair."
    return {
      ok: false,
      reason: `worktree is clean (HEAD="${head}", base="${baseCommit}") but the expected scripted repair (${REPAIR_NAME}) is not present in HEAD's current files -- refusing to treat an unrepaired checkout as completed work`,
    };
  }

  const commitsSinceBase = await commitCountBetween(worktreeDir, baseCommit, head, env);
  if (commitsSinceBase !== 1) {
    return {
      ok: false,
      reason: `expected exactly 1 commit between base ("${baseCommit}") and HEAD ("${head}") for this deterministic single-commit repair fixture, found ${commitsSinceBase} -- refusing to treat this as the expected completed result`,
    };
  }
  return { ok: true };
}

function describeEntries(entries: readonly { status: string; path: string }[]): string {
  return entries.map((e) => `${e.status} ${e.path}`).join(", ");
}

/**
 * Phase 2A.2: the five supported workspace states (named per the task
 * that requested this patch):
 *
 *   A. "ready_for_repair"       -- HEAD === baseCommit, clean, the known
 *      defect is still present. The normal starting point: a fresh
 *      checkout, or a reused worktree nothing has touched yet.
 *   B. "repair_pending_commit"  -- HEAD === baseCommit, dirty, and the
 *      ENTIRE changed-path set is exactly the one supported repair
 *      target (`src/add.js`) with its content already matching the
 *      known fix. This is the "repair applied, worker interrupted
 *      before commit" crash window -- the ONLY dirty state ever
 *      permitted through.
 *   C. "repair_committed"       -- clean, HEAD is a real descendant of
 *      baseCommit, the repair is present in HEAD's tree, and the
 *      commit-count delta from base is exactly 1 (see
 *      `checkCommittedRepairState`). Whether a checkpoint for this
 *      commit has already been durably persisted (state D in the task's
 *      own naming -- "checkpoint already persisted") is NOT something
 *      this Git-level classification can or needs to determine: states C
 *      and D look IDENTICAL from the worktree's point of view, and the
 *      distinction is handled entirely downstream by
 *      `TaskStore.recordGitCheckpoint`'s own idempotency ledger (a
 *      retried publish for the same commit/digest returns `replay: true`
 *      rather than a duplicate row -- see src/worker/checkpoint.ts).
 *   E. "unexpected"              -- anything else: a dirty worktree with
 *      ANY path besides the one repair target touched (an unrelated
 *      tracked-file edit, a staged edit, an untracked file, a modified
 *      verify.js), a dirty worktree whose HEAD is not even the base
 *      commit, or a clean worktree whose HEAD does not satisfy
 *      `checkCommittedRepairState`. NEVER mutated, staged, committed, or
 *      deleted -- the caller (`ensureWorktreeAndCommit`) throws using
 *      this classification's `reason` and stops.
 *
 * Read-only: issues `git status`/`git rev-parse`/`git merge-base`/
 * `git rev-list` calls only. Never stages, writes, commits, or deletes
 * anything.
 */
export async function classifyWorkspace(worktreeDir: string, baseCommit: string, env: NodeJS.ProcessEnv): Promise<WorkspaceClassification> {
  const changed = await changedPaths(worktreeDir, env);
  const head = await currentCommit(worktreeDir, env);

  if (changed.length === 0) {
    if (head === baseCommit) {
      const alreadyFixed = await isRepairPresent(worktreeDir);
      if (alreadyFixed) {
        // Should never happen for this fixture (the base commit is
        // known, fixed content, to contain the defect) -- an explicit,
        // named rejection rather than silently proceeding on an
        // unexplained mismatch between the recorded base commit and its
        // actual file content.
        return {
          kind: "unexpected",
          reason: `HEAD equals the base commit ("${baseCommit}") but the scripted repair's fixed content is already present there -- this fixture's base commit is expected to contain the known defect, not the fix; refusing to proceed on an unexplained base-commit content mismatch`,
        };
      }
      return { kind: "ready_for_repair" };
    }
    const verdict = await checkCommittedRepairState(worktreeDir, baseCommit, head, env);
    if (!verdict.ok) return { kind: "unexpected", reason: verdict.reason };
    return { kind: "repair_committed", head };
  }

  // Dirty: the ONLY supported dirty state is "repair written, not yet
  // committed," which requires HEAD to still be exactly the base commit
  // (a dirty worktree whose HEAD has ALSO moved past base is an
  // ambiguous mix this patch explicitly refuses to guess about) AND the
  // entire changed-path set to be exactly the one repair target.
  if (head !== baseCommit) {
    return {
      kind: "unexpected",
      reason: `worktree is dirty AND HEAD ("${head}") is not the base commit ("${baseCommit}") -- refusing to reconcile an ambiguous mixed state (changed paths: ${describeEntries(changed)})`,
    };
  }
  const unexpected = changed.filter((entry) => entry.path !== TARGET_RELATIVE_PATH);
  if (unexpected.length > 0) {
    return {
      kind: "unexpected",
      reason: `worktree has changed path(s) outside the one supported repair target ("${TARGET_RELATIVE_PATH}"): ${describeEntries(unexpected)} -- refusing to stage or commit anything until this is resolved`,
    };
  }
  const fixedContentPresent = await isRepairPresent(worktreeDir);
  if (!fixedContentPresent) {
    return {
      kind: "unexpected",
      reason: `the only changed path is the expected repair target ("${TARGET_RELATIVE_PATH}"), but its content is neither the known defect nor the known fix -- refusing to guess what this uncommitted edit is`,
    };
  }
  return { kind: "repair_pending_commit" };
}
