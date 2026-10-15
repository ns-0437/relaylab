# 0010: Explicit-path commits, not `git add -A`, for the scripted repair

## What was wrong

Phase 2A.1 proved that a CLEAN result worktree's HEAD genuinely contained
the repair (ancestry + exact commit-count-delta checks -- see
`assertCleanWorktreeIsValidResult`). It did not ask the next question: is
a REUSED worktree's actual dirty/staged/untracked state, at the moment
the worker is about to act on it, consistent with that one supported
repair at all?

`ensureWorktreeAndCommit` called `applyScriptedRepair` unconditionally,
then committed via `git add -A`, before ever inspecting what else was
dirty. Two concrete consequences:

- An unrelated tracked-file edit, a stray staged change, or an untracked
  file sitting in the worktree would be silently swept into the SAME
  commit as the scripted repair by `git add -A`, with nothing ever having
  decided that was safe.
- A worktree that was clean but sitting at the ORIGINAL buggy commit
  still had the unconditional repair APPLIED to it before any check ran,
  which (for a worktree whose real state should have been rejected as
  inconsistent) destroyed the evidence of that inconsistency before it
  could ever be inspected.

## Decision

1. **Classify before touching anything.** `classifyWorkspace`
   (`src/worker/workspace-classify.ts`) is a READ-ONLY function --
   `git status`/`rev-parse`/`merge-base`/`rev-list` only, never a write --
   called BEFORE `ensureWorktreeAndCommit` applies or commits anything.
   It returns one of exactly four outcomes: `ready_for_repair` (clean, at
   base, defect present), `repair_pending_commit` (dirty, but the ENTIRE
   changed-path set is exactly the one supported repair target with its
   content already matching the known fix), `repair_committed` (clean,
   HEAD is a real descendant of base with the repair present and the
   expected exact commit-count delta), or `unexpected` (everything else,
   with the offending path(s)/HEAD state named in the rejection reason).
2. **`commitAllowedPaths`, never `commitAll`/`git add -A`**
   (`src/worker/git.ts`). Stages EXACTLY the caller-supplied path list,
   verifies the worktree's changed-path set contains nothing else BEFORE
   staging anything, and verifies the worktree is fully clean AFTER
   committing -- so a path outside the allowed set is impossible to
   silently absorb, whether it was already dirty before this call or
   (in principle) became dirty during it.
3. **`unexpected` throws immediately, before any mutation.** No file is
   written, staged, committed, or deleted on this path -- the caller gets
   back exactly what made the state ambiguous, for inspection, never a
   best-effort attempt to "fix" it by overwriting or resetting anything.

## What this does not do

- It does not build a generic patch-reconciliation engine. The ONE
  allowed dirty state is still defined narrowly, in terms of this
  fixture's one known repair target and its two known contents (defect,
  fix) -- see `src/worker/repair.ts`. A different fixture/repair would
  need its own classification, not a generalized one.
- It does not change what counts as a genuinely completed result for a
  CLEAN worktree -- `checkCommittedRepairState` (shared by
  `classifyWorkspace` and `assertCleanWorktreeIsValidResult`) is
  unchanged Phase 2A.1 logic, just no longer reached via an unconditional
  repair-apply step first.

## Alternatives rejected

- **`git stash` the unexpected changes before repairing, then decide what
  to do with the stash**: still requires deciding what "the stash is
  safe to apply later" means, and silently moves evidence of an
  inconsistent workspace somewhere else rather than surfacing it --
  rejected for the same reason unconditional repair-and-commit was
  rejected.
- **`git clean -fd` + `git checkout -- .` to force the worktree back to a
  known-clean state before repairing**: this is exactly "delete/overwrite
  evidence of an inconsistent workspace," which the task this decision
  implements explicitly forbids.
