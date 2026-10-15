// Phase 2A: THE ONE explicitly scripted, deterministic repair this
// milestone implements. No LLM, no generic patch/diff application, no
// task-supplied code -- a single, named, hardcoded text substitution
// against a specific, known file. Labeled clearly as scripted everywhere
// it is referenced (CLAUDE.md, docs/ARCHITECTURE.md, this file).
//
// Target fixture defect: `src/add.js` in the worker fixture repository
// (see tests/integration/git-worker-fixture.ts, which creates that
// repository) contains `return a - b;` where the intended, tested
// behavior is addition. This function changes exactly that line.
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";

export const REPAIR_NAME = "fix-add-subtraction-bug-v1";

const TARGET_RELATIVE_PATH = "src/add.js";
const DEFECT_LINE = "  return a - b;";
const FIXED_LINE = "  return a + b;";

export interface RepairResult {
  /** Whether this call actually changed the file (false = it already had the fix -- idempotent re-application, e.g. after a worker restart). */
  changed: boolean;
}

/**
 * Idempotent by construction: re-running this against a worktree that
 * ALREADY has the fix applied (e.g. because a prior attempt applied it
 * and then the worker crashed before committing) reads the current file
 * content, sees the defect line is already gone, and makes NO write --
 * this is what lets restart reconciliation (Part G) safely re-run the
 * repair step without risk of corrupting an already-repaired file or
 * producing a different result the second time.
 */
export async function applyScriptedRepair(worktreeDir: string): Promise<RepairResult> {
  const targetPath = path.join(worktreeDir, TARGET_RELATIVE_PATH);
  const original = await readFile(targetPath, "utf8");
  if (!original.includes(DEFECT_LINE)) {
    if (original.includes(FIXED_LINE)) {
      return { changed: false }; // already repaired -- idempotent no-op
    }
    throw new Error(
      `${REPAIR_NAME}: expected to find the known defect line in ${TARGET_RELATIVE_PATH}, found neither it nor the fixed line -- refusing to guess. File may be a different fixture revision than this repair targets.`,
    );
  }
  const patched = original.replace(DEFECT_LINE, FIXED_LINE);
  await writeFile(targetPath, patched, "utf8");
  return { changed: true };
}

/**
 * Phase 2A.1, section 1: a NAMED, explicit check for "is the intended
 * repair actually present in this worktree's current files" -- reusing
 * the exact same constants `applyScriptedRepair` uses, so the two can
 * never silently drift apart. Used by the reconciliation path in
 * run-task.ts to make the "clean worktree already has the fix" case an
 * EXPLICIT, asserted fact rather than something merely implied by
 * `applyScriptedRepair`'s own idempotency. Returns false (not an error)
 * if the file is missing or matches neither known line -- callers
 * decide what that means for their own state machine.
 */
export async function isRepairPresent(worktreeDir: string): Promise<boolean> {
  const targetPath = path.join(worktreeDir, TARGET_RELATIVE_PATH);
  let content: string;
  try {
    content = await readFile(targetPath, "utf8");
  } catch {
    return false;
  }
  return content.includes(FIXED_LINE) && !content.includes(DEFECT_LINE);
}

export { TARGET_RELATIVE_PATH };
