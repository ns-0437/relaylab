// Phase 2A.1, section 1: a narrow, NAMED sidecar-metadata check that a
// REUSED worktree directory actually belongs to the current
// (repo, taskId, attemptId, baseCommit) request -- not a generic agent
// reconciliation framework, just enough to stop a stale or
// differently-sourced worktree from being silently treated as "this
// task's completed work." The metadata file lives NEXT TO the worktree
// (never inside it, so it is never touched by `git add -A`/commits) at
// `${worktreeDir}.identity.json`.
import { readFile, writeFile } from "node:fs/promises";

export interface WorktreeIdentity {
  readonly taskId: string;
  readonly attemptId: string;
  /** The resolved, real (symlink-free) path of the SOURCE repo this worktree was created from -- not the worktree's own path. */
  readonly sourceRepoRealPath: string;
  readonly baseCommit: string;
  readonly branch: string;
  readonly createdAtIso: string;
}

export function identityFilePathFor(worktreeDir: string): string {
  return `${worktreeDir}.identity.json`;
}

export async function writeWorktreeIdentity(worktreeDir: string, identity: WorktreeIdentity): Promise<void> {
  await writeFile(identityFilePathFor(worktreeDir), JSON.stringify(identity, null, 2), "utf8");
}

export async function readWorktreeIdentity(worktreeDir: string): Promise<WorktreeIdentity | undefined> {
  try {
    const raw = await readFile(identityFilePathFor(worktreeDir), "utf8");
    return JSON.parse(raw) as WorktreeIdentity;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw err;
  }
}

/**
 * Compares a reused worktree's RECORDED identity against the CURRENT
 * call's expected identity. Throws a specific, field-naming error on any
 * mismatch rather than silently proceeding -- callers treat this as an
 * "unexpected workspace state," not something to paper over. A missing
 * identity file (e.g. a worktree created before this check existed, or
 * one created by a test that bypasses this module) is ALSO a mismatch:
 * callers must not assume compatibility with unidentified state.
 */
export function assertIdentityMatches(recorded: WorktreeIdentity | undefined, expected: Omit<WorktreeIdentity, "createdAtIso">): void {
  if (recorded === undefined) {
    throw new Error(
      `worktree exists on disk but has no recorded identity sidecar (expected one at "${identityFilePathFor("<worktreeDir>")}" pattern) -- refusing to reuse a worktree whose provenance cannot be verified`,
    );
  }
  const fields: Array<keyof Omit<WorktreeIdentity, "createdAtIso">> = ["taskId", "attemptId", "sourceRepoRealPath", "baseCommit", "branch"];
  for (const field of fields) {
    if (recorded[field] !== expected[field]) {
      throw new Error(
        `worktree identity mismatch on field "${field}": recorded="${recorded[field]}" vs. expected="${expected[field]}" -- refusing to reuse a worktree whose recorded identity does not match this request (this is exactly the "clean worktree belonging to a different repo/task/attempt/base" case this check exists to catch)`,
      );
    }
  }
}
