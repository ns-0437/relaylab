// Phase 2A: thin, explicit wrappers around real `git` invocations. Every
// call goes through `execFile` with an ARGUMENT ARRAY (never a shell
// string) -- there is no shell interpolation anywhere in this module, so
// a value containing spaces, quotes, or shell metacharacters is passed
// to the `git` process exactly as given, never re-parsed by a shell.
// See src/worker/workspace.ts for the managed-workspace path boundary
// this module's callers are expected to enforce BEFORE calling into it
// (this module itself does not know what "the managed workspace" is).
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

/**
 * Environment every `git` invocation in this module runs with -- NEVER
 * the caller's own ambient git config/credentials/hooks. "Account for
 * local Git hooks/signing/configuration so the controlled fixture does
 * not unexpectedly execute unrelated hooks, hang for credentials, or
 * require the user's identity" (Part D): `GIT_CONFIG_NOSYSTEM=1` and
 * `HOME`/`XDG_CONFIG_HOME` pointed at a throwaway, EMPTY directory
 * (created once per worker process, see `makeIsolatedGitEnv`) mean this
 * project's own `~/.gitconfig` (hooks path, commit signing, credential
 * helpers, aliases) is never read. `core.hooksPath=/dev/null`-equivalent
 * (an empty directory with no executable files) additionally guarantees
 * no hook script runs even if one were somehow present. Commit identity
 * is supplied explicitly per-call (`-c user.name=... -c user.email=...`)
 * rather than relying on ANY discovered identity. `GIT_TERMINAL_PROMPT=0`
 * means an operation that would otherwise prompt for credentials fails
 * immediately instead of hanging. None of this makes running an
 * ARBITRARY repository safe -- see src/worker/workspace.ts and
 * docs/ARCHITECTURE.md "Phase 2A worker" for the explicit, narrow trust
 * boundary this project actually operates within (a disposable fixture
 * repository the test harness itself creates, never a real/untrusted one).
 */
export function makeIsolatedGitEnv(isolatedHomeDir: string): NodeJS.ProcessEnv {
  return {
    ...process.env,
    HOME: isolatedHomeDir,
    USERPROFILE: isolatedHomeDir, // Windows equivalent of HOME for some tools
    XDG_CONFIG_HOME: isolatedHomeDir,
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_TERMINAL_PROMPT: "0",
    GIT_ASKPASS: "echo", // never prompts; returns empty credentials immediately if somehow reached
    GIT_SSH_COMMAND: "false", // any SSH transport attempt fails immediately rather than hanging
  };
}

export interface GitResult {
  stdout: string;
  stderr: string;
}

export class GitCommandError extends Error {
  readonly _tag = "GitCommandError";
  constructor(
    readonly args: readonly string[],
    readonly exitCode: number | null,
    readonly stderrExcerpt: string,
  ) {
    super(`git ${args.join(" ")} failed (exit ${exitCode}): ${stderrExcerpt.slice(0, 500)}`);
  }
}

const GIT_TIMEOUT_MS = 30_000;
const MAX_OUTPUT_BYTES = 1024 * 1024; // 1 MiB cap on captured stdout/stderr

/** Runs `git <args>` with NO shell involved (argument array, not a command string), bounded output, and a bounded timeout. */
export async function git(args: readonly string[], cwd: string, env: NodeJS.ProcessEnv): Promise<GitResult> {
  try {
    const { stdout, stderr } = await execFileAsync("git", args as string[], {
      cwd,
      env,
      timeout: GIT_TIMEOUT_MS,
      maxBuffer: MAX_OUTPUT_BYTES,
      windowsHide: true,
    });
    return { stdout, stderr };
  } catch (err) {
    const e = err as { code?: number | string; killed?: boolean; signal?: string; stderr?: string; message?: string };
    const exitCode = typeof e.code === "number" ? e.code : null;
    throw new GitCommandError(args, exitCode, e.stderr ?? e.message ?? "unknown error");
  }
}

/** `git rev-parse --verify <ref>^{commit}` -- resolves a ref/SHA to a full 40-char commit SHA, rejecting anything that isn't a real, existing commit (never silently falls back to HEAD or any other default). */
export async function resolveCommit(repoDir: string, ref: string, env: NodeJS.ProcessEnv): Promise<string> {
  // `^{commit}` is git's own syntax for "must resolve to a commit object,
  // not a tag/tree/blob" -- rejects a non-commit ref explicitly rather
  // than silently accepting it.
  const { stdout } = await git(["rev-parse", "--verify", `${ref}^{commit}`], repoDir, env);
  const sha = stdout.trim();
  if (!/^[0-9a-f]{40}$/.test(sha)) {
    throw new Error(`git rev-parse did not return a full 40-character commit SHA for "${ref}": "${sha}"`);
  }
  return sha;
}

/** `git rev-parse HEAD` inside a worktree -- the actual current commit, never assumed. */
export async function currentCommit(worktreeDir: string, env: NodeJS.ProcessEnv): Promise<string> {
  const { stdout } = await git(["rev-parse", "HEAD"], worktreeDir, env);
  return stdout.trim();
}

/** `git rev-parse HEAD^{tree}` -- the actual tree object ID HEAD points at. */
export async function currentTree(worktreeDir: string, env: NodeJS.ProcessEnv): Promise<string> {
  const { stdout } = await git(["rev-parse", "HEAD^{tree}"], worktreeDir, env);
  return stdout.trim();
}

/**
 * Adds a worktree at `worktreeDir`, on a NEW branch `branchName`, starting
 * from `startCommit` -- NOT whatever HEAD currently points to in the
 * source repo. `-b` creates the branch; `--no-checkout` is deliberately
 * NOT used (this worker needs real files on disk to patch).
 */
export async function addWorktree(sourceRepoDir: string, worktreeDir: string, branchName: string, startCommit: string, env: NodeJS.ProcessEnv): Promise<void> {
  await git(["worktree", "add", "-b", branchName, worktreeDir, startCommit], sourceRepoDir, env);
}

export interface GitTreeEntry {
  /** The raw octal mode string git reports, e.g. "100644" (file), "100755" (executable file), "120000" (symlink), "040000" (subdirectory), "160000" (gitlink/submodule). */
  mode: string;
  type: string;
  object: string;
  path: string;
}

/**
 * Phase 2C.1: `git ls-tree -r -z --full-tree <ref>` -- every entry in
 * `ref`'s tree, recursively, with its RECORDED mode (the mode git
 * itself assigned when the blob was committed, independent of the
 * current host's filesystem or `core.fileMode` setting). `-z` (NUL-
 * terminated, same convention `changedPaths` above already uses) avoids
 * any ambiguity from a path containing a literal tab or newline. Used by
 * `src/worker/docker-verify.ts`'s `materializeCandidateInput` to refuse
 * archiving a tree containing an entry this project's narrow export path
 * does not support (a symlink or a submodule gitlink) BEFORE calling
 * `git archive`, rather than silently materializing an incomplete or
 * unsafe input.
 */
export async function listTreeEntries(repoDir: string, ref: string, env: NodeJS.ProcessEnv): Promise<GitTreeEntry[]> {
  const { stdout } = await git(["ls-tree", "-r", "-z", "--full-tree", ref], repoDir, env);
  return stdout
    .split("\0")
    .filter((s) => s.length > 0)
    .map((line) => {
      const tabIndex = line.indexOf("\t");
      const meta = line.slice(0, tabIndex);
      const filePath = line.slice(tabIndex + 1);
      const [mode, type, object] = meta.split(" ");
      return { mode: mode!, type: type!, object: object!, path: filePath };
    });
}

/**
 * Phase 2C correctness patch, Part 3: `git cat-file -p <object>` --
 * the RAW, STORED bytes of a blob, completely unaffected by
 * `.gitattributes` `export-ignore`/`export-subst` (both are
 * `git archive`-time-only behaviors; `cat-file -p` never applies
 * them -- empirically confirmed against this project's own Git).
 * Binary-safe: invoked with `encoding: "buffer"` directly (NOT
 * through `git()` above, which decodes stdout as a utf8 STRING and
 * would corrupt binary content). Used by
 * `src/worker/docker-verify.ts`'s `materializeCandidateInput` to read
 * every tracked blob's EXACT content directly from the object store,
 * never through a second, independently-filtering archive export.
 */
export async function catFileBlob(repoDir: string, object: string, env: NodeJS.ProcessEnv): Promise<Buffer> {
  try {
    const { stdout } = await execFileAsync("git", ["cat-file", "-p", object], {
      cwd: repoDir,
      env,
      timeout: GIT_TIMEOUT_MS,
      maxBuffer: MAX_OUTPUT_BYTES,
      windowsHide: true,
      encoding: "buffer",
    });
    return stdout as unknown as Buffer;
  } catch (err) {
    const e = err as { code?: number | string; stderr?: Buffer | string; message?: string };
    const exitCode = typeof e.code === "number" ? e.code : null;
    const stderrText = Buffer.isBuffer(e.stderr) ? e.stderr.toString("utf8") : e.stderr ?? e.message ?? "unknown error";
    throw new GitCommandError(["cat-file", "-p", object], exitCode, stderrText);
  }
}

/** Removes a worktree. `--force` only discards the WORKTREE CHECKOUT (uncommitted changes in it), never touches the source repo's own history/branches. */
export async function removeWorktree(sourceRepoDir: string, worktreeDir: string, env: NodeJS.ProcessEnv): Promise<void> {
  await git(["worktree", "remove", "--force", worktreeDir], sourceRepoDir, env).catch(() => {
    // Best-effort: if the worktree directory was already removed from
    // disk (e.g. a prior cleanup partially succeeded), `git worktree
    // remove` fails; that is not itself a correctness problem for the
    // caller, which is cleaning up defensively.
  });
}

/**
 * Phase 2A.2: structured `git status --porcelain=v1 -z` output -- the
 * NUL-terminated form, parsed into one entry per changed path (status
 * code + path), never a raw string a caller might substring-match
 * against. Used by `classifyWorkspace` (src/worker/workspace-classify.ts)
 * to decide EXACTLY which paths are dirty before deciding whether that
 * is a supported state -- `hasUncommittedChanges` below answers only
 * "is anything dirty," which is not enough to tell "the expected repair
 * target is dirty" apart from "something unrelated is dirty." Rename/
 * copy entries (status starting with `R`/`C`) carry a second,
 * NUL-separated "original path" field in the `-z` format; this fixture
 * never produces one, but the second field is still consumed (not left
 * to be misparsed as an unrelated entry) for correctness.
 */
export interface ChangedPathEntry {
  /** The raw two-character porcelain status code (e.g. "??", " M", "A ", "M "). */
  status: string;
  /** The (possibly renamed-to) path, relative to the worktree root. */
  path: string;
}

export async function changedPaths(worktreeDir: string, env: NodeJS.ProcessEnv): Promise<ChangedPathEntry[]> {
  const { stdout } = await git(["status", "--porcelain=v1", "-z"], worktreeDir, env);
  const parts = stdout.split("\0").filter((s) => s.length > 0);
  const entries: ChangedPathEntry[] = [];
  let i = 0;
  while (i < parts.length) {
    const entry = parts[i]!;
    const status = entry.slice(0, 2);
    const filePath = entry.slice(3);
    entries.push({ status, path: filePath });
    // A rename/copy entry is followed by one extra NUL-separated field
    // (the original path) in the `-z` format -- consume and discard it
    // rather than let the next loop iteration misparse it as its own entry.
    i += status[0] === "R" || status[0] === "C" ? 2 : 1;
  }
  return entries;
}

/**
 * Stages EXACTLY the given paths (never `git add -A`/`-u`, which would
 * silently absorb ANY other dirty path into this commit -- see
 * docs/decisions/0010-explicit-path-commits.md) and commits them with an
 * explicit author/committer identity (never relying on discovered config
 * -- see `makeIsolatedGitEnv`). Verifies the worktree's changed-path set
 * BOTH before staging (every dirty path must be in `allowedPaths`, or
 * this refuses to touch anything) AND after committing (the worktree
 * must end up fully clean -- anything still dirty means something
 * outside `allowedPaths` was present and is now EXPOSED, not hidden, by
 * this check). Returns the new commit's full SHA.
 */
export async function commitAllowedPaths(worktreeDir: string, allowedPaths: readonly string[], message: string, env: NodeJS.ProcessEnv): Promise<string> {
  const before = await changedPaths(worktreeDir, env);
  const unexpectedBefore = before.filter((entry) => !allowedPaths.includes(entry.path));
  if (unexpectedBefore.length > 0) {
    throw new Error(
      `commitAllowedPaths: refusing to commit -- found changed path(s) outside the allowed set ${JSON.stringify(allowedPaths)}: ${unexpectedBefore.map((e) => `${e.status} ${e.path}`).join(", ")}`,
    );
  }
  if (before.length === 0) {
    throw new Error(`commitAllowedPaths: refusing to commit -- no changed paths at all (expected at least one of ${JSON.stringify(allowedPaths)} to be dirty)`);
  }

  await git(["add", "--", ...allowedPaths], worktreeDir, env);
  await git(
    [
      "-c",
      "user.name=relaylab-worker",
      "-c",
      "user.email=relaylab-worker@localhost",
      "-c",
      "commit.gpgsign=false",
      "-c",
      "tag.gpgsign=false",
      "commit",
      "--no-verify", // never runs a pre-commit/commit-msg hook, even a benign one -- see the file header comment
      "-m",
      message,
    ],
    worktreeDir,
    env,
  );
  const sha = await currentCommit(worktreeDir, env);

  const after = await changedPaths(worktreeDir, env);
  if (after.length > 0) {
    throw new Error(
      `commitAllowedPaths: worktree still has changed path(s) after committing the allowed set ${JSON.stringify(allowedPaths)} -- something outside it remains uncommitted: ${after.map((e) => `${e.status} ${e.path}`).join(", ")}`,
    );
  }
  return sha;
}

/** `git status --porcelain` -- true if the worktree has any uncommitted change (staged, unstaged, or untracked). Used to decide whether a repair actually needs to be (re-)applied -- see src/worker/repair.ts. */
export async function hasUncommittedChanges(worktreeDir: string, env: NodeJS.ProcessEnv): Promise<boolean> {
  const { stdout } = await git(["status", "--porcelain"], worktreeDir, env);
  return stdout.trim().length > 0;
}

/**
 * Phase 2A.1, section 2/3: `git merge-base --is-ancestor <ancestor>
 * <descendant>` -- true iff `ancestor` is a (possibly non-strict, i.e.
 * equal-is-ancestor-of-itself) ancestor of `descendant` in `worktreeDir`'s
 * object graph. Exit code 0 = yes, 1 = no (NOT an error -- git's own
 * documented convention for this subcommand); any other exit code is a
 * genuine failure (e.g. one of the two commits doesn't exist) and is
 * rethrown. Used to assert "the result commit's history still contains
 * the base commit" (expected ancestry) after a restart.
 */
export async function isAncestor(worktreeDir: string, ancestor: string, descendant: string, env: NodeJS.ProcessEnv): Promise<boolean> {
  try {
    await git(["merge-base", "--is-ancestor", ancestor, descendant], worktreeDir, env);
    return true;
  } catch (err) {
    if (err instanceof GitCommandError && err.exitCode === 1) return false;
    throw err;
  }
}

/** `git rev-list --count <from>..<to>` -- the number of commits reachable from `to` but not `from`. Used to assert the exact, expected commit-count delta from base to result (e.g. "exactly one new commit -- the scripted repair -- was added, no more, no fewer"). */
export async function commitCountBetween(worktreeDir: string, from: string, to: string, env: NodeJS.ProcessEnv): Promise<number> {
  const { stdout } = await git(["rev-list", "--count", `${from}..${to}`], worktreeDir, env);
  const n = Number.parseInt(stdout.trim(), 10);
  if (!Number.isFinite(n)) {
    throw new Error(`git rev-list --count did not return a parseable integer for ${from}..${to}: "${stdout.trim()}"`);
  }
  return n;
}

/**
 * AI repair milestone: `git reset --hard <commit>` -- discards BOTH the
 * current commit (moves HEAD back to `commit`) AND any uncommitted
 * working-tree changes. Used only by run-task-ai.ts's bounded retry
 * loop, to discard a failed attempt's committed (but verification-
 * rejected) edit before generating and committing a fresh one against
 * the same clean base -- never used by the deterministic repair path,
 * which has no retry loop to reset between.
 */
export async function resetHard(worktreeDir: string, commit: string, env: NodeJS.ProcessEnv): Promise<void> {
  await git(["reset", "--hard", commit], worktreeDir, env);
}

/**
 * AI repair milestone: `git diff --name-only <from> <to>` -- the set of
 * paths that differ between two COMMITS (not worktree status, which
 * `changedPaths` above reports). Used to classify a reused worktree
 * whose HEAD has already moved past the base commit: confirms the
 * committed diff touches only the expected allowlisted path before
 * trusting it as a resumable prior AI attempt, rather than assuming so
 * from commit count alone.
 */
export async function changedPathsBetweenCommits(worktreeDir: string, from: string, to: string, env: NodeJS.ProcessEnv): Promise<string[]> {
  const { stdout } = await git(["diff", "--name-only", `${from}..${to}`], worktreeDir, env);
  return stdout.split("\n").filter((s) => s.length > 0);
}
