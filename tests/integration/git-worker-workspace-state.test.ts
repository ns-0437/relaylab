// Phase 2A.2: real Git, real MySQL tests driving the FULL real
// orchestration path (`runGitWorkerTask`, the exact function
// src/worker-main.ts's real CLI entrypoint calls -- never a direct call
// into `classifyWorkspace`/`assertCleanWorktreeIsValidResult` in
// isolation, which already have their own dedicated unit-style coverage
// in tests/integration/git-worker-crash.ts). Every scenario here
// manually constructs a REUSED worktree (identity sidecar written,
// matching this call's expected identity) in some deliberately
// inconsistent or interrupted state, then calls `runGitWorkerTask`
// exactly as a restarted worker process would, and asserts on the real,
// observable result: did it reject explicitly, did it leave the
// tampered path untouched, did it commit/checkpoint exactly what was
// expected.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, realpath, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type mysql from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";
import { createFixtureRepo, type FixtureRepo } from "../../src/worker/fixture-repo.js";
import { runGitWorkerTask, defaultAttemptId, branchNameFor } from "../../src/worker/run-task.js";
import { createManagedWorkspaceRoot, resolveWorktreePath } from "../../src/worker/workspace.js";
import { addWorktree, currentCommit, git, hasUncommittedChanges, makeIsolatedGitEnv } from "../../src/worker/git.js";
import { writeWorktreeIdentity } from "../../src/worker/worktree-identity.js";

let pool: mysql.Pool;
let managedRoot: string;
let fixtureParentDir: string;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
});

beforeEach(async () => {
  await truncateAll(pool);
  const isolatedRoot = await mkdtemp(path.join(tmpdir(), "relaylab-workspace-state-test-root-"));
  managedRoot = await createManagedWorkspaceRoot(isolatedRoot);
  fixtureParentDir = await mkdtemp(path.join(tmpdir(), "relaylab-workspace-state-test-"));
});

after(async () => {
  await pool.end();
});

async function freshFixture(): Promise<FixtureRepo> {
  const dir = path.join(fixtureParentDir, uniqueId("fixture"));
  return createFixtureRepo(dir);
}

async function claim(taskId: string, ownerId = "worker-a") {
  const store = makeTaskStoreLive(pool);
  return Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));
}

/**
 * Manually constructs a REUSED worktree (real `git worktree add` at the
 * fixture's base commit, plus a real, matching identity sidecar) --
 * exactly the on-disk shape a RESTARTED worker would find, before the
 * test tampers with it further. Returns the worktree dir and the env
 * every subsequent git call in a given test should reuse.
 */
async function setUpReusedWorktree(taskId: string, fixture: FixtureRepo, homeLabel: string) {
  const env = makeIsolatedGitEnv(path.join(fixtureParentDir, homeLabel));
  const attemptId = defaultAttemptId(taskId);
  const branch = branchNameFor(taskId);
  const worktreeDir = resolveWorktreePath(managedRoot, taskId, attemptId);
  await addWorktree(fixture.repoDir, worktreeDir, branch, fixture.baseCommit, env);
  // `run-task.ts`'s own identity check compares against `realpath(sourceRepoDir)`
  // (never the raw, possibly-8.3-short-name path) -- this must match
  // exactly, or every scenario below would fail on an identity mismatch
  // before ever reaching the classification this test is for.
  const sourceRepoRealPath = await realpath(fixture.repoDir);
  await writeWorktreeIdentity(worktreeDir, {
    taskId,
    attemptId,
    sourceRepoRealPath,
    baseCommit: fixture.baseCommit,
    branch,
    createdAtIso: new Date().toISOString(),
  });
  return { env, worktreeDir, attemptId };
}

async function checkpointCount(taskId: string): Promise<number> {
  const [rows] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  return rows[0]!.c;
}

test("real orchestration path: an unrelated UNSTAGED tracked-file edit (verify.js) is rejected -- no commit, no checkpoint, edit left untouched", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-unrelated-unstaged");
  const claimResult = await claim(taskId);
  const { env, worktreeDir } = await setUpReusedWorktree(taskId, fixture, "home-unrelated-unstaged");

  const tamperedPath = path.join(worktreeDir, "verify.js");
  const tamperedContent = (await readFile(tamperedPath, "utf8")) + "\n// unrelated edit, never staged\n";
  await writeFile(tamperedPath, tamperedContent, "utf8");
  const headBefore = await currentCommit(worktreeDir, env);

  await assert.rejects(
    () =>
      runGitWorkerTask({
        taskId,
        ownerId: "worker-a",
        epoch: claimResult.epoch,
        sourceRepoDir: fixture.repoDir,
        requestedCommitRef: fixture.baseCommit,
        managedWorkspaceRoot: managedRoot,
        pool,
      }),
    /unexpected workspace state/i,
  );

  assert.equal(await currentCommit(worktreeDir, env), headBefore, "no new commit may be created when rejecting");
  assert.equal(await checkpointCount(taskId), 0, "no checkpoint may be persisted when rejecting");
  assert.equal(await readFile(tamperedPath, "utf8"), tamperedContent, "the unrelated edit must be left EXACTLY as the test wrote it -- never reset, staged, or committed");
});

test("real orchestration path: an unexpected STAGED edit (verify.js) is rejected -- no commit, no checkpoint, staged edit left untouched", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-unexpected-staged");
  const claimResult = await claim(taskId);
  const { env, worktreeDir } = await setUpReusedWorktree(taskId, fixture, "home-unexpected-staged");

  const tamperedPath = path.join(worktreeDir, "verify.js");
  const tamperedContent = (await readFile(tamperedPath, "utf8")).replace("PASS", "PASS (tampered)");
  await writeFile(tamperedPath, tamperedContent, "utf8");
  await git(["add", "--", "verify.js"], worktreeDir, env);
  const headBefore = await currentCommit(worktreeDir, env);

  await assert.rejects(
    () =>
      runGitWorkerTask({
        taskId,
        ownerId: "worker-a",
        epoch: claimResult.epoch,
        sourceRepoDir: fixture.repoDir,
        requestedCommitRef: fixture.baseCommit,
        managedWorkspaceRoot: managedRoot,
        pool,
      }),
    /unexpected workspace state/i,
  );

  assert.equal(await currentCommit(worktreeDir, env), headBefore, "no new commit may be created when rejecting");
  assert.equal(await checkpointCount(taskId), 0, "no checkpoint may be persisted when rejecting");
  assert.equal(await readFile(tamperedPath, "utf8"), tamperedContent, "the staged edit must be left EXACTLY as the test wrote it -- never unstaged, reset, or committed");
  const staged = await git(["diff", "--cached", "--name-only"], worktreeDir, env);
  assert.equal(staged.stdout.trim(), "verify.js", "the staged edit must remain staged, exactly as the test left it");
});

test("real orchestration path: an unexpected UNTRACKED file is rejected -- no commit, no checkpoint, the file is left in place", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-unexpected-untracked");
  const claimResult = await claim(taskId);
  const { env, worktreeDir } = await setUpReusedWorktree(taskId, fixture, "home-unexpected-untracked");

  const untrackedPath = path.join(worktreeDir, "src", "extra.js");
  await writeFile(untrackedPath, "module.exports = { unrelated: true };\n", "utf8");
  const headBefore = await currentCommit(worktreeDir, env);

  await assert.rejects(
    () =>
      runGitWorkerTask({
        taskId,
        ownerId: "worker-a",
        epoch: claimResult.epoch,
        sourceRepoDir: fixture.repoDir,
        requestedCommitRef: fixture.baseCommit,
        managedWorkspaceRoot: managedRoot,
        pool,
      }),
    /unexpected workspace state/i,
  );

  assert.equal(await currentCommit(worktreeDir, env), headBefore, "no new commit may be created when rejecting");
  assert.equal(await checkpointCount(taskId), 0, "no checkpoint may be persisted when rejecting");
  assert.equal(await readFile(untrackedPath, "utf8"), "module.exports = { unrelated: true };\n", "the untracked file must be left in place, never deleted or swept into a commit");
});

test("real orchestration path: the repair target IS fixed AND verify.js is ALSO dirty -- rejected, not silently absorbed into the repair commit", async () => {
  // This is the direct test of the vulnerability this patch closes: the
  // repair itself is genuinely, correctly present, but an UNRELATED
  // file is also dirty. The old unconditional `git add -A` would have
  // swept verify.js's edit into the "scripted repair" commit without
  // anyone ever deciding that was safe.
  const fixture = await freshFixture();
  const taskId = uniqueId("task-repair-plus-unrelated");
  const claimResult = await claim(taskId);
  const { env, worktreeDir } = await setUpReusedWorktree(taskId, fixture, "home-repair-plus-unrelated");

  const addJsPath = path.join(worktreeDir, "src", "add.js");
  const fixedAddJs = (await readFile(addJsPath, "utf8")).replace("return a - b;", "return a + b;");
  await writeFile(addJsPath, fixedAddJs, "utf8");

  const verifyJsPath = path.join(worktreeDir, "verify.js");
  const tamperedVerifyJs = (await readFile(verifyJsPath, "utf8")) + "\n// unrelated edit alongside a genuinely correct repair\n";
  await writeFile(verifyJsPath, tamperedVerifyJs, "utf8");
  const headBefore = await currentCommit(worktreeDir, env);

  await assert.rejects(
    () =>
      runGitWorkerTask({
        taskId,
        ownerId: "worker-a",
        epoch: claimResult.epoch,
        sourceRepoDir: fixture.repoDir,
        requestedCommitRef: fixture.baseCommit,
        managedWorkspaceRoot: managedRoot,
        pool,
      }),
    /unexpected workspace state/i,
  );

  assert.equal(await currentCommit(worktreeDir, env), headBefore, "no new commit may be created when rejecting -- in particular, the correct repair must NOT be committed together with the unrelated edit");
  assert.equal(await checkpointCount(taskId), 0, "no checkpoint may be persisted when rejecting");
  assert.equal(await readFile(addJsPath, "utf8"), fixedAddJs, "the repair's own edit must be left in place, untouched");
  assert.equal(await readFile(verifyJsPath, "utf8"), tamperedVerifyJs, "the unrelated edit must be left EXACTLY as the test wrote it");
  assert.equal(await hasUncommittedChanges(worktreeDir, env), true, "the worktree must remain dirty -- nothing was committed");
});

test("real orchestration path: a clean worktree whose HEAD moved to an unrelated commit (never the repair) is rejected", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-unexpected-clean-head");
  const claimResult = await claim(taskId);
  const { env, worktreeDir } = await setUpReusedWorktree(taskId, fixture, "home-unexpected-clean-head");

  // Advance HEAD with a real, committed, UNRELATED change -- clean
  // afterwards, but the repair was never applied.
  const verifyJsPath = path.join(worktreeDir, "verify.js");
  await writeFile(verifyJsPath, (await readFile(verifyJsPath, "utf8")) + "\n// an unrelated, already-committed change\n", "utf8");
  await git(["add", "--", "verify.js"], worktreeDir, env);
  await git(["-c", "user.name=tamper", "-c", "user.email=tamper@localhost", "commit", "--no-verify", "-m", "an unrelated commit, not the scripted repair"], worktreeDir, env);
  const unrelatedHead = await currentCommit(worktreeDir, env);
  assert.notEqual(unrelatedHead, fixture.baseCommit);
  assert.equal(await hasUncommittedChanges(worktreeDir, env), false, "test setup check: must be clean after the unrelated commit");

  await assert.rejects(
    () =>
      runGitWorkerTask({
        taskId,
        ownerId: "worker-a",
        epoch: claimResult.epoch,
        sourceRepoDir: fixture.repoDir,
        requestedCommitRef: fixture.baseCommit,
        managedWorkspaceRoot: managedRoot,
        pool,
      }),
    /unexpected workspace state/i,
  );

  assert.equal(await currentCommit(worktreeDir, env), unrelatedHead, "HEAD must remain exactly the unrelated commit -- never moved, reset, or added to");
  assert.equal(await checkpointCount(taskId), 0, "no checkpoint may be persisted when rejecting");
});

test("real orchestration path: the known interrupted-repair state (fix written, not yet committed) recovers successfully through runGitWorkerTask", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-interrupted-repair-recovers");
  const claimResult = await claim(taskId);
  const { env, worktreeDir } = await setUpReusedWorktree(taskId, fixture, "home-interrupted-repair-recovers");

  const addJsPath = path.join(worktreeDir, "src", "add.js");
  const fixedAddJs = (await readFile(addJsPath, "utf8")).replace("return a - b;", "return a + b;");
  await writeFile(addJsPath, fixedAddJs, "utf8");
  assert.equal(await hasUncommittedChanges(worktreeDir, env), true, "test setup check: the fix must be written but NOT yet committed");
  assert.equal(await currentCommit(worktreeDir, env), fixture.baseCommit, "test setup check: HEAD must still be exactly the base commit");

  const outcome = await runGitWorkerTask({
    taskId,
    ownerId: "worker-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
  });

  assert.equal(outcome.kind, "checkpointed", "the known interrupted-repair state must recover successfully, not be rejected");
  if (outcome.kind !== "checkpointed") return;
  assert.equal(outcome.manifest.baseCommit, fixture.baseCommit);
  assert.notEqual(outcome.manifest.resultCommit, fixture.baseCommit, "a real new commit must have been created for the resumed repair");
  assert.equal(await hasUncommittedChanges(worktreeDir, env), false, "the worktree must be clean after the resumed repair is committed");
  assert.equal(await readFile(addJsPath, "utf8"), fixedAddJs, "the committed content must be EXACTLY the fix this test wrote -- nothing re-applied or altered it");
  assert.equal(await checkpointCount(taskId), 1, "expected exactly one durable checkpoint");
});
