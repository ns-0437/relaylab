// Phase 2A: real Git, real MySQL, real process (verification subprocess)
// tests for the smallest real execution worker. Covers Part H tests
// 1-9; crash/restart reconciliation (10-12) live in
// tests/integration/git-worker-crash.ts (needs real forked OS
// processes + failpoints, kept separate per this project's existing
// `.manual.ts`/crash-windows.ts convention for process-kill tests).
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type mysql from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";
import { createFixtureRepo } from "../../src/worker/fixture-repo.js";
import { runGitWorkerTask, defaultAttemptId, branchNameFor, checkpointCommandId, assertRequestedPathsAreSafe } from "../../src/worker/run-task.js";
import { assertNoSymlinkEscapes, assertSafeForDeletion, createManagedWorkspaceRoot, removeManagedWorkspaceRoot, resolveWorktreePath } from "../../src/worker/workspace.js";
import { git, makeIsolatedGitEnv, resolveCommit, currentCommit, commitAllowedPaths } from "../../src/worker/git.js";
import { runVerification } from "../../src/worker/verify.js";

let pool: mysql.Pool;
let managedRoot: string;
let fixtureParentDir: string;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
});

beforeEach(async () => {
  await truncateAll(pool);
  // An EXPLICIT, freshly randomized root per test (NOT the default
  // stable/shared one `createManagedWorkspaceRoot()` falls back to,
  // which is what src/worker-main.ts's real CLI path and the crash/
  // restart test rely on for restart reconciliation) -- these tests
  // want isolation and cleanup, not persistence across runs.
  const isolatedRoot = await mkdtemp(path.join(tmpdir(), "relaylab-git-worker-test-root-"));
  managedRoot = await createManagedWorkspaceRoot(isolatedRoot);
  fixtureParentDir = await mkdtemp(path.join(tmpdir(), "relaylab-git-worker-test-"));
});

after(async () => {
  await pool.end();
});

async function freshFixture() {
  const dir = path.join(fixtureParentDir, uniqueId("fixture"));
  return createFixtureRepo(dir);
}

async function claim(taskId: string, ownerId = "worker-a") {
  const store = makeTaskStoreLive(pool);
  return Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));
}

test("1. checkout starts at the REQUESTED commit, not whatever HEAD currently points to", async () => {
  const fixture = await freshFixture();
  // Advance the fixture repo's own HEAD past the base commit, so "start
  // from the requested commit" and "start from HEAD" would disagree if
  // the worker got this wrong.
  const isolatedHome = path.join(fixtureParentDir, "home-advance");
  const env = makeIsolatedGitEnv(isolatedHome);
  await writeFile(path.join(fixture.repoDir, "README.md"), "unrelated later change\n", "utf8");
  await git(["add", "-A"], fixture.repoDir, env);
  await git(["-c", "user.name=x", "-c", "user.email=x@x", "commit", "--no-verify", "-m", "advance HEAD past base"], fixture.repoDir, env);
  const advancedHead = await currentCommit(fixture.repoDir, env);
  assert.notEqual(advancedHead, fixture.baseCommit);

  const taskId = uniqueId("task-checkout");
  const claimResult = await claim(taskId);
  const outcome = await runGitWorkerTask({
    taskId,
    ownerId: "worker-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit, // NOT "HEAD"
    managedWorkspaceRoot: managedRoot,
    pool,
  });
  assert.equal(outcome.kind, "checkpointed");
  if (outcome.kind === "checkpointed") {
    assert.equal(outcome.manifest.baseCommit, fixture.baseCommit, "must have started from the explicitly requested commit");
    assert.notEqual(outcome.manifest.baseCommit, advancedHead);
  }
});

test("2. the original fixture repository's OWN worktree/files remain unchanged by the worker", async () => {
  const fixture = await freshFixture();
  const beforeContent = await readFile(path.join(fixture.repoDir, "src", "add.js"), "utf8");
  assert.ok(beforeContent.includes("return a - b;"), "sanity: the source repo starts with the known bug");

  const taskId = uniqueId("task-source-unchanged");
  const claimResult = await claim(taskId);
  await runGitWorkerTask({
    taskId,
    ownerId: "worker-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
  });

  const afterContent = await readFile(path.join(fixture.repoDir, "src", "add.js"), "utf8");
  assert.equal(afterContent, beforeContent, "the SOURCE repository's own checked-out files must be byte-for-byte unchanged -- all work happened in a separate worktree");
});

test("3. baseline verification FAILS for the intended assertion; the repaired implementation PASSES", async () => {
  const fixture = await freshFixture();
  const isolatedHome = path.join(fixtureParentDir, "home-baseline");
  const env = makeIsolatedGitEnv(isolatedHome);

  // Baseline: run verification directly against the UNMODIFIED fixture checkout.
  const baseline = await runVerification(fixture.repoDir, env);
  assert.equal(baseline.outcome, "failed", "the known defect must make baseline verification fail");
  assert.notEqual(baseline.exitCode, 0);

  // Repaired, via the real worker path.
  const taskId = uniqueId("task-repair-passes");
  const claimResult = await claim(taskId);
  const outcome = await runGitWorkerTask({
    taskId,
    ownerId: "worker-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
  });
  assert.equal(outcome.kind, "checkpointed");
  if (outcome.kind === "checkpointed") assert.equal(outcome.manifest.verification.outcome, "passed");
});

test("4. the repair does NOT edit the test file (verify.js) -- only the implementation", async () => {
  const fixture = await freshFixture();
  const originalVerifyJs = await readFile(path.join(fixture.repoDir, "verify.js"), "utf8");

  const taskId = uniqueId("task-tests-unchanged");
  const claimResult = await claim(taskId);
  await runGitWorkerTask({
    taskId,
    ownerId: "worker-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
  });

  const worktreeDir = resolveWorktreePath(managedRoot, taskId, defaultAttemptId(taskId));
  const verifyJsAfter = await readFile(path.join(worktreeDir, "verify.js"), "utf8");
  assert.equal(verifyJsAfter, originalVerifyJs, "verify.js must be byte-for-byte identical to the fixture's original -- the repair only ever touches src/add.js");
});

test("5. the persisted manifest's commit/tree IDs match the ACTUAL resulting Git state", async () => {
  const fixture = await freshFixture();
  const isolatedHome = path.join(fixtureParentDir, "home-manifest-match");
  const env = makeIsolatedGitEnv(isolatedHome);

  const taskId = uniqueId("task-manifest-match");
  const claimResult = await claim(taskId);
  const outcome = await runGitWorkerTask({
    taskId,
    ownerId: "worker-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
  });
  assert.equal(outcome.kind, "checkpointed");
  if (outcome.kind !== "checkpointed") return;

  const worktreeDir = resolveWorktreePath(managedRoot, taskId, defaultAttemptId(taskId));
  const realHead = await currentCommit(worktreeDir, env);
  const { stdout: realTree } = await git(["rev-parse", "HEAD^{tree}"], worktreeDir, env);
  assert.equal(outcome.manifest.resultCommit, realHead, "manifest.resultCommit must equal the worktree's REAL current HEAD");
  assert.equal(outcome.manifest.resultTree, realTree.trim(), "manifest.resultTree must equal the REAL tree object HEAD points at");

  const [rows] = await pool.query<any[]>(`SELECT base_commit, result_commit, result_tree FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal(rows[0]!.result_commit, realHead, "the DURABLY STORED row must also match reality, not just the in-memory manifest");
  assert.equal(rows[0]!.result_tree, realTree.trim());
});

test("6. a verification FAILURE prevents successful completion -- no checkpoint is published", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-verify-fails");
  const claimResult = await claim(taskId);
  const outcome = await runGitWorkerTask({
    taskId,
    ownerId: "worker-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    skipRepairForTesting: true, // the defect is never fixed -- verification must genuinely fail
  });
  assert.equal(outcome.kind, "verification_failed");
  if (outcome.kind === "verification_failed") assert.notEqual(outcome.verification.exitCode, 0);

  const [rows] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal(rows[0]!.c, 0, "NO checkpoint row may exist when verification failed");
  const [task] = await pool.query<any[]>(`SELECT checkpoint_digest FROM tasks WHERE task_id = ?`, [taskId]);
  assert.equal(task[0]!.checkpoint_digest, null, "the task's own checkpoint_digest must remain unset");
});

test("Phase 3A Part A: a fixture whose verify.js TAMPERS with a tracked file and exits zero still prevents a successful checkpoint", async () => {
  // Real, end-to-end exercise of the input-integrity check through the
  // ACTUAL worker orchestration (run-task.ts), not just the adapter in
  // isolation (see tests/integration/docker-verify.test.ts for that).
  // The scripted repair genuinely runs and fixes add.js (this is NOT
  // "verification fails because the defect is unrepaired" -- test 6
  // above already covers that distinct case); the ONLY thing wrong is
  // that the committed `verify.js` itself tampers with a different
  // tracked file and exits 0, claiming success.
  const fixture = await freshFixture();
  const env = makeIsolatedGitEnv(path.join(fixtureParentDir, "home-tamper"));

  const tamperingVerifyJs = [
    `const fs = require("node:fs");`,
    `fs.chmodSync("tamper-target.txt", 0o644);`,
    `fs.writeFileSync("tamper-target.txt", "TAMPERED BY A MALICIOUS FIXTURE");`,
    `console.log("claiming success despite tampering with tracked input");`,
    `process.exit(0);`,
    ``,
  ].join("\n");
  await writeFile(path.join(fixture.repoDir, "verify.js"), tamperingVerifyJs, "utf8");
  await writeFile(path.join(fixture.repoDir, "tamper-target.txt"), "original content\n", "utf8");
  const tamperedBase = await commitAllowedPaths(fixture.repoDir, ["verify.js", "tamper-target.txt"], "inject a tampering verify.js", env);

  const taskId = uniqueId("task-tamper-exit-zero");
  const claimResult = await claim(taskId);
  const outcome = await runGitWorkerTask({
    taskId,
    ownerId: "worker-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: tamperedBase,
    managedWorkspaceRoot: managedRoot,
    pool,
  });
  assert.equal(outcome.kind, "verification_failed", `the tampering fixture exits 0, but the integrity check must still refuse it; got ${outcome.kind}`);
  if (outcome.kind === "verification_failed") {
    assert.equal(outcome.verification.exitCode, 0, "the real process exit code (0) must be reported honestly -- the REJECTION is a distinct, separately-reported fact, not a fabricated nonzero exit code");
    assert.match(outcome.verification.stderr, /integrity check.*refusing to accept/i);
  }

  const [rows] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal(rows[0]!.c, 0, "NO checkpoint row may exist when the final verification input was tampered with, regardless of the raw exit code");
  const [task] = await pool.query<any[]>(`SELECT checkpoint_digest FROM tasks WHERE task_id = ?`, [taskId]);
  assert.equal(task[0]!.checkpoint_digest, null);
});

test("7. an invalid commit ref and an out-of-managed-root path are both rejected safely, not silently corrupted", async () => {
  const fixture = await freshFixture();
  const isolatedHome = path.join(fixtureParentDir, "home-invalid");
  const env = makeIsolatedGitEnv(isolatedHome);

  await assert.rejects(() => resolveCommit(fixture.repoDir, "0000000000000000000000000000000000dead", env), /rev-parse|not a valid|unknown revision/i);

  assert.throws(() => assertRequestedPathsAreSafe(managedRoot, "../escape-attempt", "attempt-1"), /allowlist|escapes/i);
  assert.throws(() => resolveWorktreePath(managedRoot, "task/with/slashes", "attempt-1"), /allowlist/i);
});

test("8. a verification TIMEOUT cleans up its own owned child process (no leaked process)", async () => {
  const fixture = await freshFixture();
  const isolatedHome = path.join(fixtureParentDir, "home-timeout");
  const env = makeIsolatedGitEnv(isolatedHome);
  // Overwrite verify.js with one that hangs forever -- a REAL process
  // that would never exit on its own if not killed.
  await writeFile(path.join(fixture.repoDir, "verify.js"), "setInterval(() => {}, 1000);\n", "utf8");
  await git(["add", "-A"], fixture.repoDir, env);
  await git(["-c", "user.name=x", "-c", "user.email=x@x", "commit", "--no-verify", "-m", "hang forever"], fixture.repoDir, env);

  const result = await runVerification(fixture.repoDir, env, 500); // 500ms bound, well under a hang
  assert.equal(result.outcome, "timeout");
  // If the child were NOT actually killed, this test process would hang
  // on exit (an orphaned `node` with an active setInterval keeps the
  // event loop alive) -- node:test's own process exit at the end of this
  // file is itself evidence the child did not leak, in addition to the
  // explicit outcome check above.
});

test("9. a STALE worker (superseded epoch) cannot publish an authoritative successful checkpoint", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("task-stale-epoch");
  const staleClaimResult = await claim(taskId, "worker-stale");

  // A genuine takeover: a DIFFERENT owner claims after forcing the lease to look expired.
  await pool.query(`UPDATE tasks SET lease_expires_at = NOW(3) - INTERVAL 1 SECOND WHERE task_id = ?`, [taskId]);
  const newClaimResult = await claim(taskId, "worker-new");
  assert.notEqual(newClaimResult.epoch, staleClaimResult.epoch, "the takeover must have incremented the epoch");

  // The STALE worker, still believing it holds `staleClaimResult.epoch`,
  // attempts to do the real Git work and publish -- the Git work itself
  // succeeds (this milestone's honest limitation: a DB check does not
  // fence the filesystem), but the FINAL publish, at the authoritative
  // DB boundary, must be rejected.
  await assert.rejects(
    () =>
      runGitWorkerTask({
        taskId,
        ownerId: "worker-stale",
        epoch: staleClaimResult.epoch,
        sourceRepoDir: fixture.repoDir,
        requestedCommitRef: fixture.baseCommit,
        managedWorkspaceRoot: managedRoot,
        pool,
      }),
    /stale/i,
  );

  const [rows] = await pool.query<any[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal(rows[0]!.c, 0, "the stale worker's checkpoint must NOT have been durably recorded");
});

test("branch naming and checkpoint command-id helpers are deterministic (sanity check backing the reconciliation design)", () => {
  assert.equal(branchNameFor("abc"), "relaylab/abc");
  assert.equal(checkpointCommandId("abc"), "git-checkpoint-abc");
  assert.equal(defaultAttemptId("abc"), defaultAttemptId("abc"));
});

/**
 * Phase 2A.1 section 6 boundary test, real on THIS host (win32): a real
 * NTFS junction (`fs.symlink(..., "junction")` -- junctions, unlike
 * Windows symlinks, do not require elevated/Developer Mode privileges,
 * which is why this is the reparse-point type used here) placed INSIDE
 * the managed root, pointing OUTSIDE it, must be rejected by both
 * `assertNoSymlinkEscapes` and the stricter `assertSafeForDeletion` --
 * not merely by the lexical `assertWithinRoot` check (a path through the
 * junction is lexically inside the root; only a real filesystem check
 * catches it). This test is SKIPPED on any non-Windows host, since this
 * project has not verified the equivalent (a real POSIX symlink) on one
 * -- see docs/ARCHITECTURE.md "Phase 2A worker" for that explicit,
 * untested-platform disclosure.
 */
test("11. a real NTFS junction inside the managed root, pointing outside it, is rejected (not merely a lexical check)", { skip: process.platform !== "win32" ? "junction/reparse-point behavior verified on win32 only" : false }, async () => {
  const outsideDir = await mkdtemp(path.join(tmpdir(), "relaylab-outside-root-"));
  await mkdir(path.join(outsideDir, "payload"), { recursive: true });
  const taskId = "junction-escape-task";
  const junctionPath = path.join(managedRoot, taskId);
  await mkdir(managedRoot, { recursive: true });
  await symlink(outsideDir, junctionPath, "junction");

  try {
    const candidate = path.join(junctionPath, "attempt-1");
    await assert.rejects(
      () => assertNoSymlinkEscapes(managedRoot, candidate),
      /symlink|junction|reparse|OUTSIDE/i,
      "a junction placed inside the managed root, pointing outside it, must be rejected",
    );
    await assert.rejects(
      () => assertSafeForDeletion(managedRoot, junctionPath),
      /symlink|junction|reparse|OUTSIDE|segment/i,
      "deletion through a junction must ALSO be rejected -- deletion is held to at least the same standard as inspection",
    );
  } finally {
    // Remove the junction link itself, never its target: Node's `fs.rm`
    // lstat's the path first and, when the path itself is a symlink/
    // junction, unlinks that entry directly rather than recursing into
    // whatever it points to -- `recursive: true` here is only to permit
    // removing a directory-type entry, not an instruction to follow it.
    await rm(junctionPath, { recursive: true, force: true }).catch(() => {});
    await rm(outsideDir, { recursive: true, force: true }).catch(() => {});
  }
});
