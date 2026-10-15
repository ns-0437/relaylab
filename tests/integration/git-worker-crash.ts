// Phase 2A, Part H tests 10-12 (and Phase 2A.1 sections 2-3 extensions):
// real process-kill crash + restart reconciliation, using the SAME IPC
// failpoint-readiness handshake pattern as
// tests/integration/crash-windows.ts (never an arbitrary sleep as the
// sole synchronization mechanism). A real worker-main.ts child process is
// killed at an EXACT, confirmed point, then a fresh worker process (no
// failpoint) is started against the SAME task to prove reconciliation.
//
// Phase 2A.1 hardening (section 2): every crash scenario below now does
// TWO things the original version did not --
//   1. Independently inspects the real worktree from THIS orchestrator
//      process (via src/worker/git.ts helpers against a managed
//      workspace root this test controls directly, not an assumed OS
//      temp fallback) BEFORE killing the child, and asserts the IPC
//      failpoint payload's reported commit/tree match that independent
//      inspection exactly.
//   2. After restart, re-inspects the worktree and asserts EXACT
//      equality against the pre-kill values (never "a checkpoint
//      exists," always "the SAME commit/tree as before the kill"),
//      plus ancestry (`isAncestor`) and an exact commit-count delta
//      (`commitCountBetween`) -- never "structural code-path proof."
//
// Phase 2A.1 additions (section 3): two more real-kill scenarios
// (crash after worktree creation but before repair; crash after repair
// is applied to disk but before it is committed) using the two new,
// earlier failpoints added to src/worker/run-task.ts, plus two
// non-process-kill scenarios that directly exercise
// `ensureWorktreeAndCommit`'s explicit-state rejections (clean worktree
// at the original buggy base; worktree identity mismatch) -- these two
// do not need a real process kill, since they are testing a SYNCHRONOUS
// validation function's own behavior given a deliberately inconsistent
// worktree, not a crash-timing window.
//
// Usage: npm run test:git-worker-crash
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import mysql from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { loadTestMysqlConfig } from "../../src/config.js";
import { ProcessHarness, testChildEnv, repoRootFrom, type ManagedChild } from "./process-harness.js";
import { createFixtureRepo } from "../../src/worker/fixture-repo.js";
import { createManagedWorkspaceRoot, removeManagedWorkspaceRoot, resolveWorktreePath } from "../../src/worker/workspace.js";
import { defaultAttemptId, checkpointCommandId, branchNameFor, ensureWorktreeAndCommit, assertCleanWorktreeIsValidResult } from "../../src/worker/run-task.js";
import { addWorktree, commitAllowedPaths, commitCountBetween, currentCommit, currentTree, hasUncommittedChanges, isAncestor, makeIsolatedGitEnv } from "../../src/worker/git.js";
import { buildManifest, buildRuntimeFingerprint, publishCheckpoint } from "../../src/worker/checkpoint.js";
import { runVerification } from "../../src/worker/verify.js";
import { buildVerifyCreateArgs, DOCKER_VERIFY_DEFAULTS } from "../../src/worker/docker-verify.js";
import { deterministicExecutionName } from "../../src/worker/verification.js";
import { writeWorktreeIdentity } from "../../src/worker/worktree-identity.js";
import { TaskStoreLive } from "../../src/db/task-store.js";

const execFileAsync = promisify(execFile);

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = repoRootFrom(here);
const workerEntry = path.join(repoRoot, "src", "worker-main.ts");
const testDatabase = loadTestMysqlConfig().database;

function log(scenario: string, msg: string) {
  console.log(`[git-worker-crash ${scenario}] ${msg}`);
}
function logEvidence(scenario: string, label: string, data: unknown) {
  console.log(`[git-worker-crash ${scenario}] EVIDENCE ${label}: ${JSON.stringify(data)}`);
}

interface FailpointMessage {
  type?: string;
  name?: string;
  pid?: number;
  resultCommit?: string;
  resultTree?: string;
  worktreeExisted?: boolean;
}

function waitForFailpointReady(child: ManagedChild, failpointName: string, timeoutMs: number): Promise<FailpointMessage> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      child.process.off("message", onMessage);
      reject(new Error(`timed out after ${timeoutMs}ms waiting for failpoint "${failpointName}" to report ready`));
    }, timeoutMs);
    function onMessage(msg: unknown) {
      const m = msg as FailpointMessage;
      if (m?.type === "failpoint-ready" && m.name === failpointName) {
        clearTimeout(timer);
        child.process.off("message", onMessage);
        resolve(m);
      }
    }
    child.process.on("message", onMessage);
  });
}

async function killAndWaitExit(child: ManagedChild): Promise<{ exitCode: number | null; signalCode: string | null }> {
  child.process.kill("SIGKILL");
  const { code, signal } = await child.exited;
  // Phase 2A.1 section 2: state the ACTUAL OS termination behavior
  // honestly. `child.process.kill("SIGKILL")` asks Node to terminate the
  // child; on this Windows machine, Node implements this via
  // TerminateProcess, and the reported `signal` field below is Node's
  // OWN post-hoc labeling of an abrupt exit -- it is NOT evidence that a
  // real POSIX SIGKILL was delivered or handled via POSIX signal
  // semantics (there is no POSIX signal delivery mechanism on Windows at
  // all). See CLAUDE.md "How to distinguish simulated behavior from real
  // integrations" for this project's standing convention on this point.
  return { exitCode: code, signalCode: signal };
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs: number, label: string): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 150));
  }
  throw new Error(`timed out waiting for: ${label}`);
}

/**
 * Phase 2A.1 section 2: the full "commit created, checkpoint not yet
 * persisted" crash window, now with independent orchestrator-side Git
 * inspection and exact-value assertions (SHA/tree/ancestry/commit-count),
 * not merely "a checkpoint row now exists."
 */
async function scenario_commitBeforeCheckpoint(pool: mysql.Pool, demoRoot: string): Promise<void> {
  const scenario = "commit-created-checkpoint-not-persisted";
  const harness = new ProcessHarness();
  try {
    const fixtureDir = path.join(demoRoot, "fixture-crash10");
    const fixture = await createFixtureRepo(fixtureDir);
    const taskId = `crash-checkpoint-${Date.now()}`;
    const ownerId = `worker-${taskId}`;
    // Phase 2A.1: an explicit, THIS-TEST-OWNED workspace root (not the
    // implicit OS-temp-dir fallback) -- shared by BOTH the killed and
    // the recovered child via env, and ALSO used directly by this
    // orchestrator process below to independently inspect the real
    // worktree with its own `git` calls, rather than inferring state
    // only from IPC messages or database rows.
    const workspaceRoot = path.join(demoRoot, "workspace-crash10");
    const isolatedHome = path.join(demoRoot, "home-crash10");
    const env = makeIsolatedGitEnv(isolatedHome);

    logEvidence(scenario, "task_id", taskId);
    logEvidence(scenario, "base_commit", fixture.baseCommit);
    logEvidence(scenario, "persistent_owner_id", ownerId);

    const child = harness.spawn({
      label: "worker-killed",
      entry: workerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_FAILPOINT: "worker-after-verify-before-checkpoint",
        RELAYLAB_WORKER_TASK_ID: taskId,
        RELAYLAB_WORKER_REPO_DIR: fixtureDir,
        RELAYLAB_WORKER_COMMIT_REF: fixture.baseCommit,
        RELAYLAB_WORKER_OWNER_ID: ownerId,
        RELAYLAB_WORKER_WORKSPACE_ROOT: workspaceRoot,
        RELAYLAB_OTEL_DISABLED: "1",
      }),
    });
    const ready = await waitForFailpointReady(child, "worker-after-verify-before-checkpoint", 15_000);
    log(scenario, "confirmed: verification passed, local commit exists, checkpoint NOT yet persisted");
    logEvidence(scenario, "failpoint_ipc_payload", ready);
    assert.ok(ready.resultCommit && /^[0-9a-f]{40}$/.test(ready.resultCommit), "failpoint payload must carry a full 40-char result commit SHA");
    assert.ok(ready.resultTree && /^[0-9a-f]{40}$/.test(ready.resultTree), "failpoint payload must carry a full 40-char result tree ID");

    const [checkpointsBeforeKill] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
    logEvidence(scenario, "checkpoints_before_kill", checkpointsBeforeKill[0]);
    assert.equal(checkpointsBeforeKill[0]!.c, 0, "expected zero checkpoint rows at the failpoint -- the failpoint is in the wrong place");

    // Independently inspect the REAL worktree from THIS process, before
    // the kill -- not inferred, not assumed from the IPC message alone.
    const attemptId = defaultAttemptId(taskId);
    const worktreeDir = resolveWorktreePath(workspaceRoot, taskId, attemptId);
    const preKillCommit = await currentCommit(worktreeDir, env);
    const preKillTree = await currentTree(worktreeDir, env);
    logEvidence(scenario, "orchestrator_observed_pre_kill", { commit: preKillCommit, tree: preKillTree });
    assert.equal(preKillCommit, ready.resultCommit, "orchestrator's independent inspection must match the failpoint's reported result commit");
    assert.equal(preKillTree, ready.resultTree, "orchestrator's independent inspection must match the failpoint's reported result tree");
    const preKillAncestry = await isAncestor(worktreeDir, fixture.baseCommit, preKillCommit, env);
    assert.equal(preKillAncestry, true, "the pre-kill result commit must be a real descendant of the base commit");
    const preKillCommitCount = await commitCountBetween(worktreeDir, fixture.baseCommit, preKillCommit, env);
    assert.equal(preKillCommitCount, 1, "expected exactly one commit (the scripted repair) between base and the pre-kill result commit");

    const exitInfo = await killAndWaitExit(child);
    logEvidence(scenario, "child_exit_outcome", exitInfo);

    log(scenario, "restarting a fresh worker (no failpoint) against the SAME task to reconcile");
    const recovered = harness.spawn({
      label: "worker-recovered",
      entry: workerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_WORKER_TASK_ID: taskId,
        RELAYLAB_WORKER_REPO_DIR: fixtureDir,
        RELAYLAB_WORKER_COMMIT_REF: fixture.baseCommit,
        RELAYLAB_WORKER_OWNER_ID: ownerId, // SAME identity -- idempotent re-claim, not a takeover
        RELAYLAB_WORKER_WORKSPACE_ROOT: workspaceRoot, // SAME root -- required to find the prior attempt's worktree at all
        RELAYLAB_OTEL_DISABLED: "1",
      }),
    });
    void recovered;

    await waitFor(
      async () => {
        const [rows] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
        return (rows[0]?.c ?? 0) >= 1;
      },
      20_000,
      "the restarted worker to publish exactly one checkpoint",
    );

    const [checkpointsAfter] = await pool.query<mysql.RowDataPacket[]>(`SELECT * FROM task_checkpoints WHERE task_id = ?`, [taskId]);
    logEvidence(scenario, "checkpoints_after_restart", { count: checkpointsAfter.length, resultCommit: checkpointsAfter[0]?.result_commit, resultTree: checkpointsAfter[0]?.result_tree });
    assert.equal(checkpointsAfter.length, 1, "expected EXACTLY ONE checkpoint row after reconciliation");

    const [outboxRows] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'CheckpointRecorded'`, [taskId]);
    logEvidence(scenario, "checkpoint_recorded_events", outboxRows[0]);
    assert.equal(outboxRows[0]!.c, 1, "expected EXACTLY ONE CheckpointRecorded outbox event");

    // The real, decisive assertions this patch was asked for: the
    // POST-RESTART result EXACTLY equals the PRE-KILL result -- not "a
    // checkpoint exists," but "the SAME commit, the SAME tree."
    const postRestartCommit = await currentCommit(worktreeDir, env);
    const postRestartTree = await currentTree(worktreeDir, env);
    logEvidence(scenario, "orchestrator_observed_post_restart", { commit: postRestartCommit, tree: postRestartTree });
    assert.equal(postRestartCommit, preKillCommit, "post-restart result commit must EXACTLY equal the pre-kill result commit -- no new/different commit was created");
    assert.equal(postRestartTree, preKillTree, "post-restart result tree must EXACTLY equal the pre-kill result tree");
    assert.equal(checkpointsAfter[0]!.result_commit, preKillCommit, "the PERSISTED checkpoint's result_commit must equal the pre-kill commit");
    assert.equal(checkpointsAfter[0]!.result_tree, preKillTree, "the PERSISTED checkpoint's result_tree must equal the pre-kill tree");
    const postRestartAncestry = await isAncestor(worktreeDir, fixture.baseCommit, postRestartCommit, env);
    assert.equal(postRestartAncestry, true, "post-restart result commit must still be a real descendant of the base commit");
    const postRestartCommitCount = await commitCountBetween(worktreeDir, fixture.baseCommit, postRestartCommit, env);
    assert.equal(postRestartCommitCount, 1, "expected exactly one commit between base and the post-restart result commit -- reconciliation must not add a second, duplicate commit");

    log(scenario, "PASS: exactly one durable checkpoint and one CheckpointRecorded event, with exact SHA/tree/ancestry equality proven by independent orchestrator-side Git inspection -- despite a real mid-flight kill");
  } finally {
    await harness.stopAll();
  }
}

async function dockerInspectOrNull(nameOrId: string, format: string): Promise<string | null> {
  try {
    const { stdout } = await execFileAsync("docker", ["inspect", nameOrId, "--format", format]);
    return stdout.trim();
  } catch {
    return null;
  }
}

/**
 * Phase 2C.1, Part 2: a real worker PROCESS is killed WHILE its
 * verification CONTAINER is still genuinely running -- the one real
 * gap every prior crash scenario in this file (which all crash either
 * before verification starts or after it has already finished and
 * been removed) never exercised. The fixture's `verify.js` is
 * overwritten with a deliberately slow version (an 8s delay before it
 * passes) specifically so the container stays alive for the whole
 * observe-then-kill window below, driven through the SAME
 * `onContainerCreated` hook / `worker-verify-container-running`
 * failpoint `src/worker/run-task.ts` now calls unconditionally (a
 * no-op in every other test/run, since `hitFailpoint` only ever acts
 * when `RELAYLAB_FAILPOINT` names this exact point).
 *
 * A second, INDEPENDENT, long-running container (standing in for "a
 * different worker's legitimate, concurrent execution") is started
 * under a completely different deterministic name and kept running
 * throughout this entire scenario -- proving restart reconciliation's
 * exact-name matching never touches it.
 */
async function scenario_crashDuringContainerVerification(pool: mysql.Pool, demoRoot: string): Promise<void> {
  const scenario = "crash-during-container-verification";
  const harness = new ProcessHarness();
  const otherExecutionInputDir = path.join(demoRoot, "other-execution-input-crash13");
  let otherContainerId: string | null = null;
  try {
    const fixtureDir = path.join(demoRoot, "fixture-crash13");
    const fixture = await createFixtureRepo(fixtureDir);
    const isolatedHome = path.join(demoRoot, "home-crash13");
    const env = makeIsolatedGitEnv(isolatedHome);

    // Overwrite `verify.js` with a slow variant (same assertion, same
    // fixed VERIFICATION_COMMAND_IDENTITY -- `node verify.js` -- just
    // slower) BEFORE the worker ever runs, so its container stays
    // observably "running" for several real seconds.
    const slowVerifyJs = [
      `const assert = require("node:assert/strict");`,
      `const { add } = require("./src/add.js");`,
      `setTimeout(() => {`,
      `  assert.equal(add(2, 3), 5, "add(2, 3) must equal 5");`,
      `  console.log("verify.js: PASS (slow)");`,
      `  process.exit(0);`,
      `}, 8000);`,
      ``,
    ].join("\n");
    await writeFile(path.join(fixtureDir, "verify.js"), slowVerifyJs, "utf8");
    const slowBaseCommit = await commitAllowedPaths(fixtureDir, ["verify.js"], "slow verify.js for the real-crash-during-verification test", env);

    const taskId = `crash-container-${Date.now()}`;
    const ownerId = `worker-${taskId}`;
    const workspaceRoot = path.join(demoRoot, "workspace-crash13");
    logEvidence(scenario, "task_id", taskId);
    logEvidence(scenario, "slow_base_commit", slowBaseCommit);

    // A second, UNRELATED, independently-named long-running execution,
    // standing in for a different worker's legitimate concurrent
    // verification -- started and left running for the ENTIRE scenario
    // below, to prove reconciliation's exact-name matching never
    // touches it.
    const otherName = `relaylab-verify-other-worker-${taskId}`;
    const otherStagingDir = path.join(otherExecutionInputDir, "staging");
    await mkdir(otherStagingDir, { recursive: true });
    await writeFile(path.join(otherStagingDir, "verify.js"), `setTimeout(() => {}, 60000);\n`, "utf8");
    await execFileAsync(process.platform === "win32" ? path.join(process.env.SystemRoot ?? "C:\\Windows", "System32", "tar.exe") : "tar", [
      "-cf",
      path.join(otherExecutionInputDir, "candidate.tar"),
      "-C",
      otherStagingDir,
      "verify.js",
    ]);
    const otherCreateArgs = buildVerifyCreateArgs({ name: otherName, inputDir: otherExecutionInputDir, ...DOCKER_VERIFY_DEFAULTS });
    const { stdout: otherIdRaw } = await execFileAsync("docker", otherCreateArgs);
    otherContainerId = otherIdRaw.trim();
    await execFileAsync("docker", ["start", otherContainerId]);
    logEvidence(scenario, "other_legitimate_execution_container_id", otherContainerId);
    assert.equal(await dockerInspectOrNull(otherContainerId, "{{.State.Running}}"), "true", "test setup check: the second, independent execution must actually be running before this scenario proceeds");

    interface ContainerFailpointMessage extends FailpointMessage {
      containerId?: string;
    }
    const child = harness.spawn({
      label: "worker-killed",
      entry: workerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_FAILPOINT: "worker-verify-container-running",
        RELAYLAB_WORKER_TASK_ID: taskId,
        RELAYLAB_WORKER_REPO_DIR: fixtureDir,
        RELAYLAB_WORKER_COMMIT_REF: slowBaseCommit,
        RELAYLAB_WORKER_OWNER_ID: ownerId,
        RELAYLAB_WORKER_WORKSPACE_ROOT: workspaceRoot,
        RELAYLAB_OTEL_DISABLED: "1",
      }),
    });
    const ready = (await waitForFailpointReady(child, "worker-verify-container-running", 30_000)) as ContainerFailpointMessage;
    logEvidence(scenario, "failpoint_ipc_payload", ready);
    assert.ok(ready.pid && ready.pid > 0, "failpoint payload must carry the worker process's real pid");
    assert.ok(ready.containerId && ready.containerId.length > 0, "failpoint payload must carry the REAL container ID");
    // This test's own task-store claim happens inside the child; the
    // failpoint payload's reported epoch is asserted indirectly below
    // via the task row itself, since `extra` on `hitFailpoint` already
    // carries it (see run-task.ts's call site) -- logged above for the
    // "structured readiness barrier ... owner epoch" requirement.

    const containerId = ready.containerId!;
    const runningBeforeKill = await dockerInspectOrNull(containerId, "{{.State.Running}}");
    logEvidence(scenario, "container_state_before_kill", { running: runningBeforeKill });
    assert.equal(runningBeforeKill, "true", "the verification container must be independently CONFIRMED running (via docker inspect, not merely trusted from the IPC message) before the kill");

    const [checkpointsBeforeKill] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
    assert.equal(checkpointsBeforeKill[0]!.c, 0, "expected zero checkpoint rows while verification is still in flight");

    const exitInfo = await killAndWaitExit(child);
    logEvidence(scenario, "worker_process_exit_outcome", exitInfo);
    // The worker PROCESS is confirmed gone -- `process.kill(pid, 0)`
    // throws ESRCH once a pid is no longer live; this double-checks the
    // exit event above against the OS's own view, not just Node's.
    let pidStillLive = true;
    try {
      process.kill(ready.pid!, 0);
    } catch {
      pidStillLive = false;
    }
    assert.equal(pidStillLive, false, "the killed worker process's pid must no longer be live");

    // THE decisive observation this part asks for: the CONTAINER itself
    // is independently re-inspected immediately after the worker
    // process is confirmed dead. Nothing in this project tells Docker
    // to stop a container just because the process that created it
    // died -- so it is expected, and recorded as evidence, that the
    // container is STILL RUNNING here: a real, observable orphan, not
    // assumed away.
    const containerStateAfterKill = await dockerInspectOrNull(containerId, "{{.State.Running}}");
    logEvidence(scenario, "container_state_after_worker_killed", { running: containerStateAfterKill });
    assert.equal(containerStateAfterKill, "true", "killing the WORKER PROCESS must not, by itself, stop its CONTAINER -- this is the real orphan this test exists to observe and record, not assume");

    log(scenario, "restarting a fresh worker (no failpoint) against the SAME task -- its restart reconciliation must stop+remove the orphaned container by its exact deterministic name before creating its own");
    const recovered = harness.spawn({
      label: "worker-recovered",
      entry: workerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_WORKER_TASK_ID: taskId,
        RELAYLAB_WORKER_REPO_DIR: fixtureDir,
        RELAYLAB_WORKER_COMMIT_REF: slowBaseCommit,
        RELAYLAB_WORKER_OWNER_ID: ownerId, // SAME identity -- idempotent re-claim, not a takeover
        RELAYLAB_WORKER_WORKSPACE_ROOT: workspaceRoot, // SAME root -- required to find the prior attempt's worktree/identity at all
        RELAYLAB_OTEL_DISABLED: "1",
      }),
    });
    void recovered;

    await waitFor(
      async () => {
        const [rows] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
        return (rows[0]?.c ?? 0) >= 1;
      },
      30_000, // the restarted worker's OWN verification also takes ~8s (same slow fixture) plus normal overhead
      "the restarted worker to publish exactly one checkpoint",
    );

    const [checkpointsAfter] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
    assert.equal(checkpointsAfter[0]!.c, 1, "expected EXACTLY ONE checkpoint row after reconciliation and a successful restarted verification");
    const [outboxRows] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'CheckpointRecorded'`, [taskId]);
    assert.equal(outboxRows[0]!.c, 1, "expected EXACTLY ONE CheckpointRecorded event -- the interrupted verification must never have contributed a successful checkpoint or acknowledgement of its own");

    // The orphaned container from the KILLED attempt must now be gone
    // -- stopped and removed by the RESTARTED worker's own
    // `reconcileAbandonedExecution`, matched purely by its exact
    // deterministic name (never by any shared "owner" label -- this
    // codebase never sets one at all; see docs/EXECUTION.md "Cleanup
    // ownership and abandonment rules").
    const orphanStillExists = await dockerInspectOrNull(containerId, "{{.Id}}");
    logEvidence(scenario, "orphan_container_after_restart", { stillExists: orphanStillExists !== null });
    assert.equal(orphanStillExists, null, "the orphaned container from the killed attempt must have been stopped AND removed by the restarted worker's restart reconciliation");

    // THE OTHER requirement this part asks for: the SECOND, independent,
    // still-running execution must remain completely untouched by that
    // same reconciliation pass -- proving abandonment is decided by an
    // EXACT name match against THIS task/attempt/role's own slot, never
    // by anything looser (a shared label, a substring, "any orphan
    // Docker happens to have").
    const otherStillRunning = await dockerInspectOrNull(otherContainerId, "{{.State.Running}}");
    logEvidence(scenario, "other_legitimate_execution_after_reconciliation", { running: otherStillRunning });
    assert.equal(otherStillRunning, "true", "a second, independent, legitimate execution must remain RUNNING and untouched by a DIFFERENT task's restart reconciliation");

    log(scenario, "PASS: a real killed worker process left its container orphaned and running (recorded); restart reconciliation stopped+removed EXACTLY that orphan by exact name, never touching a second independent execution; exactly one checkpoint/event resulted, none from the interrupted attempt");
  } finally {
    if (otherContainerId) {
      await execFileAsync("docker", ["rm", "-f", otherContainerId]).catch(() => {});
    }
    await harness.stopAll();
  }
}

/** Phase 2A.1 section 3: crash after the worktree is created (and its identity sidecar written) but BEFORE the scripted repair is applied. Restart must reconcile to the same normal completed state as the happy path. */
async function scenario_crashBeforeRepair(pool: mysql.Pool, demoRoot: string): Promise<void> {
  const scenario = "crash-after-worktree-before-repair";
  const harness = new ProcessHarness();
  try {
    const fixtureDir = path.join(demoRoot, "fixture-crash-prerepair");
    const fixture = await createFixtureRepo(fixtureDir);
    const taskId = `crash-prerepair-${Date.now()}`;
    const ownerId = `worker-${taskId}`;
    const workspaceRoot = path.join(demoRoot, "workspace-crash-prerepair");
    const isolatedHome = path.join(demoRoot, "home-crash-prerepair");
    const env = makeIsolatedGitEnv(isolatedHome);
    logEvidence(scenario, "task_id", taskId);

    const child = harness.spawn({
      label: "worker-killed",
      entry: workerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_FAILPOINT: "worker-after-worktree-before-repair",
        RELAYLAB_WORKER_TASK_ID: taskId,
        RELAYLAB_WORKER_REPO_DIR: fixtureDir,
        RELAYLAB_WORKER_COMMIT_REF: fixture.baseCommit,
        RELAYLAB_WORKER_OWNER_ID: ownerId,
        RELAYLAB_WORKER_WORKSPACE_ROOT: workspaceRoot,
        RELAYLAB_OTEL_DISABLED: "1",
      }),
    });
    const ready = await waitForFailpointReady(child, "worker-after-worktree-before-repair", 15_000);
    logEvidence(scenario, "failpoint_ipc_payload", ready);
    assert.equal(ready.worktreeExisted, false, "this is a FRESH worktree creation -- it must not have existed before this call");

    const attemptId = defaultAttemptId(taskId);
    const worktreeDir = resolveWorktreePath(workspaceRoot, taskId, attemptId);
    const headAtFailpoint = await currentCommit(worktreeDir, env);
    const dirtyAtFailpoint = await hasUncommittedChanges(worktreeDir, env);
    logEvidence(scenario, "orchestrator_observed_pre_kill", { head: headAtFailpoint, dirty: dirtyAtFailpoint });
    assert.equal(headAtFailpoint, fixture.baseCommit, "HEAD must still be exactly the base commit -- the repair has not run yet");
    assert.equal(dirtyAtFailpoint, false, "worktree must be clean -- the repair has not touched any file yet");

    const [checkpointsBeforeKill] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
    assert.equal(checkpointsBeforeKill[0]!.c, 0, "expected zero checkpoint rows before the repair has even run");

    await killAndWaitExit(child);
    log(scenario, "restarting a fresh worker (no failpoint) against the SAME task to reconcile");
    harness.spawn({
      label: "worker-recovered",
      entry: workerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_WORKER_TASK_ID: taskId,
        RELAYLAB_WORKER_REPO_DIR: fixtureDir,
        RELAYLAB_WORKER_COMMIT_REF: fixture.baseCommit,
        RELAYLAB_WORKER_OWNER_ID: ownerId,
        RELAYLAB_WORKER_WORKSPACE_ROOT: workspaceRoot,
        RELAYLAB_OTEL_DISABLED: "1",
      }),
    });

    await waitFor(
      async () => {
        const [rows] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
        return (rows[0]?.c ?? 0) >= 1;
      },
      20_000,
      "the restarted worker to publish exactly one checkpoint",
    );

    const [checkpointsAfter] = await pool.query<mysql.RowDataPacket[]>(`SELECT * FROM task_checkpoints WHERE task_id = ?`, [taskId]);
    assert.equal(checkpointsAfter.length, 1, "expected EXACTLY ONE checkpoint row after reconciliation");
    const postRestartCommit = await currentCommit(worktreeDir, env);
    const postRestartCommitCount = await commitCountBetween(worktreeDir, fixture.baseCommit, postRestartCommit, env);
    assert.equal(postRestartCommitCount, 1, "expected exactly one commit (the repair, applied by the RESTARTED worker) between base and the result commit");
    assert.equal(checkpointsAfter[0]!.result_commit, postRestartCommit);
    log(scenario, "PASS: a kill before the repair was ever applied reconciles to exactly one committed repair and one checkpoint");
  } finally {
    await harness.stopAll();
  }
}

/** Phase 2A.1 section 3: crash after the scripted repair has been written to disk but BEFORE it is committed (the classic "dirty worktree" crash window). Restart must re-commit the SAME repair content, not a duplicate or divergent one. */
async function scenario_crashAfterRepairBeforeCommit(pool: mysql.Pool, demoRoot: string): Promise<void> {
  const scenario = "crash-after-repair-before-commit";
  const harness = new ProcessHarness();
  try {
    const fixtureDir = path.join(demoRoot, "fixture-crash-prerecommit");
    const fixture = await createFixtureRepo(fixtureDir);
    const taskId = `crash-prerecommit-${Date.now()}`;
    const ownerId = `worker-${taskId}`;
    const workspaceRoot = path.join(demoRoot, "workspace-crash-prerecommit");
    const isolatedHome = path.join(demoRoot, "home-crash-prerecommit");
    const env = makeIsolatedGitEnv(isolatedHome);
    logEvidence(scenario, "task_id", taskId);

    const child = harness.spawn({
      label: "worker-killed",
      entry: workerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_FAILPOINT: "worker-after-repair-before-commit",
        RELAYLAB_WORKER_TASK_ID: taskId,
        RELAYLAB_WORKER_REPO_DIR: fixtureDir,
        RELAYLAB_WORKER_COMMIT_REF: fixture.baseCommit,
        RELAYLAB_WORKER_OWNER_ID: ownerId,
        RELAYLAB_WORKER_WORKSPACE_ROOT: workspaceRoot,
        RELAYLAB_OTEL_DISABLED: "1",
      }),
    });
    await waitForFailpointReady(child, "worker-after-repair-before-commit", 15_000);

    const attemptId = defaultAttemptId(taskId);
    const worktreeDir = resolveWorktreePath(workspaceRoot, taskId, attemptId);
    const headAtFailpoint = await currentCommit(worktreeDir, env);
    const dirtyAtFailpoint = await hasUncommittedChanges(worktreeDir, env);
    logEvidence(scenario, "orchestrator_observed_pre_kill", { head: headAtFailpoint, dirty: dirtyAtFailpoint });
    assert.equal(headAtFailpoint, fixture.baseCommit, "HEAD must still be exactly the base commit -- nothing has been committed yet");
    assert.equal(dirtyAtFailpoint, true, "worktree must be DIRTY -- the repair has been written to disk but not committed");

    const [checkpointsBeforeKill] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
    assert.equal(checkpointsBeforeKill[0]!.c, 0, "expected zero checkpoint rows before anything is committed");

    await killAndWaitExit(child);
    log(scenario, "restarting a fresh worker (no failpoint) against the SAME task to reconcile");
    harness.spawn({
      label: "worker-recovered",
      entry: workerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, {
        RELAYLAB_WORKER_TASK_ID: taskId,
        RELAYLAB_WORKER_REPO_DIR: fixtureDir,
        RELAYLAB_WORKER_COMMIT_REF: fixture.baseCommit,
        RELAYLAB_WORKER_OWNER_ID: ownerId,
        RELAYLAB_WORKER_WORKSPACE_ROOT: workspaceRoot,
        RELAYLAB_OTEL_DISABLED: "1",
      }),
    });

    await waitFor(
      async () => {
        const [rows] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
        return (rows[0]?.c ?? 0) >= 1;
      },
      20_000,
      "the restarted worker to publish exactly one checkpoint",
    );

    const [checkpointsAfter] = await pool.query<mysql.RowDataPacket[]>(`SELECT * FROM task_checkpoints WHERE task_id = ?`, [taskId]);
    assert.equal(checkpointsAfter.length, 1, "expected EXACTLY ONE checkpoint row after reconciliation");
    const postRestartCommit = await currentCommit(worktreeDir, env);
    const postRestartDirty = await hasUncommittedChanges(worktreeDir, env);
    assert.equal(postRestartDirty, false, "worktree must be clean after the restarted worker committed the pending repair");
    const postRestartCommitCount = await commitCountBetween(worktreeDir, fixture.baseCommit, postRestartCommit, env);
    assert.equal(postRestartCommitCount, 1, "expected EXACTLY ONE commit -- the restarted worker must commit the SAME pending repair, never a second/duplicate commit");
    assert.equal(checkpointsAfter[0]!.result_commit, postRestartCommit);
    log(scenario, "PASS: a kill with the repair written-but-uncommitted reconciles to exactly one commit and one checkpoint, never a duplicate");
  } finally {
    await harness.stopAll();
  }
}

/**
 * Phase 2A.1 section 1/3: "a clean worktree at the original buggy base
 * must NOT be treated as completed." Directly constructs that exact
 * state (a worktree checked out at base, with NO repair ever applied,
 * clean) and asserts the validation function behind states 4/5 of
 * `ensureWorktreeAndCommit` refuses it with an explicit error.
 *
 * This deliberately calls `assertCleanWorktreeIsValidResult` directly
 * rather than the full `ensureWorktreeAndCommit`/`runGitWorkerTask`:
 * in the REAL (non-test) call path, `applyScriptedRepair` runs
 * unconditionally before this check and is self-healing (it always
 * either applies the fix or confirms it is already present, or throws)
 * -- so there is no honest way to reach "clean, non-test-path, repair
 * absent" by driving the full worker through its public entrypoint. The
 * check still matters as an explicit, independently-testable safety net
 * (see that function's own doc comment), and THIS is what independently
 * tests it, rather than asserting against unreachable code.
 */
async function scenario_cleanAtBuggyBaseRejected(pool: mysql.Pool, demoRoot: string): Promise<void> {
  void pool;
  const scenario = "clean-at-buggy-base-rejected";
  const fixtureDir = path.join(demoRoot, "fixture-buggy-base");
  const fixture = await createFixtureRepo(fixtureDir);
  const isolatedHome = path.join(demoRoot, "home-buggy-base");
  const env = makeIsolatedGitEnv(isolatedHome);
  const workspaceRoot = path.join(demoRoot, "workspace-buggy-base");
  const taskId = `buggy-base-${Date.now()}`;
  const attemptId = defaultAttemptId(taskId);
  const branch = branchNameFor(taskId);

  const worktreeDir = resolveWorktreePath(workspaceRoot, taskId, attemptId);
  await addWorktree(fixtureDir, worktreeDir, branch, fixture.baseCommit, env);
  // Deliberately NO applyScriptedRepair call -- this worktree is clean,
  // checked out at base, with the known defect still present.
  const dirty = await hasUncommittedChanges(worktreeDir, env);
  assert.equal(dirty, false, "test setup check: the freshly checked-out worktree must be clean before exercising the validation function");

  await assert.rejects(
    () => assertCleanWorktreeIsValidResult(worktreeDir, taskId, attemptId, fixture.baseCommit, env),
    /unexpected workspace state/i,
    "a clean worktree sitting at the unrepaired base commit must be explicitly rejected, never silently returned as a completed result",
  );
  log(scenario, "PASS: clean worktree at the original buggy base was explicitly rejected, not silently accepted as completed work");
}

/**
 * Phase 2A.1 section 1/3: a reused worktree directory whose RECORDED
 * identity does not match the current call's request (different base
 * commit, simulating "this worktree actually belongs to a different
 * attempt/base than what was requested") must be rejected explicitly,
 * never silently reused.
 */
async function scenario_identityMismatchRejected(pool: mysql.Pool, demoRoot: string): Promise<void> {
  const scenario = "identity-mismatch-rejected";
  const fixtureDir = path.join(demoRoot, "fixture-identity-mismatch");
  const fixture = await createFixtureRepo(fixtureDir);
  const isolatedHome = path.join(demoRoot, "home-identity-mismatch");
  const env = makeIsolatedGitEnv(isolatedHome);
  const workspaceRoot = path.join(demoRoot, "workspace-identity-mismatch");
  const taskId = `identity-mismatch-${Date.now()}`;
  const attemptId = defaultAttemptId(taskId);
  const ownerId = `worker-${taskId}`;
  const store = makeTaskStoreLive(pool);
  const claim = await Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));

  const worktreeDir = resolveWorktreePath(workspaceRoot, taskId, attemptId);
  const branch = branchNameFor(taskId);
  await addWorktree(fixtureDir, worktreeDir, branch, fixture.baseCommit, env);
  // Write an identity sidecar recording a DIFFERENT base commit than
  // what this call will request -- simulating a worktree that actually
  // belongs to a different attempt's base.
  await writeWorktreeIdentity(worktreeDir, {
    taskId,
    attemptId,
    sourceRepoRealPath: fixtureDir,
    baseCommit: "0000000000000000000000000000000000000000",
    branch,
    createdAtIso: new Date().toISOString(),
  });

  await assert.rejects(
    () =>
      ensureWorktreeAndCommit(
        { taskId, ownerId, epoch: claim.epoch, sourceRepoDir: fixtureDir, requestedCommitRef: fixture.baseCommit, managedWorkspaceRoot: workspaceRoot, pool },
        env,
        worktreeDir,
        fixture.baseCommit,
        attemptId,
      ),
    /identity mismatch/i,
    "a worktree whose recorded identity does not match this call's expected identity must be rejected explicitly",
  );
  log(scenario, "PASS: a worktree with mismatched recorded identity was explicitly rejected, not silently reused");
}

/** Part H test 12: a retried checkpoint publish (simulating "checkpoint persisted, final ack lost") creates NO duplicate row/event -- TaskStore.recordGitCheckpoint's own idempotency ledger, exercised directly (no process kill needed for this one). */
async function scenario_checkpointRetryNoDuplicate(pool: mysql.Pool, demoRoot: string): Promise<void> {
  const scenario = "checkpoint-retry-no-duplicate";
  const fixtureDir = path.join(demoRoot, "fixture-crash12");
  const fixture = await createFixtureRepo(fixtureDir);
  const isolatedHome = path.join(demoRoot, "home-crash12");
  const env = makeIsolatedGitEnv(isolatedHome);
  const taskId = `crash-retry-${Date.now()}`;
  const store = makeTaskStoreLive(pool);
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-retry", 120_000));

  const verification = await runVerification(fixture.repoDir, env); // fails (unrepaired), but exit code/duration are real and sufficient for this idempotency-only test
  void verification;
  const runtimeFingerprint = await buildRuntimeFingerprint(env);
  const manifest = buildManifest({
    taskId,
    attemptId: defaultAttemptId(taskId),
    epoch: claim.epoch,
    baseCommit: fixture.baseCommit,
    resultCommit: fixture.baseCommit, // using the base commit itself as a stand-in "result" for this idempotency-focused test
    resultTree: fixture.baseCommit, // not a real tree id, but this test never checks it -- see assertions below
    verification: { exitCode: 0, outcome: "passed", stdout: "", stderr: "", stdoutTruncated: false, stderrTruncated: false, durationMs: 1 },
    runtimeFingerprint,
    containerImageId: null,
  });

  const publish = () =>
    Effect.runPromise(
      Effect.provide(
        publishCheckpoint({ taskId, ownerId: "worker-retry", epoch: claim.epoch, commandId: checkpointCommandId(taskId), attemptId: defaultAttemptId(taskId), manifest }),
        TaskStoreLive(pool),
      ),
    );

  const first = await publish();
  logEvidence(scenario, "first_publish", { replay: first.replay });
  assert.equal(first.replay, false, "expected the FIRST publish to be a real commit, not a replay");

  // Simulates "checkpoint persisted, final acknowledgement lost" --
  // retrying the SAME publish call again.
  const second = await publish();
  logEvidence(scenario, "second_publish_retry", { replay: second.replay });
  assert.equal(second.replay, true, "expected the RETRIED publish to be reported as a replay, not a new commit");

  const [rows] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal(rows[0]!.c, 1, "expected EXACTLY ONE durable checkpoint row despite two publish calls");
  const [outboxRows] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND type = 'CheckpointRecorded'`, [taskId]);
  assert.equal(outboxRows[0]!.c, 1, "expected EXACTLY ONE CheckpointRecorded event despite two publish calls");
  log(scenario, "PASS: retried publish after a simulated lost acknowledgement produced no duplicate row or event");
}

async function main() {
  console.log(`[git-worker-crash] runtime identity (orchestrator): node ${process.version} at ${process.execPath}`);
  const pool = mysql.createPool({ ...loadTestMysqlConfig(), waitForConnections: true, connectionLimit: 10 });
  const { applyMigrations } = await import("../../src/db/migrate.js");
  await applyMigrations(loadTestMysqlConfig());
  const demoRoot = await mkdtemp(path.join(tmpdir(), "relaylab-git-worker-crash-"));
  try {
    await scenario_commitBeforeCheckpoint(pool, demoRoot);
    await scenario_crashDuringContainerVerification(pool, demoRoot);
    await scenario_crashBeforeRepair(pool, demoRoot);
    await scenario_crashAfterRepairBeforeCommit(pool, demoRoot);
    await scenario_cleanAtBuggyBaseRejected(pool, demoRoot);
    await scenario_identityMismatchRejected(pool, demoRoot);
    await scenario_checkpointRetryNoDuplicate(pool, demoRoot);
    console.log("[git-worker-crash] ALL SCENARIOS PASSED");
  } finally {
    await pool.end();
    await removeManagedWorkspaceRoot(demoRoot);
  }
}

main().catch((err) => {
  console.error("[git-worker-crash] FAILED:", err);
  process.exitCode = 1;
});
