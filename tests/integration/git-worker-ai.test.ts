// AI repair milestone: real Git, real MySQL, real Docker verification,
// SIMULATED Anthropic provider responses (a fake MessagesClient --
// labeled explicitly below; this file makes NO network call and spends
// no tokens). This proves the AI repair mode's integration with the
// existing reliability pipeline (worktree/identity, commitAllowedPaths,
// Docker verification, checkpoint publish) exactly as the deterministic
// path's own tests/integration/git-worker.test.ts does for THAT path --
// see README.md "AI repair mode" for what is/isn't live-provider-verified.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type mysql from "mysql2/promise";
import { Effect } from "effect";
import Anthropic from "@anthropic-ai/sdk";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";
import { createFixtureRepo } from "../../src/worker/fixture-repo.js";
import { runAiRepairTask, MAX_AI_ATTEMPTS } from "../../src/worker/run-task-ai.js";
import { createManagedWorkspaceRoot } from "../../src/worker/workspace.js";
import { TARGET_RELATIVE_PATH } from "../../src/worker/repair.js";
import type { MessagesClient, AnthropicAdapterConfig } from "../../src/ai/anthropic-client.js";

let pool: mysql.Pool;
let managedRoot: string;
let fixtureParentDir: string;

const CONFIG: AnthropicAdapterConfig = { model: "claude-opus-5-5", maxOutputTokens: 512, requestTimeoutMs: 10_000 };

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
});

beforeEach(async () => {
  await truncateAll(pool);
  const isolatedRoot = await mkdtemp(path.join(tmpdir(), "relaylab-ai-worker-test-root-"));
  managedRoot = await createManagedWorkspaceRoot(isolatedRoot);
  fixtureParentDir = await mkdtemp(path.join(tmpdir(), "relaylab-ai-worker-test-"));
});

after(async () => {
  await pool.end();
});

async function freshFixture() {
  const dir = path.join(fixtureParentDir, uniqueId("fixture"));
  return createFixtureRepo(dir);
}

async function claim(taskId: string, ownerId = "worker-ai-a") {
  const store = makeTaskStoreLive(pool);
  return Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));
}

function simulatedMessage(text: string): Anthropic.Message {
  return {
    id: "msg_simulated",
    type: "message",
    role: "assistant",
    model: "claude-opus-5-5-simulated",
    content: [{ type: "text", text, citations: null }],
    stop_reason: "end_turn",
    stop_sequence: null,
    usage: { input_tokens: 50, output_tokens: 10, cache_creation_input_tokens: null, cache_read_input_tokens: null, server_tool_use: null, service_tier: null },
    container: null,
    context_management: null,
  } as unknown as Anthropic.Message;
}

/** A fake client, labeled explicitly: every response below is SIMULATED, not a real Anthropic API call. `responses` is consumed in order, one per attempt; the last entry repeats if more attempts occur. */
function fakeClientWithResponses(responses: string[]): MessagesClient {
  let i = 0;
  return {
    messages: {
      create: (async () => {
        const text = responses[Math.min(i, responses.length - 1)]!;
        i++;
        return simulatedMessage(text);
      }) as unknown,
    } as unknown as Anthropic.Messages,
  } as MessagesClient;
}

test("AI repair (SIMULATED provider, real Docker/Git): a valid generated edit is committed, verified, and checkpointed", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("ai-task-valid");
  const claimResult = await claim(taskId);
  const correctFix = "function add(a, b) {\n  return a + b;\n}\n\nmodule.exports = { add };\n";
  const client = fakeClientWithResponses([JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: correctFix }] })]);

  const { outcome, attempts } = await runAiRepairTask({
    taskId,
    ownerId: "worker-ai-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    aiClient: client,
    aiConfig: CONFIG,
  });

  assert.equal(outcome.kind, "checkpointed", `expected checkpointed, got ${outcome.kind}${"verification" in outcome ? ` (verification: ${JSON.stringify(outcome.verification)})` : ""}`);
  assert.equal(attempts.length, 1);
  assert.equal(attempts[0]!.outcome, "edit_applied");
  assert.equal(attempts[0]!.modelUsed, "claude-opus-5-5-simulated");
  if (outcome.kind === "checkpointed") {
    assert.notEqual(outcome.manifest.resultCommit, fixture.baseCommit);
  }
});

test("AI repair (SIMULATED provider, real Docker/Git): attempt exhaustion -- every attempt rejected (forbidden path), never checkpointed", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("ai-task-exhausted");
  const claimResult = await claim(taskId);
  // Every simulated response targets a FORBIDDEN path -- rejected before any commit, on every attempt.
  const client = fakeClientWithResponses([JSON.stringify({ edits: [{ path: "verify.js", content: "tampered" }] })]);

  const { outcome, attempts } = await runAiRepairTask({
    taskId,
    ownerId: "worker-ai-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    aiClient: client,
    aiConfig: CONFIG,
  });

  assert.equal(outcome.kind, "verification_failed");
  assert.equal(attempts.length, MAX_AI_ATTEMPTS, "must have made exactly the bounded maximum number of attempts, no more");
  for (const a of attempts) assert.equal(a.outcome, "rejected");

  const [checkpointRows] = await pool.query(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal((checkpointRows as { c: number }[])[0]!.c, 0, "attempt exhaustion must never produce a checkpoint row");
});

test("AI repair (SIMULATED provider, real Docker/Git): a syntactically valid but WRONG edit fails real verification on both bounded attempts, never checkpointed", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("ai-task-wrong-fix");
  const claimResult = await claim(taskId);
  // Syntactically valid, allowlisted-path edit, and GENUINELY DIFFERENT
  // from the fixture's original buggy content (multiply, not the
  // original subtract) so there is a real diff to commit -- but it
  // still does not satisfy verify.js's assert.equal(add(2, 3), 5).
  const stillWrong = "function add(a, b) {\n  return a * b;\n}\n\nmodule.exports = { add };\n";
  const client = fakeClientWithResponses([JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: stillWrong }] })]);

  const { outcome, attempts } = await runAiRepairTask({
    taskId,
    ownerId: "worker-ai-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    aiClient: client,
    aiConfig: CONFIG,
  });

  assert.equal(outcome.kind, "verification_failed");
  assert.equal(attempts.length, MAX_AI_ATTEMPTS);
  for (const a of attempts) assert.equal(a.outcome, "edit_applied", "the edit itself was syntactically valid and allowlisted -- it is real VERIFICATION that failed, not parsing");

  const [checkpointRows] = await pool.query(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal((checkpointRows as { c: number }[])[0]!.c, 0, "a genuinely failing verification must never produce a checkpoint row, even after real Docker re-ran it twice");
});

test("AI repair (SIMULATED provider, real Docker/Git): a retry receives bounded diagnostics from the prior failed attempt, and a corrected second attempt succeeds", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("ai-task-retry-fixes");
  const claimResult = await claim(taskId);
  const stillWrong = "function add(a, b) {\n  return a * b;\n}\n\nmodule.exports = { add };\n";
  const correctFix = "function add(a, b) {\n  return a + b;\n}\n\nmodule.exports = { add };\n";
  const client = fakeClientWithResponses([
    JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: stillWrong }] }),
    JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: correctFix }] }),
  ]);

  const { outcome, attempts } = await runAiRepairTask({
    taskId,
    ownerId: "worker-ai-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    aiClient: client,
    aiConfig: CONFIG,
  });

  assert.equal(outcome.kind, "checkpointed", `expected the corrected second attempt to succeed, got ${outcome.kind}`);
  assert.equal(attempts.length, 2);
  assert.equal(attempts[0]!.outcome, "edit_applied");
  assert.equal(attempts[1]!.outcome, "edit_applied");
});

test("AI repair (SIMULATED provider, real Docker/Git): identical (buggy) output consumes an attempt, is never committed, and a valid correction on the next attempt still succeeds", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("ai-task-no-change-then-fix");
  const claimResult = await claim(taskId);
  const buggyContent = "function add(a, b) {\n  return a - b;\n}\n\nmodule.exports = { add };\n";
  const correctFix = "function add(a, b) {\n  return a + b;\n}\n\nmodule.exports = { add };\n";
  const client = fakeClientWithResponses([
    JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: buggyContent }] }),
    JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: correctFix }] }),
  ]);

  const { outcome, attempts } = await runAiRepairTask({
    taskId,
    ownerId: "worker-ai-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    aiClient: client,
    aiConfig: CONFIG,
  });

  assert.equal(outcome.kind, "checkpointed", `expected the second, corrected attempt to succeed, got ${outcome.kind}`);
  assert.equal(attempts.length, 2);
  assert.equal(attempts[0]!.outcome, "no_change", "returning the unmodified buggy content must be its own explicit outcome, never edit_applied");
  assert.equal(attempts[1]!.outcome, "edit_applied");
});

test("AI repair (SIMULATED provider, real Docker/Git): identical output on EVERY attempt exhausts the bound, never checkpointed, never an empty commit", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("ai-task-no-change-exhausted");
  const claimResult = await claim(taskId);
  const buggyContent = "function add(a, b) {\n  return a - b;\n}\n\nmodule.exports = { add };\n";
  const client = fakeClientWithResponses([JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: buggyContent }] })]);

  const { outcome, attempts } = await runAiRepairTask({
    taskId,
    ownerId: "worker-ai-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    aiClient: client,
    aiConfig: CONFIG,
  });

  assert.equal(outcome.kind, "verification_failed");
  assert.equal(attempts.length, MAX_AI_ATTEMPTS);
  for (const a of attempts) assert.equal(a.outcome, "no_change");

  const [checkpointRows] = await pool.query(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal((checkpointRows as { c: number }[])[0]!.c, 0, "exhaustion via no_change must never produce a checkpoint, and must never have created an empty commit along the way");
});

test("AI repair (SIMULATED provider, real Docker/Git): resuming a committed-but-unverified prior attempt re-verifies without re-generating or double-committing", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("ai-task-resume-committed");
  const claimResult = await claim(taskId);
  const correctFix = "function add(a, b) {\n  return a + b;\n}\n\nmodule.exports = { add };\n";

  // Simulate "crashed after commit, before checkpoint was ever
  // published" DIRECTLY, using the SAME primitives run-task-ai.ts
  // itself uses -- never calling runAiRepairTask first, so there is no
  // PRIOR checkpoint publish to collide with (re-verifying a commit
  // that has ALREADY been checkpointed would legitimately produce a
  // different manifest digest each time, since duration/container-id
  // fields are not deterministic across two real Docker runs -- that
  // is a separate, correct behavior, not what this test exercises).
  const { defaultAttemptId, branchNameFor } = await import("../../src/worker/run-task.js");
  const { resolveWorktreePath, resolveIsolatedHomeDir, ensureDir } = await import("../../src/worker/workspace.js");
  const { addWorktree, commitAllowedPaths, makeIsolatedGitEnv } = await import("../../src/worker/git.js");
  const { writeWorktreeIdentity } = await import("../../src/worker/worktree-identity.js");
  const { realpath } = await import("node:fs/promises");
  const attemptId = defaultAttemptId(taskId);
  const worktreeDir = resolveWorktreePath(managedRoot, taskId, attemptId);
  const isolatedHome = resolveIsolatedHomeDir(managedRoot);
  await ensureDir(isolatedHome);
  const env = makeIsolatedGitEnv(isolatedHome);
  const branch = branchNameFor(taskId);
  await ensureDir(path.dirname(worktreeDir));
  await addWorktree(fixture.repoDir, worktreeDir, branch, fixture.baseCommit, env);
  await writeWorktreeIdentity(worktreeDir, {
    taskId,
    attemptId,
    sourceRepoRealPath: await realpath(fixture.repoDir),
    baseCommit: fixture.baseCommit,
    branch,
    createdAtIso: new Date().toISOString(),
  });
  const { writeFile } = await import("node:fs/promises");
  await writeFile(path.join(worktreeDir, TARGET_RELATIVE_PATH), correctFix, "utf8");
  await commitAllowedPaths(worktreeDir, [TARGET_RELATIVE_PATH], "pre-committed, never checkpointed (simulated crash)", env);

  // A client that would reject if it were ever called at all -- proves
  // the resumed run skips generation entirely and verifies/publishes
  // the ALREADY-committed result directly.
  const client = fakeClientWithResponses([JSON.stringify({ edits: [{ path: "verify.js", content: "must never be used" }] })]);
  const { outcome, attempts } = await runAiRepairTask({
    taskId,
    ownerId: "worker-ai-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    aiClient: client,
    aiConfig: CONFIG,
  });

  assert.equal(outcome.kind, "checkpointed", `expected the pre-committed result to verify and checkpoint directly, got ${outcome.kind}`);
  assert.equal(attempts.length, 0, "resuming an already-committed result must not consume a generation attempt at all");

  const [checkpointRows] = await pool.query(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  assert.equal((checkpointRows as { c: number }[])[0]!.c, 1, "exactly one checkpoint, published for the first time by the resumed run");
});

test("AI repair (SIMULATED provider, real Docker/Git): an unexpected workspace modification is refused, never discarded, and never silently reconciled", async () => {
  const fixture = await freshFixture();
  const taskId = uniqueId("ai-task-unexpected-workspace");
  const claimResult = await claim(taskId);
  const correctFix = "function add(a, b) {\n  return a + b;\n}\n\nmodule.exports = { add };\n";

  const client1 = fakeClientWithResponses([JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: correctFix }] })]);
  const first = await runAiRepairTask({
    taskId,
    ownerId: "worker-ai-a",
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    aiClient: client1,
    aiConfig: CONFIG,
  });
  assert.equal(first.outcome.kind, "checkpointed");

  // Simulate something unrelated touching the worktree between runs --
  // e.g. a stray process, not another AI attempt.
  const { defaultAttemptId } = await import("../../src/worker/run-task.js");
  const { resolveWorktreePath } = await import("../../src/worker/workspace.js");
  const { writeFile } = await import("node:fs/promises");
  const worktreeDir = resolveWorktreePath(managedRoot, taskId, defaultAttemptId(taskId));
  await writeFile(path.join(worktreeDir, "an-unexpected-file.txt"), "nobody authorized this", "utf8");

  const client2 = fakeClientWithResponses([JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: correctFix }] })]);
  await assert.rejects(
    () =>
      runAiRepairTask({
        taskId,
        ownerId: "worker-ai-a",
        epoch: claimResult.epoch,
        sourceRepoDir: fixture.repoDir,
        requestedCommitRef: fixture.baseCommit,
        managedWorkspaceRoot: managedRoot,
        pool,
        aiClient: client2,
        aiConfig: CONFIG,
      }),
    /unexpected workspace state/,
  );

  // The unauthorized file must still be there -- refused, not discarded.
  const { readFile } = await import("node:fs/promises");
  const stillThere = await readFile(path.join(worktreeDir, "an-unexpected-file.txt"), "utf8");
  assert.equal(stillThere, "nobody authorized this");
});

test("AI repair -> handoff (SIMULATED provider, real Docker/Git, real handoff APIs): AI edit -> checkpoint -> artifact -> ownership transfer -> worker B restore/re-verify -> acknowledgement", async () => {
  const { defaultAttemptId, branchNameFor } = await import("../../src/worker/run-task.js");
  const { resolveWorktreePath, resolveIsolatedHomeDir, createManagedWorkspaceRoot: createRoot } = await import("../../src/worker/workspace.js");
  const { makeIsolatedGitEnv } = await import("../../src/worker/git.js");
  const { prepareHandoffArtifact, requestOwnershipTransfer, restoreAndAcknowledge, handoffCommandId } = await import("../../src/worker/handoff.js");
  const { makeTaskStoreLive: makeStore } = await import("../../src/db/task-store.js");

  const fixture = await freshFixture();
  const taskId = uniqueId("ai-task-handoff");
  const ownerA = "worker-ai-handoff-a";
  const claimResult = await claim(taskId, ownerA);
  const correctFix = "function add(a, b) {\n  return a + b;\n}\n\nmodule.exports = { add };\n";
  const client = fakeClientWithResponses([JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: correctFix }] })]);

  // Step 1: AI edit -> real Docker verification -> durable checkpoint,
  // via the SAME AI repair path the other tests in this file exercise.
  const outcomeA = await runAiRepairTask({
    taskId,
    ownerId: ownerA,
    epoch: claimResult.epoch,
    sourceRepoDir: fixture.repoDir,
    requestedCommitRef: fixture.baseCommit,
    managedWorkspaceRoot: managedRoot,
    pool,
    aiClient: client,
    aiConfig: CONFIG,
  });
  assert.equal(outcomeA.outcome.kind, "checkpointed");
  if (outcomeA.outcome.kind !== "checkpointed") return;

  const store = makeStore(pool);
  const taskAfterA = await Effect.runPromise(store.getTask(taskId));

  // Step 2-3: build the SAME self-contained artifact and request the
  // SAME atomic ownership transfer the deterministic handoff demo uses
  // -- these APIs are entirely generic over how the checkpoint was
  // produced; no AI-specific handoff logic was written or needed.
  const attemptId = defaultAttemptId(taskId);
  const worktreeDirA = resolveWorktreePath(managedRoot, taskId, attemptId);
  const envA = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRoot));
  const handoffId = `ai-handoff-${taskId}`;
  const artifactOutDir = await mkdtemp(path.join(tmpdir(), "relaylab-ai-handoff-artifacts-"));
  const { artifactPath, manifestPath, manifest } = await prepareHandoffArtifact({
    taskId,
    handoffId,
    worktreeDir: worktreeDirA,
    env: envA,
    resultRef: branchNameFor(taskId),
    sourceCheckpointCommandId: `git-checkpoint-${taskId}`,
    sourceManifestDigest: taskAfterA.checkpointDigest!,
    baseCommit: outcomeA.outcome.manifest.baseCommit,
    resultCommit: outcomeA.outcome.manifest.resultCommit,
    resultTree: outcomeA.outcome.manifest.resultTree,
    outDir: artifactOutDir,
  });

  const ownerB = "worker-ai-handoff-b";
  const transfer = await requestOwnershipTransfer(pool, {
    taskId,
    handoffId: handoffCommandId(taskId, handoffId),
    fromOwnerId: ownerA,
    fromEpoch: taskAfterA.epoch,
    toOwnerId: ownerB,
    checkpointDigest: taskAfterA.checkpointDigest!,
    artifactDigest: manifest.artifactDigest,
    leaseMs: 120_000,
  });

  // Step 4-6: worker B restores into its OWN independent repository,
  // re-verifies in its OWN separate Docker container, and acknowledges.
  const workspaceBRoot = await mkdtemp(path.join(tmpdir(), "relaylab-ai-handoff-workspace-b-"));
  const destRepoDir = path.join(workspaceBRoot, "restored-repo");
  const managedRootB = await createRoot(path.join(workspaceBRoot, "home"));
  const envB = makeIsolatedGitEnv(resolveIsolatedHomeDir(managedRootB));
  const restoreOutcome = await restoreAndAcknowledge(pool, {
    taskId,
    handoffId,
    ownerId: ownerB,
    epoch: transfer.toEpoch,
    artifactPath,
    manifestPath,
    destRepoDir,
    env: envB,
  });

  assert.equal(restoreOutcome.kind, "restored", `expected worker B to restore successfully, got ${restoreOutcome.kind}`);
  if (restoreOutcome.kind !== "restored") return;

  // Identity assertions: the AI-generated result and B's independently
  // restored/re-verified result must be the SAME commit/tree, verified
  // by two DISTINCT container executions.
  assert.equal(restoreOutcome.restoredCommit, outcomeA.outcome.manifest.resultCommit, "A's AI-generated resultCommit and B's restoredCommit must be IDENTICAL");
  assert.equal(restoreOutcome.restoredTree, outcomeA.outcome.manifest.resultTree, "A's resultTree and B's restoredTree must be IDENTICAL");
  assert.notEqual(restoreOutcome.verification.executionId, outcomeA.outcome.verification.executionId, "A's and B's verifications must be DISTINCT real container executions");
  assert.equal(restoreOutcome.receipt.replay, false, "the restore acknowledgement must be a fresh record, not a replay");
});
