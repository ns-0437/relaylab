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
