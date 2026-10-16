// AI repair milestone: focused unit tests for parsing/validation and
// single-attempt orchestration, using a FAKE Anthropic client -- no
// network, no real credentials, no real tokens spent. Every simulated
// provider response below is labeled as such; none of these are live
// provider evidence (see tests/integration/git-worker-ai.test.ts and
// README.md "AI repair mode" for what IS/ISN'T live-verified).
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { parseAndValidateEdits, attemptAiRepair } from "../../src/worker/ai-repair.js";
import { TARGET_RELATIVE_PATH } from "../../src/worker/repair.js";
import { requestStructuredEdit, AnthropicAdapterError, type MessagesClient, type AnthropicAdapterConfig } from "../../src/ai/anthropic-client.js";
import Anthropic from "@anthropic-ai/sdk";

const CONFIG: AnthropicAdapterConfig = { model: "claude-opus-5-5", maxOutputTokens: 512, requestTimeoutMs: 5_000 };
const BUGGY_CONTENT = "function add(a, b) {\n  return a - b;\n}\n\nmodule.exports = { add };\n";
const FIXED_CONTENT = "function add(a, b) {\n  return a + b;\n}\n\nmodule.exports = { add };\n";

async function makeWorktreeWithFixture(content = BUGGY_CONTENT): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), "relaylab-ai-repair-unit-"));
  await mkdir(path.join(dir, "src"), { recursive: true });
  await writeFile(path.join(dir, TARGET_RELATIVE_PATH), content, "utf8");
  return dir;
}

/** A fake client whose `messages.create` returns a fixed, labeled-as-simulated response. Never touches the network. */
function fakeClient(responder: () => Promise<Anthropic.Message> | Anthropic.Message): MessagesClient {
  return {
    messages: {
      create: (async (..._args: unknown[]) => responder()) as unknown,
    } as unknown as Anthropic.Messages,
  } as MessagesClient;
}

function simulatedMessage(text: string, overrides: Partial<Anthropic.Message> = {}): Anthropic.Message {
  return {
    id: "msg_simulated",
    type: "message",
    role: "assistant",
    model: "claude-opus-5-5-simulated",
    content: [{ type: "text", text, citations: null }],
    stop_reason: "end_turn",
    stop_sequence: null,
    usage: { input_tokens: 42, output_tokens: 7, cache_creation_input_tokens: null, cache_read_input_tokens: null, server_tool_use: null, service_tier: null },
    container: null,
    context_management: null,
    ...overrides,
  } as unknown as Anthropic.Message;
}

test("parseAndValidateEdits: accepts a valid single edit for the allowlisted path", () => {
  const result = parseAndValidateEdits(JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: FIXED_CONTENT }] }));
  assert.equal(result.ok, true);
  if (result.ok) assert.deepEqual(result.edits, [{ path: TARGET_RELATIVE_PATH, content: FIXED_CONTENT }]);
});

test("parseAndValidateEdits: rejects an edit targeting a FORBIDDEN path (not the allowlisted repair target)", () => {
  for (const forbiddenPath of ["verify.js", "tests/add.test.js", "package.json", "src/add.js.config.yaml", "../../etc/passwd"]) {
    const result = parseAndValidateEdits(JSON.stringify({ edits: [{ path: forbiddenPath, content: "x" }] }));
    assert.equal(result.ok, false, `expected "${forbiddenPath}" to be rejected`);
    if (!result.ok) assert.match(result.reason, /not the allowlisted repair target/);
  }
});

test("parseAndValidateEdits: rejects malformed (non-JSON) responses", () => {
  for (const malformed of ["not json at all", "```json\n{\"edits\":[]}\n```", "{edits: [1,2,3]}", ""]) {
    const result = parseAndValidateEdits(malformed);
    assert.equal(result.ok, false, `expected malformed input to be rejected: ${JSON.stringify(malformed)}`);
  }
});

test("parseAndValidateEdits: rejects more than one edit, and a missing/wrong-typed content field", () => {
  const tooMany = parseAndValidateEdits(JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: "a" }, { path: TARGET_RELATIVE_PATH, content: "b" }] }));
  assert.equal(tooMany.ok, false);
  const badContent = parseAndValidateEdits(JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: 12345 }] }));
  assert.equal(badContent.ok, false);
});

test("attemptAiRepair: a valid SIMULATED response is parsed, validated, and written to disk", async () => {
  const worktreeDir = await makeWorktreeWithFixture();
  try {
    const client = fakeClient(() => simulatedMessage(JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: FIXED_CONTENT }] })));
    const record = await attemptAiRepair(worktreeDir, client, CONFIG, 1);
    assert.equal(record.outcome, "edit_applied");
    assert.equal(record.modelUsed, "claude-opus-5-5-simulated");
    assert.deepEqual(record.usage, { inputTokens: 42, outputTokens: 7 });
    const written = await readFile(path.join(worktreeDir, TARGET_RELATIVE_PATH), "utf8");
    assert.equal(written, FIXED_CONTENT);
  } finally {
    await rm(worktreeDir, { recursive: true, force: true });
  }
});

test("attemptAiRepair: a SIMULATED response naming a forbidden path is rejected and the file on disk is untouched", async () => {
  const worktreeDir = await makeWorktreeWithFixture();
  try {
    const client = fakeClient(() => simulatedMessage(JSON.stringify({ edits: [{ path: "verify.js", content: "console.log('tampered')" }] })));
    const record = await attemptAiRepair(worktreeDir, client, CONFIG, 1);
    assert.equal(record.outcome, "rejected");
    assert.match(record.reason ?? "", /not the allowlisted repair target/);
    const stillBuggy = await readFile(path.join(worktreeDir, TARGET_RELATIVE_PATH), "utf8");
    assert.equal(stillBuggy, BUGGY_CONTENT, "the allowlisted file must be untouched when the model targeted a different path");
  } finally {
    await rm(worktreeDir, { recursive: true, force: true });
  }
});

test("attemptAiRepair: a MALFORMED SIMULATED response is rejected, not applied", async () => {
  const worktreeDir = await makeWorktreeWithFixture();
  try {
    const client = fakeClient(() => simulatedMessage("this is not JSON at all, just prose"));
    const record = await attemptAiRepair(worktreeDir, client, CONFIG, 1);
    assert.equal(record.outcome, "rejected");
    const stillBuggy = await readFile(path.join(worktreeDir, TARGET_RELATIVE_PATH), "utf8");
    assert.equal(stillBuggy, BUGGY_CONTENT);
  } finally {
    await rm(worktreeDir, { recursive: true, force: true });
  }
});

test("attemptAiRepair: a SIMULATED provider timeout is reported as provider_error, not silently retried or faked", async () => {
  const worktreeDir = await makeWorktreeWithFixture();
  try {
    const client = fakeClient(() => {
      // Simulates the SDK's own typed timeout behavior (APIConnectionTimeoutError) without a real network wait.
      throw new Anthropic.APIConnectionTimeoutError({ message: "Request timed out (simulated)." });
    });
    const record = await attemptAiRepair(worktreeDir, client, CONFIG, 1);
    assert.equal(record.outcome, "provider_error");
    assert.match(record.reason ?? "", /timed out|Anthropic API call failed/i);
    const stillBuggy = await readFile(path.join(worktreeDir, TARGET_RELATIVE_PATH), "utf8");
    assert.equal(stillBuggy, BUGGY_CONTENT);
  } finally {
    await rm(worktreeDir, { recursive: true, force: true });
  }
});

test("attemptAiRepair: cancellation via AbortSignal is reported as cancelled, not provider_error", async () => {
  const worktreeDir = await makeWorktreeWithFixture();
  try {
    const controller = new AbortController();
    controller.abort();
    const client = fakeClient(() => {
      throw new Anthropic.APIUserAbortError({ message: "Request was aborted (simulated)." });
    });
    const record = await attemptAiRepair(worktreeDir, client, CONFIG, 1, undefined, controller.signal);
    assert.equal(record.outcome, "cancelled");
  } finally {
    await rm(worktreeDir, { recursive: true, force: true });
  }
});

test("attemptAiRepair: a SIMULATED response whose content is IDENTICAL to the current (buggy) file is an explicit no_change outcome, not a write", async () => {
  const worktreeDir = await makeWorktreeWithFixture();
  try {
    const client = fakeClient(() => simulatedMessage(JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: BUGGY_CONTENT }] })));
    const record = await attemptAiRepair(worktreeDir, client, CONFIG, 1);
    assert.equal(record.outcome, "no_change");
    const stillBuggy = await readFile(path.join(worktreeDir, TARGET_RELATIVE_PATH), "utf8");
    assert.equal(stillBuggy, BUGGY_CONTENT, "no_change must never write anything to disk");
  } finally {
    await rm(worktreeDir, { recursive: true, force: true });
  }
});

test("attemptAiRepair: a symlinked repair target is rejected before being read or written", async (t) => {
  const dir = await mkdtemp(path.join(tmpdir(), "relaylab-ai-repair-unit-symlink-"));
  try {
    const outsideDir = await mkdtemp(path.join(tmpdir(), "relaylab-ai-repair-outside-"));
    const outsideFile = path.join(outsideDir, "secret.txt");
    await writeFile(outsideFile, "should never be read or written by this module", "utf8");
    await mkdir(path.join(dir, "src"), { recursive: true });
    const { symlink } = await import("node:fs/promises");
    try {
      await symlink(outsideFile, path.join(dir, TARGET_RELATIVE_PATH));
    } catch (err) {
      // Creating a symlink itself requires elevated privileges on this
      // Windows host (EPERM) -- an environment limitation unrelated to
      // the lstat-based rejection logic under test here. Skip rather
      // than fail: there is nothing this test can assert without a real
      // symlink to point at.
      if ((err as NodeJS.ErrnoException).code === "EPERM") {
        t.skip("symlink creation requires elevated privileges on this host");
        return;
      }
      throw err;
    }

    const client = fakeClient(() => simulatedMessage(JSON.stringify({ edits: [{ path: TARGET_RELATIVE_PATH, content: FIXED_CONTENT }] })));
    const record = await attemptAiRepair(dir, client, CONFIG, 1);
    assert.equal(record.outcome, "rejected");
    assert.match(record.reason ?? "", /not a regular file|symlink/);
    const outsideContent = await readFile(outsideFile, "utf8");
    assert.equal(outsideContent, "should never be read or written by this module", "the file outside the worktree must be completely untouched");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("requestStructuredEdit: a SIMULATED provider error's message never leaks the raw request body, prompt text, or API key", async () => {
  const secretLookingPrompt = "PROMPT_MARKER_SHOULD_NEVER_APPEAR_IN_ERROR sk-ant-fake-secret-token-xyz";
  const client: MessagesClient = {
    messages: {
      create: (async () => {
        // Simulates a real Anthropic APIError whose body happens to echo
        // the request back (some validation-error responses do this) --
        // the adapter must still never surface that raw text.
        throw new Anthropic.BadRequestError(400, { error: { message: `echo: ${secretLookingPrompt}` } }, `echo: ${secretLookingPrompt}`, new Headers());
      }) as unknown,
    } as unknown as Anthropic.Messages,
  } as MessagesClient;

  await assert.rejects(
    () => requestStructuredEdit(client, CONFIG, secretLookingPrompt),
    (err: unknown) => {
      assert.ok(err instanceof AnthropicAdapterError);
      assert.doesNotMatch(err.message, /PROMPT_MARKER_SHOULD_NEVER_APPEAR_IN_ERROR/);
      assert.doesNotMatch(err.message, /sk-ant-fake-secret-token-xyz/);
      return true;
    },
  );
});
