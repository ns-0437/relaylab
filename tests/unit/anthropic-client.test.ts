// AI repair milestone: exercises the REAL @anthropic-ai/sdk client,
// with a LOCAL FAKE `fetch` transport standing in for the network --
// no real network call, no real credentials, no tokens spent. This is
// what proves maxRetries:0 and the per-request timeout actually behave
// as configured at the real SDK layer, not merely that our own wrapper
// code calls the right-looking options.
import { test } from "node:test";
import assert from "node:assert/strict";
import Anthropic from "@anthropic-ai/sdk";
import { requestStructuredEdit, loadAnthropicAdapterConfig, type AnthropicAdapterConfig } from "../../src/ai/anthropic-client.js";

const CONFIG: AnthropicAdapterConfig = { model: "claude-opus-5-5", maxOutputTokens: 256, requestTimeoutMs: 2_000 };

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

test("real SDK client, fake transport: maxRetries:0 means a single transient-looking failure is NOT retried -- exactly 1 HTTP call", async () => {
  let callCount = 0;
  const fakeFetch: typeof fetch = async () => {
    callCount++;
    // 429 is one of the SDK's own default-retryable statuses -- if
    // maxRetries were not actually 0 here, the SDK would retry this
    // and callCount would exceed 1.
    return jsonResponse({ type: "error", error: { type: "rate_limit_error", message: "simulated rate limit" } }, 429);
  };
  const client = new Anthropic({ apiKey: "sk-ant-fake-test-key-never-real", fetch: fakeFetch });

  await assert.rejects(() => requestStructuredEdit(client, CONFIG, "irrelevant prompt"));
  assert.equal(callCount, 1, "exactly one real HTTP attempt must have been made -- the SDK's own default retries must be disabled");
});

test("real SDK client, fake transport: the configured request timeout actually aborts a hanging call", async () => {
  const fakeFetch: typeof fetch = (_url, init) =>
    new Promise((_resolve, reject) => {
      const signal = init?.signal as AbortSignal | undefined;
      signal?.addEventListener("abort", () => reject(new DOMException("simulated abort", "AbortError")));
      // Never resolves on its own -- only the configured timeout's abort should end this.
    });
  const client = new Anthropic({ apiKey: "sk-ant-fake-test-key-never-real", fetch: fakeFetch });
  const shortConfig: AnthropicAdapterConfig = { ...CONFIG, requestTimeoutMs: 300 };

  const start = Date.now();
  await assert.rejects(() => requestStructuredEdit(client, shortConfig, "irrelevant prompt"));
  const elapsed = Date.now() - start;
  assert.ok(elapsed < 5_000, `expected the configured ~300ms timeout to abort well under 5s, took ${elapsed}ms`);
});

test("real SDK client, fake transport: a valid SIMULATED success response round-trips model/usage/text correctly through the real SDK's own response parsing", async () => {
  const fakeFetch: typeof fetch = async () =>
    jsonResponse({
      id: "msg_fake",
      type: "message",
      role: "assistant",
      model: "claude-opus-5-5-simulated",
      content: [{ type: "text", text: JSON.stringify({ edits: [] }) }],
      stop_reason: "end_turn",
      stop_sequence: null,
      usage: { input_tokens: 11, output_tokens: 3 },
    });
  const client = new Anthropic({ apiKey: "sk-ant-fake-test-key-never-real", fetch: fakeFetch });

  const result = await requestStructuredEdit(client, CONFIG, "irrelevant prompt");
  assert.equal(result.modelUsed, "claude-opus-5-5-simulated");
  assert.deepEqual(result.usage, { inputTokens: 11, outputTokens: 3 });
});

test("loadAnthropicAdapterConfig: requires an explicit model unless the verified default is opted into", () => {
  const savedModel = process.env.RELAYLAB_AI_MODEL;
  const savedAllow = process.env.RELAYLAB_AI_ALLOW_DEFAULT_MODEL;
  delete process.env.RELAYLAB_AI_MODEL;
  delete process.env.RELAYLAB_AI_ALLOW_DEFAULT_MODEL;
  try {
    assert.throws(() => loadAnthropicAdapterConfig(), /RELAYLAB_AI_MODEL must be set explicitly/);
    process.env.RELAYLAB_AI_ALLOW_DEFAULT_MODEL = "1";
    assert.doesNotThrow(() => loadAnthropicAdapterConfig());
  } finally {
    if (savedModel === undefined) delete process.env.RELAYLAB_AI_MODEL;
    else process.env.RELAYLAB_AI_MODEL = savedModel;
    if (savedAllow === undefined) delete process.env.RELAYLAB_AI_ALLOW_DEFAULT_MODEL;
    else process.env.RELAYLAB_AI_ALLOW_DEFAULT_MODEL = savedAllow;
  }
});

test("loadAnthropicAdapterConfig: rejects non-integer and out-of-range token/timeout settings with documented ceilings", () => {
  const saved = { model: process.env.RELAYLAB_AI_MODEL, tokens: process.env.RELAYLAB_AI_MAX_OUTPUT_TOKENS, timeout: process.env.RELAYLAB_AI_TIMEOUT_MS };
  process.env.RELAYLAB_AI_MODEL = "claude-opus-5-5";
  try {
    process.env.RELAYLAB_AI_MAX_OUTPUT_TOKENS = "1.5";
    assert.throws(() => loadAnthropicAdapterConfig(), /must be an integer/);
    process.env.RELAYLAB_AI_MAX_OUTPUT_TOKENS = "999999999";
    assert.throws(() => loadAnthropicAdapterConfig(), /must be between/);
    delete process.env.RELAYLAB_AI_MAX_OUTPUT_TOKENS;
    process.env.RELAYLAB_AI_TIMEOUT_MS = "0";
    assert.throws(() => loadAnthropicAdapterConfig(), /must be between/);
  } finally {
    for (const [k, v] of Object.entries({ RELAYLAB_AI_MODEL: saved.model, RELAYLAB_AI_MAX_OUTPUT_TOKENS: saved.tokens, RELAYLAB_AI_TIMEOUT_MS: saved.timeout })) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
});
