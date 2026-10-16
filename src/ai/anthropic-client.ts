// Phase "AI repair" milestone: the ONE provider adapter this milestone
// adds -- a thin wrapper over the official `@anthropic-ai/sdk`, used
// exactly once (src/worker/ai-repair.ts) to generate a structured text
// edit for the existing scripted-repair fixture. No provider
// abstraction framework, no second provider, no generic "LLM client"
// interface -- just this one function against this one SDK.
//
// Credentials are read from the environment ONLY, by the SDK itself
// (`new Anthropic()` resolves `ANTHROPIC_API_KEY`/`ANTHROPIC_AUTH_TOKEN`
// -- see the SDK's own README). This module never reads, logs, or
// persists the key, and never hardcodes one. A missing key is NOT
// treated as a reason to fall back to a canned/mock response -- the
// SDK's own first real call throws a real `Anthropic.AuthenticationError`,
// which callers must handle as a genuine provider failure, never
// silently swallowed into a fake "success."
import Anthropic from "@anthropic-ai/sdk";

/** Model string is configurable, never hardcoded to one value callers can't override. */
export const DEFAULT_AI_REPAIR_MODEL = "claude-opus-5-5";
export const DEFAULT_AI_REPAIR_MAX_OUTPUT_TOKENS = 2048;
export const DEFAULT_AI_REPAIR_TIMEOUT_MS = 60_000;

export interface AnthropicAdapterConfig {
  model: string;
  maxOutputTokens: number;
  requestTimeoutMs: number;
}

/**
 * Reads ONLY the three tuning knobs this adapter exposes from the
 * environment -- never the API key itself (the SDK client construction
 * below does that). Safe to call with no credentials present at all:
 * this never throws for a missing key, so importing/configuring the
 * adapter (and anything that imports it, including this project's own
 * test files) never requires one. A key is only ever needed at the
 * point of an actual `messages.create` call.
 */
export function loadAnthropicAdapterConfig(): AnthropicAdapterConfig {
  const model = process.env.RELAYLAB_AI_MODEL?.trim() || DEFAULT_AI_REPAIR_MODEL;
  const maxOutputTokens = Number(process.env.RELAYLAB_AI_MAX_OUTPUT_TOKENS ?? DEFAULT_AI_REPAIR_MAX_OUTPUT_TOKENS);
  const requestTimeoutMs = Number(process.env.RELAYLAB_AI_TIMEOUT_MS ?? DEFAULT_AI_REPAIR_TIMEOUT_MS);
  if (!Number.isFinite(maxOutputTokens) || maxOutputTokens <= 0) {
    throw new Error(`RELAYLAB_AI_MAX_OUTPUT_TOKENS must be a positive number, got ${process.env.RELAYLAB_AI_MAX_OUTPUT_TOKENS}`);
  }
  if (!Number.isFinite(requestTimeoutMs) || requestTimeoutMs <= 0) {
    throw new Error(`RELAYLAB_AI_TIMEOUT_MS must be a positive number, got ${process.env.RELAYLAB_AI_TIMEOUT_MS}`);
  }
  return { model, maxOutputTokens, requestTimeoutMs };
}

/**
 * Constructs the real SDK client. Deliberately a zero-arg `new
 * Anthropic()` -- credentials resolve from the environment via the
 * SDK's own precedence (`ANTHROPIC_API_KEY`, then
 * `ANTHROPIC_AUTH_TOKEN`), never passed in as a string literal here.
 */
export function createAnthropicClient(): Anthropic {
  return new Anthropic();
}

/** The minimal slice of the real SDK client this module needs -- lets tests inject a fake client without depending on `Anthropic`'s full shape. */
export type MessagesClient = Pick<Anthropic, "messages">;

export interface AdapterCallResult {
  /** The model the PROVIDER actually reports having used -- not merely the requested string, so a silent server-side substitution would be visible. */
  modelUsed: string;
  text: string;
  usage: { inputTokens: number; outputTokens: number };
  stopReason: string | null;
}

export class AnthropicAdapterError extends Error {
  constructor(message: string, override readonly cause: unknown) {
    super(message);
    this.name = "AnthropicAdapterError";
  }
}

/**
 * One bounded, non-streaming request. Bounds input size (callers must
 * keep `prompt` small -- see ai-repair.ts's own context-size limit),
 * output tokens (`config.maxOutputTokens`), and wall-clock time
 * (`config.requestTimeoutMs`, enforced via the SDK's own per-request
 * timeout option, which the SDK turns into a real abort). Accepts an
 * external `AbortSignal` so a caller can cancel a still-in-flight
 * request (e.g. the whole repair attempt being cancelled).
 *
 * Never retries internally beyond the SDK's own default transient-error
 * retries -- the bounded-attempts policy (at most two REPAIR attempts,
 * each a fresh prompt) lives in ai-repair.ts, not here. Any provider
 * failure (auth, rate limit, timeout, 5xx) is wrapped in
 * `AnthropicAdapterError` and thrown -- never swallowed, never
 * downgraded into a fabricated response.
 */
export async function requestStructuredEdit(
  client: MessagesClient,
  config: AnthropicAdapterConfig,
  prompt: string,
  signal?: AbortSignal,
): Promise<AdapterCallResult> {
  let response: Anthropic.Message;
  try {
    response = await client.messages.create(
      {
        model: config.model,
        max_tokens: config.maxOutputTokens,
        messages: [{ role: "user", content: prompt }],
      },
      { timeout: config.requestTimeoutMs, signal },
    );
  } catch (err) {
    throw new AnthropicAdapterError(
      `Anthropic API call failed (model=${config.model}): ${err instanceof Error ? err.message : String(err)}`,
      err,
    );
  }

  const textBlock = response.content.find((block): block is Anthropic.TextBlock => block.type === "text");
  return {
    modelUsed: response.model,
    text: textBlock?.text ?? "",
    usage: { inputTokens: response.usage.input_tokens, outputTokens: response.usage.output_tokens },
    stopReason: response.stop_reason,
  };
}
