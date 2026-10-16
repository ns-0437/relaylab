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

/**
 * Model string is configurable, never hardcoded to one value callers
 * can't override. This default is a model ID verified against the
 * Anthropic model catalog current at the time this milestone was
 * written (not guessed/invented) -- `loadAnthropicAdapterConfig` still
 * requires `RELAYLAB_AI_MODEL` to be set EXPLICITLY unless the caller
 * opts into trusting this verified default (see
 * `RELAYLAB_AI_ALLOW_DEFAULT_MODEL` below), so a silent, unreviewed
 * drift onto a renamed/retired model ID is never possible.
 */
export const DEFAULT_AI_REPAIR_MODEL = "claude-opus-5-5";
export const DEFAULT_AI_REPAIR_MAX_OUTPUT_TOKENS = 2048;
export const DEFAULT_AI_REPAIR_TIMEOUT_MS = 60_000;
/** Hard ceilings -- not merely "positive", a documented upper bound so a misconfigured env var cannot request an unbounded-cost or unbounded-duration call. */
export const MAX_ALLOWED_OUTPUT_TOKENS = 8_192;
export const MAX_ALLOWED_REQUEST_TIMEOUT_MS = 180_000;
export const MIN_ALLOWED_REQUEST_TIMEOUT_MS = 1_000;

export interface AnthropicAdapterConfig {
  model: string;
  maxOutputTokens: number;
  requestTimeoutMs: number;
}

function parseBoundedInt(raw: string | undefined, name: string, fallback: number, min: number, max: number): number {
  if (raw === undefined) return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n)) {
    throw new Error(`${name} must be an integer, got "${raw}"`);
  }
  if (n < min || n > max) {
    throw new Error(`${name} must be between ${min} and ${max} inclusive, got ${n}`);
  }
  return n;
}

/**
 * Reads ONLY the tuning knobs this adapter exposes from the
 * environment -- never the API key itself (the SDK client construction
 * below does that). Safe to call with no credentials present at all:
 * this never throws for a missing key, so importing/configuring the
 * adapter (and anything that imports it, including this project's own
 * test files) never requires one. A key is only ever needed at the
 * point of an actual `messages.create` call.
 *
 * `model` requires an EXPLICIT `RELAYLAB_AI_MODEL` unless
 * `RELAYLAB_AI_ALLOW_DEFAULT_MODEL=1` is also set -- this is the
 * "require an explicit model setting unless the default is verified"
 * boundary: the fallback value above IS a verified real model ID, but
 * this function still refuses to silently use it in an unattended
 * config unless the caller affirmatively opts in, so a future model
 * rename/retirement is caught at config time, not discovered from a
 * confusing provider error at call time.
 */
export function loadAnthropicAdapterConfig(): AnthropicAdapterConfig {
  const rawModel = process.env.RELAYLAB_AI_MODEL?.trim();
  let model: string;
  if (rawModel) {
    model = rawModel;
  } else if (process.env.RELAYLAB_AI_ALLOW_DEFAULT_MODEL === "1") {
    model = DEFAULT_AI_REPAIR_MODEL;
  } else {
    throw new Error(
      `RELAYLAB_AI_MODEL must be set explicitly (or set RELAYLAB_AI_ALLOW_DEFAULT_MODEL=1 to use the verified default "${DEFAULT_AI_REPAIR_MODEL}")`,
    );
  }
  const maxOutputTokens = parseBoundedInt(process.env.RELAYLAB_AI_MAX_OUTPUT_TOKENS, "RELAYLAB_AI_MAX_OUTPUT_TOKENS", DEFAULT_AI_REPAIR_MAX_OUTPUT_TOKENS, 1, MAX_ALLOWED_OUTPUT_TOKENS);
  const requestTimeoutMs = parseBoundedInt(
    process.env.RELAYLAB_AI_TIMEOUT_MS,
    "RELAYLAB_AI_TIMEOUT_MS",
    DEFAULT_AI_REPAIR_TIMEOUT_MS,
    MIN_ALLOWED_REQUEST_TIMEOUT_MS,
    MAX_ALLOWED_REQUEST_TIMEOUT_MS,
  );
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
 * Issues at most ONE real HTTP request -- `maxRetries: 0` explicitly
 * disables the SDK's own default transient-error retries, so the
 * bounded-attempts policy run-task-ai.ts enforces (at most two REPAIR
 * attempts) also bounds the actual number of HTTP calls made, not just
 * the number of logical attempts. Any provider failure (auth, rate
 * limit, timeout, 5xx) is wrapped in a sanitized `AnthropicAdapterError`
 * and thrown -- never swallowed, never downgraded into a fabricated
 * response, and never carrying the raw SDK error body (which can echo
 * request content) into a message a caller might log.
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
      // maxRetries: 0 is REQUIRED, not cosmetic -- the SDK's own default
      // (2 retries on 408/409/429/5xx/connection errors) would silently
      // make ONE logical "attempt" issue up to 3 real HTTP requests,
      // defeating the bounded-attempts policy (at most 2 end-to-end
      // repair attempts) that run-task-ai.ts enforces at the attempt
      // level. Disabling SDK-level retries here is what makes "2
      // attempts" also bound the actual number of HTTP calls made.
      { timeout: config.requestTimeoutMs, signal, maxRetries: 0 },
    );
  } catch (err) {
    // Sanitized deliberately: never include the raw SDK error's full
    // message/body (which can echo back request content, including
    // this prompt, in some validation-error responses) -- only a fixed
    // classification (error constructor name + HTTP status, when
    // present) ever reaches a log or an attempt record.
    const status = err instanceof Anthropic.APIError ? err.status : undefined;
    const kind = err instanceof Error ? err.constructor.name : "UnknownError";
    throw new AnthropicAdapterError(`Anthropic API call failed (model=${config.model}): ${kind}${status ? ` (status ${status})` : ""}`, err);
  }

  const textBlock = response.content.find((block): block is Anthropic.TextBlock => block.type === "text");
  return {
    modelUsed: response.model,
    text: textBlock?.text ?? "",
    usage: { inputTokens: response.usage.input_tokens, outputTokens: response.usage.output_tokens },
    stopReason: response.stop_reason,
  };
}
