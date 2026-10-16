// AI repair milestone: an explicit SECOND repair mode alongside
// repair.ts's scripted, deterministic one. Targets the SAME existing
// small fixture and the SAME single allowlisted path
// (`repair.ts`'s TARGET_RELATIVE_PATH, "src/add.js") -- this milestone
// does not widen the allowlist or add a second fixture. The model is
// given the current (buggy) file content plus a plain description of
// the expected behavior, and asked to return ONE structured JSON edit.
// There is no tool-use surface here at all (no `tools` in the request)
// -- the model cannot choose a shell command, a file path, or anything
// else procedural; it can only produce text, which this module then
// validates strictly before anything is written to disk.
import { lstat, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { TARGET_RELATIVE_PATH } from "./repair.js";
import {
  requestStructuredEdit,
  AnthropicAdapterError,
  type AnthropicAdapterConfig,
  type MessagesClient,
} from "../ai/anthropic-client.js";

export const AI_REPAIR_NAME = "ai-repair-add-subtraction-bug-v1";

/** Hard ceiling, independent of any one request's `maxOutputTokens` -- defends against a response whose `content` field is technically within the token budget but still absurdly large (e.g. a repeated-token degenerate completion). */
const MAX_RESPONSE_CONTENT_CHARS = 20_000;
/** Bound on the file content this module will ever read into a prompt or accept back from the model -- this fixture's real file is a few lines; anything near this size is already a sign something is wrong, not a legitimate edit. */
const MAX_FILE_CONTENT_CHARS = 5_000;

export interface RepairEdit {
  path: string;
  content: string;
}

export type AiRepairAttemptOutcome =
  | { kind: "edit_applied"; changed: boolean }
  | { kind: "no_change"; reason: string }
  | { kind: "rejected"; reason: string }
  | { kind: "provider_error"; reason: string }
  | { kind: "cancelled" };

export interface AiRepairAttemptRecord {
  attempt: number;
  /** The model the PROVIDER reported using, not merely the one requested -- absent entirely for a provider_error/cancelled attempt that never got a response. */
  modelUsed: string | null;
  outcome: AiRepairAttemptOutcome["kind"];
  reason?: string;
  usage?: { inputTokens: number; outputTokens: number };
}

function buildPrompt(currentContent: string, diagnostics?: string): string {
  const diagnosticsBlock = diagnostics
    ? `\n\nThe previous attempt's edit FAILED real verification. Bounded diagnostics from that run:\n${diagnostics}\n\nFix the actual defect; do not repeat the same change if it did not work.`
    : "";
  return [
    `You are repairing exactly one file in a small fixture repository: ${TARGET_RELATIVE_PATH}.`,
    "",
    "Current file content:",
    "```",
    currentContent,
    "```",
    "",
    'The function is named `add` and is intended to return the sum of its two arguments, but its current body does not. Fix ONLY this defect.',
    diagnosticsBlock,
    "",
    "Respond with ONLY a single JSON object, no other text, no markdown code fences, matching exactly this shape:",
    `{"edits": [{"path": ${JSON.stringify(TARGET_RELATIVE_PATH)}, "content": "<the full corrected file content>"}]}`,
    "",
    "Rules: exactly one edit, for exactly that path, with the FULL corrected file content (not a diff/patch). Do not include any other path. Do not include any explanation, shell command, or additional text outside the JSON object.",
  ].join("\n");
}

/**
 * Parses and validates the model's raw text response. Every rejection
 * reason is specific and never silently falls back to a default edit --
 * an invalid response always results in `{ kind: "rejected", reason }`,
 * never a guess.
 *
 * Rejects: non-JSON text, a top-level shape other than `{edits: [...]}`,
 * any number of edits other than exactly one, any path other than the
 * single allowlisted one (`TARGET_RELATIVE_PATH` -- this is what
 * excludes tests, verification scripts, configuration, and every
 * unrelated file: nothing else is ever accepted, regardless of what the
 * model names), a non-string content field, or content exceeding the
 * bounded size.
 */
export function parseAndValidateEdits(responseText: string): { ok: true; edits: RepairEdit[] } | { ok: false; reason: string } {
  if (responseText.length === 0) {
    return { ok: false, reason: "empty response text" };
  }
  if (responseText.length > MAX_RESPONSE_CONTENT_CHARS) {
    return { ok: false, reason: `response text exceeds the bounded size limit (${responseText.length} > ${MAX_RESPONSE_CONTENT_CHARS} chars)` };
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(responseText);
  } catch (err) {
    return { ok: false, reason: `response is not valid JSON: ${err instanceof Error ? err.message : String(err)}` };
  }

  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return { ok: false, reason: "top-level response is not a JSON object" };
  }
  const edits = (parsed as Record<string, unknown>).edits;
  if (!Array.isArray(edits)) {
    return { ok: false, reason: '"edits" field is missing or not an array' };
  }
  if (edits.length !== 1) {
    return { ok: false, reason: `expected exactly 1 edit, got ${edits.length}` };
  }

  const rawEdit = edits[0];
  if (typeof rawEdit !== "object" || rawEdit === null) {
    return { ok: false, reason: "edit entry is not an object" };
  }
  const editPath = (rawEdit as Record<string, unknown>).path;
  const editContent = (rawEdit as Record<string, unknown>).content;

  if (typeof editPath !== "string") {
    return { ok: false, reason: '"path" field is missing or not a string' };
  }
  // The ONE allowlist check that actually matters: strict equality
  // against the single supported repair target. Never a prefix match,
  // never a glob, never "starts with src/" -- an exact string
  // comparison that rejects every test file, verification script,
  // config file, and unrelated path, with no pattern-matching logic to
  // get subtly wrong.
  if (editPath !== TARGET_RELATIVE_PATH) {
    return { ok: false, reason: `edit path "${editPath}" is not the allowlisted repair target ("${TARGET_RELATIVE_PATH}") -- refusing to touch anything else` };
  }
  if (typeof editContent !== "string") {
    return { ok: false, reason: '"content" field is missing or not a string' };
  }
  if (editContent.length === 0) {
    return { ok: false, reason: "edit content is empty" };
  }
  if (editContent.length > MAX_FILE_CONTENT_CHARS) {
    return { ok: false, reason: `edit content exceeds the bounded size limit (${editContent.length} > ${MAX_FILE_CONTENT_CHARS} chars)` };
  }

  return { ok: true, edits: [{ path: editPath, content: editContent }] };
}

/**
 * ONE bounded provider call plus validation plus (on success) the
 * actual file write -- never a commit (the caller, run-task-ai.ts,
 * owns committing via the SAME `commitAllowedPaths` the deterministic
 * path uses, so the allowlist is enforced a SECOND time at the Git
 * layer independently of this module's own check above).
 *
 * Never logs the prompt or the raw response text -- only the bounded
 * outcome record (model, outcome kind, reason, token usage) is ever
 * returned for the caller to persist/log.
 */
export async function attemptAiRepair(
  worktreeDir: string,
  client: MessagesClient,
  config: AnthropicAdapterConfig,
  attemptNumber: number,
  diagnostics?: string,
  signal?: AbortSignal,
): Promise<AiRepairAttemptRecord> {
  const targetPath = path.join(worktreeDir, TARGET_RELATIVE_PATH);

  // Reject a symlinked repair target BEFORE reading or writing it --
  // `lstat` (not `stat`) reports the link itself, never following it,
  // so a symlink planted at the allowlisted path pointing outside the
  // worktree is caught here instead of this module unknowingly
  // reading/writing through it to an arbitrary location.
  try {
    const st = await lstat(targetPath);
    if (!st.isFile()) {
      return { attempt: attemptNumber, modelUsed: null, outcome: "rejected", reason: `${TARGET_RELATIVE_PATH} is not a regular file (symlink or other special file) -- refusing to read or write through it` };
    }
  } catch (err) {
    return { attempt: attemptNumber, modelUsed: null, outcome: "rejected", reason: `could not stat ${TARGET_RELATIVE_PATH}: ${err instanceof Error ? err.message : String(err)}` };
  }

  let currentContent: string;
  try {
    currentContent = await readFile(targetPath, "utf8");
  } catch (err) {
    return { attempt: attemptNumber, modelUsed: null, outcome: "rejected", reason: `could not read ${TARGET_RELATIVE_PATH}: ${err instanceof Error ? err.message : String(err)}` };
  }
  if (currentContent.length > MAX_FILE_CONTENT_CHARS) {
    return { attempt: attemptNumber, modelUsed: null, outcome: "rejected", reason: `current file content exceeds the bounded size limit this adapter will send as context (${currentContent.length} > ${MAX_FILE_CONTENT_CHARS} chars)` };
  }

  const prompt = buildPrompt(currentContent, diagnostics);

  let call;
  try {
    call = await requestStructuredEdit(client, config, prompt, signal);
  } catch (err) {
    if (signal?.aborted) {
      return { attempt: attemptNumber, modelUsed: null, outcome: "cancelled" };
    }
    const reason = err instanceof AnthropicAdapterError ? err.message : err instanceof Error ? err.message : String(err);
    return { attempt: attemptNumber, modelUsed: null, outcome: "provider_error", reason };
  }

  const validated = parseAndValidateEdits(call.text);
  if (!validated.ok) {
    return {
      attempt: attemptNumber,
      modelUsed: call.modelUsed,
      outcome: "rejected",
      reason: validated.reason,
      usage: call.usage,
    };
  }

  // A generated edit whose content is BYTE-IDENTICAL to the file's
  // current content is an explicit, named outcome -- "no_change" --
  // never silently written (writing identical bytes produces no Git
  // diff at all, which would otherwise surface downstream as
  // commitAllowedPaths' confusing "nothing to commit" error) and never
  // treated as a successful edit. This consumes this attempt (the
  // caller still advances attemptNumber and may retry) exactly like a
  // rejected/malformed response does.
  const edit = validated.edits[0]!;
  if (edit.content === currentContent) {
    return {
      attempt: attemptNumber,
      modelUsed: call.modelUsed,
      outcome: "no_change",
      reason: "generated content is identical to the file's current content -- no edit to commit",
      usage: call.usage,
    };
  }
  // Cancellation is checked again HERE, immediately before the actual
  // write -- a signal that fired while the provider call above was in
  // flight (and therefore didn't throw, e.g. the response had already
  // started arriving) must still stop this attempt from applying
  // anything to disk.
  if (signal?.aborted) {
    return { attempt: attemptNumber, modelUsed: call.modelUsed, outcome: "cancelled" };
  }
  await writeFile(targetPath, edit.content, "utf8");
  return {
    attempt: attemptNumber,
    modelUsed: call.modelUsed,
    outcome: "edit_applied",
    usage: call.usage,
  };
}
