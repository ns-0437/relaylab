// Phase 1.2 audit, Part B: a small shared wrapper so every ownership/
// command span follows the exact same span-status policy (see
// docs/OBSERVABILITY.md "span-status policy") without repeating the
// try/catch/finally boilerplate at each call site.
import { SpanStatusCode } from "@opentelemetry/api";
import { getTracer } from "./init.js";
import { setRelaylabAttributes, classifyError, isExpectedDomainRejection, type RelaylabSpanAttributes } from "./attributes.js";
import {
  recordCommandAttempt,
  recordOwnershipAttempt,
  type BoundedRejectReason,
  type BoundedCommandType,
  type CommandOutcome,
  type OwnershipOutcome,
} from "./metrics.js";

const KNOWN_REJECT_REASONS: ReadonlySet<string> = new Set([
  "EpochStale",
  "ClaimLost",
  "NotAuthorized",
  "CommandConflict",
  "CommandRejected",
  "TaskNotFound",
]);

function boundedReason(errorType: string | undefined): BoundedRejectReason {
  if (!errorType) return "n/a";
  return KNOWN_REJECT_REASONS.has(errorType) ? (errorType as BoundedRejectReason) : "InfrastructureError";
}

function boundedCommandType(commandType: string | undefined): BoundedCommandType {
  const known = ["Checkpoint", "SubmitCandidate", "RecordVerification", "Complete"];
  return commandType && known.includes(commandType) ? (commandType as BoundedCommandType) : "unknown";
}

export async function withRelaylabSpan<T>(
  name: string,
  attrs: RelaylabSpanAttributes,
  fn: () => Promise<T>,
  onSuccess?: (result: T) => RelaylabSpanAttributes,
): Promise<T> {
  const tracer = getTracer();
  // Phase 1.2.1 Part C: this ONE shared wrapper is also the ONE place
  // that records relaylab_command_attempts_total/duration (for
  // "relaylab.command.process") and relaylab_ownership_attempts_total
  // (for "relaylab.ownership.claim"/"relaylab.ownership.renew") -- see
  // docs/METRICS.md's dictionary. Which bucket applies is determined by
  // the span `name`, since those are the only three names this helper is
  // ever called with (see src/db/task-store.ts).
  const startedAtMs = Date.now();
  return tracer.startActiveSpan(name, async (span) => {
    setRelaylabAttributes(span, attrs);
    try {
      const result = await fn();
      const successAttrs = onSuccess ? onSuccess(result) : {};
      setRelaylabAttributes(span, successAttrs);
      recordAttempt(name, attrs, successAttrs.outcome, undefined, startedAtMs);
      return result;
    } catch (err) {
      const errorType = classifyError(err);
      const expected = isExpectedDomainRejection(errorType);
      // Phase 1.2.1 monitoring-reliability patch, section 2: the SPAN
      // attribute keeps using "rejected" for ANY throw (unchanged --
      // span-level outcome is a looser, trace-oriented label, not the
      // metrics outcome enum), but the METRIC outcome now distinguishes
      // an expected domain rejection from an unexpected infrastructure
      // failure -- see recordAttempt below and docs/METRICS.md.
      setRelaylabAttributes(span, { outcome: "rejected", errorType });
      recordAttempt(name, attrs, expected ? "rejected" : "infrastructure_failure", errorType, startedAtMs);
      // Expected domain rejections (ClaimLost, EpochStale, NotAuthorized,
      // CommandConflict, CommandRejected, TaskNotFound) are a normal
      // business outcome, not a span-level error -- see
      // docs/OBSERVABILITY.md. Anything else is an infrastructure
      // failure and IS marked ERROR.
      if (!expected) {
        span.setStatus({ code: SpanStatusCode.ERROR, message: errorType });
      }
      throw err;
    } finally {
      span.end();
    }
  });
}

/**
 * `outcome` here is EITHER the real success-path outcome string
 * ("committed"/"replayed"/"acquired"/"renewed", from `onSuccess`) OR,
 * on the catch path, the caller-computed "rejected" | "infrastructure_failure"
 * bucket (see the catch block above) -- never a generic fallback string,
 * so this function does not need (and must not apply) its own
 * success/failure guessing on top of that.
 */
function recordAttempt(
  spanName: string,
  attrs: RelaylabSpanAttributes,
  outcome: string | undefined,
  errorType: string | undefined,
  startedAtMs: number,
): void {
  const durationSeconds = (Date.now() - startedAtMs) / 1000;
  const reason = boundedReason(errorType);
  if (spanName === "relaylab.command.process") {
    const commandOutcome: CommandOutcome =
      outcome === "committed" || outcome === "replayed" || outcome === "infrastructure_failure" ? outcome : "rejected";
    recordCommandAttempt(commandOutcome, reason, boundedCommandType(attrs.commandType), durationSeconds);
  } else if (spanName === "relaylab.ownership.claim") {
    const ownershipOutcome: OwnershipOutcome = outcome === "acquired" || outcome === "infrastructure_failure" ? outcome : "rejected";
    recordOwnershipAttempt("claim", ownershipOutcome, reason);
  } else if (spanName === "relaylab.ownership.renew") {
    const ownershipOutcome: OwnershipOutcome = outcome === "renewed" || outcome === "infrastructure_failure" ? outcome : "rejected";
    recordOwnershipAttempt("renew", ownershipOutcome, reason);
  }
}
