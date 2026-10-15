// Phase 1.2 audit, Part B: a FIXED, typed shape for every attribute this
// project ever attaches to a span. There is no "pass an arbitrary object"
// escape hatch anywhere else in the codebase -- every call site that
// wants to annotate a span must go through `setRelaylabAttributes`,
// which only accepts fields from this interface. That is the redaction
// mechanism: it is structurally impossible to attach a credential, a raw
// SQL statement, file contents, a prompt, or an environment dump to a
// span, because there is no parameter that would carry one.
import type { Span } from "@opentelemetry/api";

export type RelaylabOutcome =
  | "committed"
  | "replayed"
  | "rejected"
  | "failed"
  | "deduped"
  | "buffered"
  | "applied"
  | "conflict"
  | "dead_lettered"
  | "published"
  | "publish_failed"
  | "acquired"
  | "renewed"
  | "lost";

export interface RelaylabSpanAttributes {
  taskId?: string;
  eventId?: string;
  commandId?: string;
  seq?: number;
  epoch?: number;
  commandType?: string;
  eventType?: string;
  deliveryAttempt?: number;
  outcome?: RelaylabOutcome;
  /** A stable error TAG (e.g. "EpochStale", "CommandConflict") -- never a raw exception message. */
  errorType?: string;
}

export function setRelaylabAttributes(span: Span, attrs: RelaylabSpanAttributes): void {
  if (attrs.taskId !== undefined) span.setAttribute("relaylab.task_id", attrs.taskId);
  if (attrs.eventId !== undefined) span.setAttribute("relaylab.event_id", attrs.eventId);
  if (attrs.commandId !== undefined) span.setAttribute("relaylab.command_id", attrs.commandId);
  if (attrs.seq !== undefined) span.setAttribute("relaylab.seq", attrs.seq);
  if (attrs.epoch !== undefined) span.setAttribute("relaylab.epoch", attrs.epoch);
  if (attrs.commandType !== undefined) span.setAttribute("relaylab.command_type", attrs.commandType);
  if (attrs.eventType !== undefined) span.setAttribute("relaylab.event_type", attrs.eventType);
  if (attrs.deliveryAttempt !== undefined) span.setAttribute("relaylab.delivery_attempt", attrs.deliveryAttempt);
  if (attrs.outcome !== undefined) span.setAttribute("relaylab.outcome", attrs.outcome);
  if (attrs.errorType !== undefined) span.setAttribute("relaylab.error_type", attrs.errorType);
}

/**
 * Stable error classification, independent of the raw `Error#message`
 * (which is never recorded on a span by this helper -- see
 * docs/OBSERVABILITY.md "redaction rules"). Maps each of this project's
 * typed domain errors (src/domain/errors.ts) to a short tag.
 */
export function classifyError(err: unknown): string {
  if (err && typeof err === "object" && "_tag" in err && typeof (err as { _tag: unknown })._tag === "string") {
    return (err as { _tag: string })._tag;
  }
  return "UnknownError";
}

/** Is this a tagged domain error (expected business outcome) vs. an infrastructure failure? See docs/OBSERVABILITY.md "span-status policy". */
export function isExpectedDomainRejection(errorType: string): boolean {
  return ["EpochStale", "ClaimLost", "NotAuthorized", "CommandConflict", "CommandRejected", "TaskNotFound"].includes(
    errorType,
  );
}
