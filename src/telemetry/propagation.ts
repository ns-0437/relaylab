// Phase 1.2 audit, Part B: W3C trace-context carrier handling across the
// durable outbox and NATS -- the two boundaries where an ordinary
// AsyncLocalStorage-based active context does NOT survive, because a
// publisher reads the outbox row in a LATER, separate process, and a
// consumer receives the NATS message in yet another separate process.
//
// Everything here is deliberately narrow:
//   - Only the two standard W3C fields (`traceparent`, `tracestate`) are
//     ever captured or restored. No baggage, no arbitrary headers.
//   - The carrier is NEVER treated as authorization. It only ever feeds
//     `propagation.extract`, which can at most attach span linkage; it
//     cannot change who owns a task or what a command is allowed to do.
//   - The carrier is stored in ITS OWN column (`outbox_events.trace_carrier`,
//     a nullable JSON blob with exactly these two keys), never mixed into
//     the business event payload that command/event idempotency
//     fingerprints and content hashes are computed over -- see
//     src/domain/fingerprint.ts callers, none of which ever receive a
//     TraceCarrier.
import { context, propagation, trace, ROOT_CONTEXT, type Context } from "@opentelemetry/api";

export interface TraceCarrier {
  traceparent?: string;
  tracestate?: string;
}

/** Captures the CURRENTLY ACTIVE span's context as a W3C carrier, allowlisted to exactly two fields. */
export function captureCurrentCarrier(): TraceCarrier | null {
  const raw: Record<string, string> = {};
  propagation.inject(context.active(), raw);
  const carrier: TraceCarrier = {};
  if (typeof raw.traceparent === "string") carrier.traceparent = raw.traceparent;
  if (typeof raw.tracestate === "string") carrier.tracestate = raw.tracestate;
  return carrier.traceparent ? carrier : null; // no active span => nothing meaningful to persist
}

/**
 * Builds a fresh Context from a stored/received carrier, rooted at
 * ROOT_CONTEXT rather than `context.active()`. This is deliberate and
 * required for "concurrent tasks do not inherit one another's context":
 * extracting onto the CURRENT ambient context would silently graft one
 * task's trace onto whatever (unrelated) span happens to be active in
 * this process at the moment -- e.g. if a publisher or consumer ever
 * processes more than one task's events in an overlapping async flow.
 * Rooting at ROOT_CONTEXT guarantees total isolation between tasks
 * regardless of what else is running in this process.
 *
 * Absent or invalid headers are handled safely: `propagation.extract` on
 * a carrier with no recognizable `traceparent` simply returns the
 * context unchanged (ROOT_CONTEXT back out), so callers never need to
 * branch on validity themselves -- the subsequent span creation just
 * starts a new, parentless trace instead of failing.
 */
export function contextFromCarrier(carrier: TraceCarrier | null | undefined): Context {
  if (!carrier || typeof carrier.traceparent !== "string") return ROOT_CONTEXT;
  return propagation.extract(ROOT_CONTEXT, carrier as Record<string, string>);
}

/** True iff the given context carries a valid, non-trivial span context (used to decide parent vs. link semantics at call sites). */
export function hasValidSpanContext(ctx: Context): boolean {
  const sc = trace.getSpanContext(ctx);
  return !!sc && trace.isSpanContextValid(sc);
}
