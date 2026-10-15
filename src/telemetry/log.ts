// Phase 1.2 audit, Part B: "add trace/span IDs to relevant existing
// structured logs without replacing the logging system." This is
// intentionally tiny -- a suffix-string helper, not a new logger. The
// existing `console.log`/`console.error` call sites in
// publisher-main.ts/consumer-main.ts stay exactly as they are; they just
// append this where it helps correlate a log line with a trace.
import { trace } from "@opentelemetry/api";

/** Returns " traceId=... spanId=..." if there is a valid active span, else "". */
export function traceLogSuffix(): string {
  const span = trace.getActiveSpan();
  if (!span) return "";
  const ctx = span.spanContext();
  if (!trace.isSpanContextValid(ctx)) return "";
  return ` traceId=${ctx.traceId} spanId=${ctx.spanId}`;
}
