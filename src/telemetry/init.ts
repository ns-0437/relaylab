// Phase 1.2 audit, Part B (initial) + Phase 1.2.1 patch (lifecycle
// rewrite): OpenTelemetry tracing initialization.
//
// Import and call `startTelemetry(serviceName)` as the FIRST statement a
// *-main.ts entrypoint executes (before `main()` does anything else).
// This project uses ONLY explicit, hand-written spans (see
// docs/OBSERVABILITY.md "why no auto-instrumentation") -- no
// `instrumentations` are registered, so there is no require-time
// module-patching step that would make import ORDER relative to e.g.
// `mysql2`/`nats` imports matter functionally. Initializing first is
// still done, and documented here, as the safe default: if
// auto-instrumentation is ever added later, it would need exactly this
// ordering, and getting it right now costs nothing.
//
// There is exactly ONE global tracer provider registered at a time: the
// one `NodeSDK.start()` registers via `@opentelemetry/api`'s global
// registry. Effect's own tracing integration (`@effect/opentelemetry`) is
// deliberately NOT installed or used anywhere in this codebase, so there
// is no second competing provider.
import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
  ATTR_SERVICE_INSTANCE_ID,
  ATTR_DEPLOYMENT_ENVIRONMENT_NAME,
} from "@opentelemetry/semantic-conventions";
import { trace, context, propagation } from "@opentelemetry/api";

const SERVICE_VERSION = "0.1.0"; // matches package.json; bump together

/**
 * Explicit lifecycle state, replacing a prior design that used a single
 * `started` boolean. The boolean could not distinguish "never started,"
 * "running," "a shutdown is currently in flight" (needed so a SECOND
 * concurrent shutdown call can share the same operation instead of
 * racing it), and "fully shut down and safe to restart" -- and resetting
 * it to `false` before the real shutdown had settled (the previous
 * design) could let a new `startTelemetry()` call construct a second
 * `NodeSDK` while the first one's async shutdown was still running,
 * which is exactly the kind of overlap this state machine exists to
 * prevent.
 */
type LifecycleState = "idle" | "running" | "shutting_down" | "shutdown";

let state: LifecycleState = "idle";
let sdk: NodeSDK | undefined;
/** The ONE real shutdown operation in flight, if any -- shared by every concurrent `shutdownTelemetry()` caller (see below). */
let inFlightShutdown: Promise<void> | undefined;

export function isTelemetryEnabled(): boolean {
  return process.env.RELAYLAB_OTEL_DISABLED !== "1";
}

/** For tests/diagnostics only -- not part of the public contract other modules should branch on. */
export function getLifecycleState(): LifecycleState {
  return state;
}

/**
 * Starts the NodeSDK with an OTLP/HTTP exporter pointed at a local
 * Collector. Clean disabled mode: set RELAYLAB_OTEL_DISABLED=1 and this
 * is a complete no-op -- `getTracer()` then returns the OTel API's own
 * built-in no-op tracer (spans become harmless no-ops; application code
 * never has to branch on whether tracing is on).
 *
 * `serviceName` MUST be distinct per process role (see
 * docs/OBSERVABILITY.md "service names") -- e.g. "relaylab-publisher",
 * "relaylab-consumer", so spans from different roles are distinguishable
 * in the Collector/backend without relying on span names alone.
 *
 * **Same-process reinitialization IS supported** (tests rely on this to
 * exercise multiple start/shutdown cycles without spawning a new OS
 * process each time) -- but ONLY after a PRIOR `shutdownTelemetry()` call
 * has fully settled (state `"shutdown"`), because `shutdownTelemetry`
 * calls `trace.disable()`/`context.disable()`/`propagation.disable()`.
 * This is the part of the fix that matters: `NodeSDK.shutdown()` on its
 * own does NOT call those -- verified by reading the installed SDK's
 * source (`node_modules/@opentelemetry/sdk-node/build/src/sdk.js`,
 * `shutdown()` only calls `this._tracerProvider.shutdown()`) and
 * empirically (a second `NodeSDK` started after a first one's
 * `.shutdown()`, WITHOUT an explicit `disable()` in between, silently
 * produced zero exported spans -- `@opentelemetry/api`'s global
 * registration ignores a second `setGlobalTracerProvider` call once one
 * is already registered, shut down or not). Calling `startTelemetry()`
 * again WHILE a shutdown is still in flight (state `"shutting_down"`) is
 * refused with a logged error, specifically to prevent that overlap.
 */
export function startTelemetry(serviceName: string): void {
  if (state === "running") return; // already running, no-op
  if (state === "shutting_down") {
    console.error(
      "[telemetry] startTelemetry() called while a previous shutdown is still in flight; ignoring. " +
        "Await the previous shutdownTelemetry() call's promise before starting again.",
    );
    return;
  }
  // state is "idle" (never started) or "shutdown" (cleanly stopped, globals disabled) -- safe to (re)start.
  if (!isTelemetryEnabled()) {
    console.log(`[telemetry] disabled (RELAYLAB_OTEL_DISABLED=1), ${serviceName} will use no-op spans`);
    state = "running"; // "running" in disabled mode just means "don't try to start again"; sdk stays undefined
    return;
  }

  const endpoint = process.env.RELAYLAB_OTEL_EXPORTER_OTLP_ENDPOINT ?? "http://127.0.0.1:4318/v1/traces";
  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: serviceName,
    [ATTR_SERVICE_VERSION]: SERVICE_VERSION,
    [ATTR_DEPLOYMENT_ENVIRONMENT_NAME]: process.env.RELAYLAB_ENV ?? "dev",
    [ATTR_SERVICE_INSTANCE_ID]: `${serviceName}-${process.pid}-${Math.random().toString(36).slice(2, 8)}`,
  });

  sdk = new NodeSDK({
    resource,
    traceExporter: new OTLPTraceExporter({
      url: endpoint,
      // Export failures must never block or retry the BUSINESS
      // transaction -- the exporter runs on its own batched, bounded
      // schedule entirely independent of command/event processing. See
      // docs/OBSERVABILITY.md "Collector failure behavior".
      timeoutMillis: 5000,
    }),
    // W3C trace context ONLY -- deliberately NOT the default composite
    // (trace context + baggage) propagator. See
    // src/telemetry/propagation.ts and docs/OBSERVABILITY.md.
    textMapPropagator: new W3CTraceContextPropagator(),
    // Explicit spans only -- see the file header comment above.
    instrumentations: [],
    // Phase 1.2.1 Part B: explicitly EMPTY, not omitted. NodeSDK, if this
    // option is left out entirely, silently constructs its OWN metrics
    // pipeline by default (defaulting to an OTLP metric reader when
    // OTEL_METRICS_EXPORTER is unset -- see
    // node_modules/@opentelemetry/sdk-node/build/src/sdk.js
    // `getMetricReadersFromEnv`) and globally registers its own
    // MeterProvider via `metrics.setGlobalMeterProvider` inside
    // `sdk.start()`. Found the hard way: with this option omitted,
    // src/telemetry/metrics.ts's own `startMetrics()` (which runs AFTER
    // this call) lost that global registration race -- `@opentelemetry/api`
    // only allows ONE MeterProvider registration, so EVERY instrument
    // created by src/telemetry/metrics.ts silently attached to NodeSDK's
    // reader-less competing provider instead, while the real Prometheus
    // exporter's HTTP server kept serving an empty "# no registered
    // metrics" response -- no error, no crash, just silently wrong data.
    // An empty array here means NodeSDK's own `_meterProviderConfig`
    // check (`readers.length > 0`) is false, so it never registers a
    // MeterProvider of its own, leaving that global slot free for
    // metrics.ts's explicit, Prometheus-backed one.
    metricReaders: [],
  });
  sdk.start();
  state = "running";
  console.log(`[telemetry] started for "${serviceName}", exporting OTLP/HTTP to ${sanitizeEndpointForLogging(endpoint)}`);
}

/**
 * Phase 1.2.1 Part A.3: never logs the complete configured endpoint --
 * `RELAYLAB_OTEL_EXPORTER_OTLP_ENDPOINT` is an env var an operator
 * controls, and a URL can carry embedded userinfo credentials
 * (`https://user:pass@host/...`) or a revealing path/query an operator
 * would not expect to end up in plain-text logs. Logs only
 * `scheme://host:port` (no userinfo, no path, no query) -- enough to
 * confirm which Collector this process is pointed at, nothing more. If
 * the configured value isn't even parseable as a URL, logs a fixed
 * placeholder rather than the raw string.
 */
function sanitizeEndpointForLogging(endpoint: string): string {
  try {
    const u = new URL(endpoint);
    return `${u.protocol}//${u.host}`;
  } catch {
    return "<unparseable-endpoint>";
  }
}

/**
 * A FINITE allowlist of Node error `.code` values this project treats as
 * safe to log verbatim alongside a constructor name -- all standard,
 * well-known POSIX/libuv network-failure codes that never themselves
 * embed request-specific data (unlike `.message`, which routinely does:
 * a connection string, a URL with embedded credentials, a header value,
 * depending on the underlying HTTP/DB/NATS client). An arbitrary,
 * unrecognized `.code` is NOT assumed safe merely because it happens to
 * be a short string in that field -- see `classifyShutdownError` below,
 * which falls back to the constructor name ALONE (no code) for anything
 * not on this list. This is the Phase 1.2.1 patch's answer to "do not
 * treat arbitrary error.code or constructor.name strings as guaranteed
 * safe": the set of strings this function can ever return is bounded to
 * {known constructor names} x ({nothing} union {this allowlist}), never
 * an attacker- or environment-controlled value.
 */
const SAFE_ERROR_CODES = new Set(["ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "ENOTFOUND", "EPIPE", "EAI_AGAIN", "ECONNABORTED"]);

/**
 * A generic (non-domain) error's SAFE classification for logging: the
 * constructor name, plus the standard Node `.code` field ONLY when it is
 * one of the known-safe values above -- never `.message` or the error
 * object itself. This mirrors the domain-error classification in
 * src/telemetry/attributes.ts's `classifyError`, but for errors that
 * never carry a `_tag` (network/export failures, not typed domain
 * errors). Shared by telemetry shutdown logging and, since Phase 1.2.1,
 * the publisher/consumer main loops' own iteration-failure logging (see
 * publisher-main.ts/consumer-main.ts) -- one safe classifier, not two
 * slightly different ones.
 */
export function classifyShutdownError(err: unknown): string {
  if (err instanceof Error) {
    const code = (err as NodeJS.ErrnoException).code;
    return code && SAFE_ERROR_CODES.has(code) ? `${err.constructor.name}(${code})` : err.constructor.name;
  }
  return "UnknownError";
}

export interface ShutdownOutcome {
  /**
   * Did THIS CALL's bounded wait end because the real shutdown operation
   * settled (true), or because `timeoutMs` elapsed first (false)?
   *
   * This is explicitly NOT the same as either of:
   *   (B) the process having exited -- callers decide that separately
   *       (see publisher-main.ts/consumer-main.ts, which log a warning
   *       and exit anyway if this is false, rather than silently hiding
   *       the fact that the bound was exceeded);
   *   (C) all spans having been successfully exported to the Collector --
   *       even when `completedWithinBound` is true, `target.shutdown()`
   *       resolving only means the SDK's own shutdown sequence finished
   *       (which may itself have logged/swallowed an export failure --
   *       see `classifyShutdownError` above); it does not retroactively
   *       prove the Collector received everything. Use
   *       `telemetry-collector.manual.ts`-style verification against the
   *       Collector's own logs for that, never this return value.
   */
  completedWithinBound: boolean;
}

/**
 * Flushes buffered spans and shuts the exporter down. Returns once
 * EITHER the real shutdown settles OR `timeoutMs` elapses, whichever is
 * first -- but the real shutdown operation itself is NEVER abandoned:
 * `Promise.race` does not cancel its losing side, so if the timeout
 * fires first, the underlying `target.shutdown()` call keeps running in
 * the background and is still fully awaited (inside `realShutdown`
 * below) before global unregistration happens; this function just
 * doesn't make THIS CALLER wait for it past the bound. A later
 * `startTelemetry()` call is refused (see above) until that background
 * work actually finishes and transitions state to `"shutdown"`, so there
 * is no window where a new SDK could be constructed while the old one is
 * still being torn down.
 *
 * Concurrent calls to this function share the SAME underlying operation
 * (`inFlightShutdown`) rather than each starting their own
 * `target.shutdown()` -- calling `NodeSDK#shutdown()` twice concurrently
 * is not a documented-safe pattern in the installed SDK, so this avoids
 * relying on it being safe.
 *
 * Called during graceful shutdown (publisher-main.ts / consumer-main.ts
 * `stop()`), AFTER application work has finished but BEFORE the process
 * exits. A hard kill (SIGKILL / Windows `TerminateProcess`) gives this no
 * chance to run at all -- any spans still sitting in the batch
 * processor's buffer at that instant are lost, permanently. This is a
 * STATED, ACCEPTED limitation (see docs/OBSERVABILITY.md "graceful
 * shutdown vs. hard-kill"): tracing is diagnostic, not a durable audit
 * record, and nothing in this codebase's correctness depends on a span
 * having been exported.
 */
export async function shutdownTelemetry(timeoutMs = 5000): Promise<ShutdownOutcome> {
  if (state === "idle" || state === "shutdown") {
    return { completedWithinBound: true }; // nothing to shut down
  }

  if (state !== "shutting_down") {
    // We are the first caller to initiate shutdown -- own the real operation.
    state = "shutting_down";
    const target = sdk;
    sdk = undefined;

    inFlightShutdown = (async () => {
      try {
        if (target) await target.shutdown();
      } catch (err) {
        // Late rejection handled HERE, inside the one real operation,
        // regardless of whether any caller is still waiting on it --
        // this `catch` is what prevents an unhandled rejection even if
        // every bounded caller already timed out and moved on.
        console.error("[telemetry] shutdown error (classification):", classifyShutdownError(err));
      } finally {
        // Only after the real shutdown attempt has fully settled
        // (success OR failure) do we unregister the global API state.
        // Doing this earlier (e.g. immediately after calling
        // target.shutdown() without awaiting it) could disable() out
        // from under spans the shutdown sequence itself is still trying
        // to flush.
        trace.disable();
        context.disable();
        propagation.disable();
        state = "shutdown";
      }
    })();
  }

  // Every caller (the one that just created inFlightShutdown, and any
  // concurrent caller that arrives while state === "shutting_down") races
  // the SAME shared promise against its OWN bound.
  const real = inFlightShutdown!;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      real.then(() => ({ completedWithinBound: true }) as ShutdownOutcome),
      new Promise<ShutdownOutcome>((resolve) => {
        timer = setTimeout(() => resolve({ completedWithinBound: false }), timeoutMs);
      }),
    ]);
  } finally {
    // Cleared regardless of which side of the race won -- a timer left
    // running after `real` wins would otherwise keep firing a resolve()
    // against an already-settled Promise.race (harmless, since the race
    // already picked a winner, but it is still a dangling timer handle
    // that can keep the event loop alive, which matters for a process
    // trying to exit cleanly).
    if (timer) clearTimeout(timer);
  }
}

export function getTracer() {
  return trace.getTracer("relaylab");
}
