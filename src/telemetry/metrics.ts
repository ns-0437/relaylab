// Phase 1.2.1 patch, Part B: Prometheus metrics via the OpenTelemetry
// Metrics API + @opentelemetry/exporter-prometheus -- see
// docs/METRICS.md "Part B: architecture decision" for why this path was
// chosen over a separate dedicated Prometheus client, and the metric
// dictionary for every instrument's exact name/type/unit/labels.
//
// Deliberately a SEPARATE module and a SEPARATE global registration
// (metrics.setGlobalMeterProvider) from src/telemetry/init.ts's tracing
// (trace.setGlobalTracerProvider) -- two different slots in
// @opentelemetry/api's global registry, so a metrics problem can never
// affect tracing and vice versa. Like trace's getTracer(), calling
// metrics.getMeter() BEFORE a MeterProvider is registered permanently
// returns a no-op meter from THAT call (the metrics API, unlike trace's
// ProxyTracerProvider, does not retroactively upgrade an already-vended
// no-op) -- every instrument accessor below is therefore a lazily
// cached getter, never created at module load time, so it always
// resolves against whatever is registered by the time it is first
// actually used (after startMetrics() has run).
import { metrics, type Meter, type Counter, type Histogram, type ObservableGauge, type ObservableResult } from "@opentelemetry/api";
import { MeterProvider } from "@opentelemetry/sdk-metrics";
import { PrometheusExporter } from "@opentelemetry/exporter-prometheus";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION, ATTR_DEPLOYMENT_ENVIRONMENT_NAME } from "@opentelemetry/semantic-conventions";

const SERVICE_VERSION = "0.1.0"; // kept in sync with package.json/init.ts by hand

type LifecycleState = "idle" | "running" | "shutting_down" | "shutdown";
let state: LifecycleState = "idle";
let provider: MeterProvider | undefined;
let exporter: PrometheusExporter | undefined;

export function isMetricsEnabled(): boolean {
  // Deliberately the SAME flag as tracing (RELAYLAB_OTEL_DISABLED), not a
  // second one -- see docs/METRICS.md "Part B" for why: this project's
  // test harness already defaults every non-telemetry-specific test to
  // disabled telemetry (tests/integration/process-harness.ts
  // `testChildEnv`), and a second independent flag would need its own
  // default wired through every one of those call sites for no benefit.
  // Metrics and tracing remain independent PIPELINES either way (see
  // "shutdown and monitoring outages" in docs/METRICS.md) -- this is one
  // on/off switch for "does this process attempt any telemetry at all,"
  // not a claim that the two signals share an exporter or a failure mode.
  return process.env.RELAYLAB_OTEL_DISABLED !== "1";
}

export function getMetricsLifecycleState(): LifecycleState {
  return state;
}

/**
 * Starts a Prometheus-scrapeable metrics endpoint on `port` (default
 * 9464 for the publisher, 9465 for the consumer -- see
 * publisher-main.ts/consumer-main.ts). Clean disabled mode, same
 * contract as startTelemetry: RELAYLAB_OTEL_DISABLED=1 is a complete
 * no-op, and every instrument accessor below then resolves against the
 * API's own no-op meter (observations/increments become harmless no-ops;
 * application code never branches on whether metrics are enabled).
 */
export function startMetrics(serviceName: string, port: number): void {
  if (state === "running") return;
  if (state === "shutting_down") {
    console.error("[metrics] startMetrics() called while a previous shutdown is still in flight; ignoring.");
    return;
  }
  if (!isMetricsEnabled()) {
    state = "running";
    return;
  }

  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: serviceName,
    [ATTR_SERVICE_VERSION]: SERVICE_VERSION,
    [ATTR_DEPLOYMENT_ENVIRONMENT_NAME]: process.env.RELAYLAB_ENV ?? "dev",
  });

  // PrometheusExporter IS a MetricReader (pull model: it runs its own
  // small HTTP server and answers a GET with the current state of every
  // registered instrument on demand) -- there is no periodic "push"
  // network call here, unlike the OTLP trace exporter. See
  // docs/METRICS.md "how shutdown and monitoring outages behave".
  exporter = new PrometheusExporter({ port, host: "0.0.0.0" });
  provider = new MeterProvider({ resource, readers: [exporter] });
  metrics.setGlobalMeterProvider(provider);
  state = "running";
  console.log(`[metrics] started for "${serviceName}", serving Prometheus scrapes on :${port}/metrics`);
}

export interface MetricsShutdownOutcome {
  completedWithinBound: boolean;
}

/** Same bounded-shutdown shape as shutdownTelemetry (src/telemetry/init.ts) -- see that file for the detailed rationale this mirrors. */
export async function shutdownMetrics(timeoutMs = 5000): Promise<MetricsShutdownOutcome> {
  if (state === "idle" || state === "shutdown") return { completedWithinBound: true };
  state = "shutting_down";
  const target = provider;
  provider = undefined;
  exporter = undefined;

  const real = (async () => {
    try {
      if (target) await target.shutdown();
    } catch (err) {
      console.error("[metrics] shutdown error:", err instanceof Error ? err.constructor.name : "UnknownError");
    } finally {
      state = "shutdown";
    }
  })();

  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      real.then(() => ({ completedWithinBound: true })),
      new Promise<MetricsShutdownOutcome>((resolve) => {
        timer = setTimeout(() => resolve({ completedWithinBound: false }), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function meter(): Meter {
  return metrics.getMeter("relaylab");
}

// Lazily created, cached on first actual use (see file header comment
// for why this must never happen at module load time). One cache slot
// per instrument -- cheap, and avoids re-registering the same
// name/descriptor with the SDK on every single call on a hot path.
const counterCache = new Map<string, Counter>();
function counter(name: string, description: string, unit?: string): Counter {
  let c = counterCache.get(name);
  if (!c) {
    c = meter().createCounter(name, unit !== undefined ? { description, unit } : { description });
    counterCache.set(name, c);
  }
  return c;
}

const histogramCache = new Map<string, Histogram>();
function histogram(name: string, description: string): Histogram {
  let h = histogramCache.get(name);
  if (!h) {
    h = meter().createHistogram(name, {
      description,
      unit: "s",
      // Second-scale buckets appropriate for an in-process DB/NATS
      // round trip (single-digit milliseconds to a few seconds) --
      // NOT the OTel SDK's generic default buckets, which are tuned for
      // a different (HTTP-latency-shaped) default use case.
      advice: { explicitBucketBoundaries: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10] },
    });
    histogramCache.set(name, h);
  }
  return h;
}

// Phase 1.2.1 monitoring-reliability patch, section 2: "rejected" is
// ONLY an expected domain rejection (EpochStale/ClaimLost/NotAuthorized/
// CommandConflict/CommandRejected/TaskNotFound -- the SAME finite set
// `isExpectedDomainRejection` already uses for span-status policy, see
// docs/OBSERVABILITY.md). A DB timeout, connection drop, or any other
// unclassified throw is now its OWN separate outcome,
// `infrastructure_failure` -- it is no longer folded into "rejected"
// (which, before this patch, meant simply "the operation threw for ANY
// reason," making a plain `sum by (outcome) (...)` query unable to tell
// "the system is correctly fencing out-of-date commands" apart from
// "the database is unreachable" without separately filtering on the
// `reason` label). See docs/METRICS.md "Final metric outcome
// definitions".
export type CommandOutcome = "committed" | "replayed" | "rejected" | "infrastructure_failure";
export type OwnershipOperation = "claim" | "renew";
export type OwnershipOutcome = "acquired" | "renewed" | "rejected" | "infrastructure_failure";
export type BoundedRejectReason =
  | "n/a"
  | "EpochStale"
  | "ClaimLost"
  | "NotAuthorized"
  | "CommandConflict"
  | "CommandRejected"
  | "TaskNotFound"
  | "InfrastructureError";
export type BoundedCommandType = "Checkpoint" | "SubmitCandidate" | "RecordVerification" | "Complete" | "unknown";
export type PublishOutcome = "published" | "failed";
export type ConsumerOutcome = "applied" | "deduped" | "buffered" | "conflict" | "dead_lettered";
export type QuarantineReasonLabel = "malformed_json" | "schema_invalid" | "max_redeliveries_exceeded";
export type QuarantineResult = "recorded" | "write_failed";
export type GaugeGroup = "outbox" | "gaps" | "quarantine";

export function recordCommandAttempt(outcome: CommandOutcome, reason: BoundedRejectReason, commandType: BoundedCommandType, durationSeconds: number): void {
  counter("relaylab_command_attempts_total", "Command processing attempts by bounded outcome").add(1, { outcome, reason, command_type: commandType });
  histogram("relaylab_command_processing_duration_seconds", "Duration of applyTaskCommand calls").record(durationSeconds, { outcome });
}

export function recordOwnershipAttempt(operation: OwnershipOperation, outcome: OwnershipOutcome, reason: BoundedRejectReason): void {
  counter("relaylab_ownership_attempts_total", "Ownership claim/renew attempts by bounded outcome/reason").add(1, { operation, outcome, reason });
}

export function recordPublishAttempt(outcome: PublishOutcome, durationSeconds: number): void {
  counter("relaylab_publish_attempts_total", "Outbox publish attempts by success/failure").add(1, { outcome });
  histogram("relaylab_publish_attempt_duration_seconds", "Duration of one outbox row's publish attempt").record(durationSeconds, { outcome });
}

export function recordConsumerAttempt(outcome: ConsumerOutcome, durationSeconds: number): void {
  counter("relaylab_consumer_attempts_total", "Consumer processing attempts by outcome").add(1, { outcome });
  histogram("relaylab_consumer_processing_duration_seconds", "Duration of one message's processing attempt").record(durationSeconds, { outcome });
}

export function recordQuarantineAttempt(reason: QuarantineReasonLabel, result: QuarantineResult): void {
  counter("relaylab_quarantine_attempts_total", "Durable quarantine write attempts").add(1, { reason, result });
}

export function recordGaugeRefreshFailure(group: GaugeGroup): void {
  counter("relaylab_gauge_refresh_failures_total", "Durable-gauge refresh failures").add(1, { group });
}

// Observable gauges: registered ONCE (cached), with a callback that reads
// a CACHED value supplied by the caller -- see src/telemetry/durable-gauges.ts,
// which owns the actual periodic DB refresh and supplies these callbacks.
// The callback itself must never perform I/O (see docs/METRICS.md "do not
// rescan on every scrape").
// Cached per NAME, not per (name + callback): an ObservableGauge
// instrument can carry MULTIPLE callbacks (e.g. one gauge name shared by
// several durable-gauge groups, each contributing its own `group` label
// via its own callback) -- callers add their own callback via
// `.addCallback()` on the returned instrument, and this function's job
// is only to ensure the SAME underlying instrument is reused for a given
// name rather than re-registered with the SDK on every call.
const observableGaugeCache = new Map<string, ObservableGauge>();
export function getObservableGauge(name: string, description: string, unit?: string): ObservableGauge {
  let g = observableGaugeCache.get(name);
  if (!g) {
    g = meter().createObservableGauge(name, unit !== undefined ? { description, unit } : { description });
    observableGaugeCache.set(name, g);
  }
  return g;
}

export type { ObservableResult } from "@opentelemetry/api";
