// See the identical comment in publisher-main.ts.
import { startTelemetry, shutdownTelemetry, classifyShutdownError } from "../telemetry/init.js";
startTelemetry("relaylab-consumer");

import { startMetrics, shutdownMetrics } from "../telemetry/metrics.js";
startMetrics("relaylab-consumer", Number(process.env.RELAYLAB_METRICS_PORT ?? 9465));

import { createPool } from "../db/pool.js";
import { loadMysqlConfig, loadNatsConfig } from "../config.js";
import { connectNats, ensureEventsStream, ensureDurableConsumer, watchConnectionState, type StreamIdentity } from "./nats-client.js";
import { defaultConsumerOptions, processBatch } from "./consumer.js";
import { reportLingeringHandlesAfter } from "../process/natural-exit.js";
import { startConsumerGaugeRefresher } from "../telemetry/durable-gauges.js";
import { getObservableGauge, type ObservableResult } from "../telemetry/metrics.js";
import { startHealthServer, startDependencyPing } from "../telemetry/health.js";
import { tryAcquireOrRenewExclusiveLock } from "./publisher-lock.js";

const MAX_BACKOFF_MS = 5000;
const STEADY_STATE_DELAY_MS = 0; // processBatch's own `expires` already paces polling
const INSTANCE_ID = `consumer-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
// Phase 3A.1/3A.2: Kubernetes' replicas:1 + the Deployments' Recreate
// strategy (see infra/k8s/manifests/50-consumer.yaml) only protect a
// NORMAL rollout -- they do nothing against a manual second Pod, a
// forced/SIGKILL-style termination racing a replacement, or someone
// setting replicas:2 by hand. This lock reuses the EXACT same
// DB-backed epoch+lease mechanism as the publisher's leader lock
// (src/messaging/publisher-lock.ts), under its own `lock_name` so the
// two locks never interact.
//
// PRECISE GUARANTEE (corrected in Phase 3A.2 -- the comment this
// replaces overclaimed it): the lease is checked ONCE per poll
// iteration, BEFORE calling processBatch, and its epoch is never
// threaded into processBatch itself. This coordinates the NORMAL case
// well -- a standby instance that fails to acquire/renew simply never
// starts a new iteration, which is what stops two instances from
// routinely competing for work. It does NOT bound how long an
// already-authorized processBatch call may run: if one call happens to
// outlive CONSUMER_LOCK_LEASE_MS (slow NATS fetch, slow DB work, GC
// pause), this process's NEXT lock check can fail and a different
// instance can legitimately take over and start its OWN iteration while
// the first call is still finishing. This is a real, understood,
// accepted gap for this milestone, not a claim of universal "exactly
// one active process at every instant" -- deterministically proven
// in tests/integration/consumer-lease-overlap.test.ts, which runs two
// genuinely concurrent applyEventWithDedup calls for the SAME event and
// confirms the result is still exactly one durable application. THAT
// correctness -- no duplicate committed effect, regardless of overlap
// -- comes from consumer.ts's own transactional row locking and
// per-event_id dedup (see applyEventWithDedup), not from this lease.
// See docs/KUBERNETES.md "Consumer exclusivity: what the lease actually
// guarantees" for the full writeup.
const CONSUMER_LOCK_NAME = "consumer-default";
const CONSUMER_LOCK_LEASE_MS = 3000;

async function main() {
  const pool = createPool(loadMysqlConfig());
  // `nc`/`js`/`jsm` are reassigned on an application-level reconnect
  // (see the "beyond budget" recovery policy below, and the identical
  // rationale documented in publisher-main.ts / nats-client.ts) -- `let`.
  let { nc, js, jsm } = await connectNats(loadNatsConfig().servers);
  let streamIdentity: StreamIdentity = await ensureEventsStream(jsm);
  await ensureDurableConsumer(jsm);
  let connectionState = watchConnectionState(nc);
  let stopping = false;
  // Phase 3A.1: see the identical rationale in publisher-main.ts.
  const dbPing = startDependencyPing(async () => {
    await pool.query("SELECT 1");
  });
  // Phase 3A Part B: see the identical rationale in publisher-main.ts and
  // src/telemetry/health.ts. "Started" follows the initial connect +
  // ensureDurableConsumer sequence this line comes after. Readiness
  // (Phase 3A.1) also reflects MySQL reachability and goes false the
  // instant shutdown begins.
  // Phase 3A.1 regression fix: see the identical rationale in
  // publisher-main.ts -- defaults to an OS-assigned ephemeral port
  // unless RELAYLAB_HEALTH_PORT is explicitly set.
  const health = startHealthServer(Number(process.env.RELAYLAB_HEALTH_PORT ?? 0), {
    isReady: () => !stopping && connectionState.isConnected() && dbPing.isUp(),
  });
  health.markStarted();

  const options = {
    ...defaultConsumerOptions(),
    // Real stream identity (name + creation timestamp), not the
    // defaults -- see src/messaging/quarantine.ts for why this matters:
    // it is part of the quarantine dedup key specifically so a sequence
    // number from a PRIOR, deleted-and-recreated stream incarnation can
    // never collide with one from the current incarnation.
    streamName: streamIdentity.name,
    streamGeneration: streamIdentity.generation,
    demoPerMessageDelayMs: Number(process.env.RELAYLAB_DEMO_SLOWDOWN_MS ?? 0),
    ...(process.env.RELAYLAB_DEMO_BATCH_SIZE ? { batchSize: Number(process.env.RELAYLAB_DEMO_BATCH_SIZE) } : {}),
  };
  console.log(`[consumer] started as "${options.consumerName}"`);

  // Owned by the consumer process -- see docs/METRICS.md "Gauge
  // ownership". `INSTANCE_ID` is heartbeated by the refresher itself so
  // an unsupported SECOND instance for the same consumer_name is
  // visibly detectable via relaylab_consumer_gauge_exporter_count,
  // rather than both instances silently exposing duplicate series.
  const gapGaugeRefresher = startConsumerGaugeRefresher(pool, options.consumerName, INSTANCE_ID);

  // Tracked so shutdown waits for whatever batch is currently in flight
  // before draining/closing connections out from under it (section 7:
  // "new work is not fetched indefinitely after shutdown starts" /
  // "in-flight processing has a defined completion policy" -- the policy
  // here is "let the current batch finish, then stop", not cancellation
  // mid-message).
  let currentWork: Promise<unknown> = Promise.resolve();

  const stop = async () => {
    if (stopping) return;
    stopping = true;
    console.log("[consumer] shutdown requested, waiting for in-flight batch to finish");
    await currentWork.catch(() => {});
    console.log("[consumer] shutting down: draining NATS, stopping gauge refresher, closing MySQL pool");
    // See the identical comment and rationale in publisher-main.ts:
    // an already (expectedly) closed connection skips drain() entirely
    // rather than calling it and swallowing whatever it throws.
    if (nc.isClosed()) {
      console.log("[consumer] NATS connection already closed (reconnect budget was exhausted) -- skipping drain(), nothing to release there");
    } else {
      await nc.drain().catch((err) => {
        console.error(`[consumer] nc.drain() failed unexpectedly during shutdown: ${classifyShutdownError(err)}`);
      });
    }
    // Stopped and AWAITED before pool.end() -- see the identical
    // reordering/rationale in publisher-main.ts.
    await gapGaugeRefresher.stop();
    await pool.end();
    // See the identical comment in publisher-main.ts: `completedWithinBound`
    // is honestly reported, not silently ignored.
    const { completedWithinBound } = await shutdownTelemetry();
    if (!completedWithinBound) {
      console.error("[consumer] telemetry shutdown did not complete within its bound; continuing shutdown anyway, some spans may be lost");
    }
    const metricsShutdown = await shutdownMetrics();
    if (!metricsShutdown.completedWithinBound) {
      console.error("[consumer] metrics shutdown did not complete within its bound; continuing shutdown anyway");
    }
    dbPing.stop();
    await health.stop();
    // Deliberately NO process.exit() -- see the identical comment and
    // rationale in publisher-main.ts (Phase 1.2.1 Part A.1 fix).
    process.exitCode = 0;
    // See the identical comment in publisher-main.ts: closing the IPC
    // channel (only ever open when this process was fork()ed, as the
    // test harness does) is part of releasing every resource this
    // process opened -- a no-op in ordinary production deployment.
    if (process.connected) process.disconnect();
    console.log("[consumer] graceful shutdown steps complete; exiting naturally once the event loop drains");
    reportLingeringHandlesAfter("consumer");
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  // See the identical comment in publisher-main.ts: on Windows,
  // child.kill("SIGTERM") does not deliver a catchable signal, so this IPC
  // path is what tests/integration/shutdown.test.ts uses to exercise the
  // real graceful-shutdown code path above on this dev machine. Inert in
  // ordinary production deployment (no IPC channel).
  process.on("message", (msg) => {
    if (msg === "shutdown") void stop();
  });

  let consecutiveFailures = 0;
  // See the identical rationale in publisher-main.ts: a healthy
  // /metrics scrape does NOT prove this process can reach NATS.
  getObservableGauge(
    "relaylab_nats_connected",
    "Whether this process currently has a live NATS connection (1) or not (0) -- a healthy /metrics scrape alone does NOT imply this",
  ).addCallback((result: ObservableResult) => result.observe(connectionState.isConnected() ? 1 : 0));

  // Phase 3A.1: mirrors relaylab_publisher_leadership_state exactly --
  // whether THIS instance currently holds the consumer exclusivity lock.
  // In normal single-instance operation this is always 1 (acquiring it
  // is uncontended); it only ever reads 0 if a second instance is
  // somehow also running, which is exactly the condition this gauge
  // exists to make visible.
  let holdsExclusiveLock = false;
  getObservableGauge(
    "relaylab_consumer_exclusivity_state",
    "Whether this consumer instance currently holds the single-instance exclusivity lock (1) or is locked out by another instance (0)",
  ).addCallback((result: ObservableResult) => result.observe(holdsExclusiveLock ? 1 : 0));

  while (!stopping) {
    const iteration = (async () => {
      try {
        const lock = await tryAcquireOrRenewExclusiveLock(pool, INSTANCE_ID, CONSUMER_LOCK_LEASE_MS, CONSUMER_LOCK_NAME);
        if (!lock.acquired) {
          if (holdsExclusiveLock) console.error("[consumer] LOST the consumer exclusivity lock -- another instance now holds it. Pausing processing until it is reacquired.");
          holdsExclusiveLock = false;
          // Deliberately does NOT call processBatch this iteration --
          // see the file header comment: this is what actually stops a
          // second instance from pulling/applying work, independent of
          // Kubernetes' own replicas:1.
          return;
        }
        if (!holdsExclusiveLock) console.log(`[consumer] acquired the consumer exclusivity lock at epoch ${lock.epoch}`);
        holdsExclusiveLock = true;

        // "Beyond budget" recovery policy -- see the identical, fully
        // documented rationale in publisher-main.ts and
        // nats-client.ts's header comment (the real diagnosed root
        // cause). `ensureDurableConsumer` re-run here is what makes
        // this safe: the durable consumer's delivery position lives on
        // the NATS SERVER, never reset by a client-side reconnect.
        if (nc.isClosed()) {
          console.error("[consumer] NATS connection permanently closed (reconnect budget exhausted) -- attempting application-level reconnect");
          const fresh = await connectNats(loadNatsConfig().servers);
          nc = fresh.nc;
          js = fresh.js;
          jsm = fresh.jsm;
          streamIdentity = await ensureEventsStream(jsm);
          await ensureDurableConsumer(jsm);
          options.streamName = streamIdentity.name;
          options.streamGeneration = streamIdentity.generation;
          connectionState = watchConnectionState(nc);
          console.log("[consumer] application-level NATS reconnect succeeded");
        }

        const outcome = await processBatch(pool, js, options);
        if (outcome.processed || outcome.deduped || outcome.deadLettered || outcome.buffered || outcome.conflicts) {
          console.log(
            `[consumer] processed=${outcome.processed} deduped=${outcome.deduped} ` +
              `deadLettered=${outcome.deadLettered} buffered=${outcome.buffered} conflicts=${outcome.conflicts}`,
          );
        }
        consecutiveFailures = 0;
      } catch (err) {
        consecutiveFailures++;
        // Phase 1.2.1 Part A.3: see the identical comment in publisher-main.ts.
        console.error(`[consumer] batch failed: ${classifyShutdownError(err)}`);
      }
    })();
    currentWork = iteration;
    await iteration;

    if (stopping) break;
    if (consecutiveFailures > 0) {
      const delay = Math.min(MAX_BACKOFF_MS, 200 * 2 ** consecutiveFailures);
      await new Promise((r) => setTimeout(r, delay));
    } else if (STEADY_STATE_DELAY_MS > 0) {
      await new Promise((r) => setTimeout(r, STEADY_STATE_DELAY_MS));
    }
  }
}

main().catch((err) => {
  console.error(`[consumer] fatal: ${classifyShutdownError(err)}`);
  process.exitCode = 1;
});
