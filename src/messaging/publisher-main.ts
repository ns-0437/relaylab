// Telemetry is started FIRST, before any other application import runs
// its own code -- see src/telemetry/init.ts for why this matters less
// here than it would with auto-instrumentation (this project uses only
// explicit spans), and why it is still done this way as the safe default.
import { startTelemetry, shutdownTelemetry, classifyShutdownError } from "../telemetry/init.js";
startTelemetry("relaylab-publisher");

import { startMetrics, shutdownMetrics, getObservableGauge, type ObservableResult } from "../telemetry/metrics.js";
startMetrics("relaylab-publisher", Number(process.env.RELAYLAB_METRICS_PORT ?? 9464));

import { createPool } from "../db/pool.js";
import { loadMysqlConfig, loadNatsConfig } from "../config.js";
import { connectNats, ensureEventsStream, watchConnectionState } from "./nats-client.js";
import { DEFAULT_PUBLISHER_OPTIONS, runPublisherOnce } from "./publisher.js";
import { tryAcquireOrRenewPublisherLeadership } from "./publisher-lock.js";
import { reportLingeringHandlesAfter } from "../process/natural-exit.js";
import { startOutboxGaugeRefresher, type Refresher } from "../telemetry/durable-gauges.js";
import { startHealthServer, startDependencyPing } from "../telemetry/health.js";

const OWNER_ID = `publisher-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
const LEADER_LEASE_MS = 3000;
const MAX_BACKOFF_MS = 5000;

async function main() {
  const pool = createPool(loadMysqlConfig());
  // `nc`/`js` are reassigned on an application-level reconnect (see the
  // "beyond budget" recovery policy below) -- `let`, not `const`.
  let { nc, js, jsm } = await connectNats(loadNatsConfig().servers);
  await ensureEventsStream(jsm);
  let connectionState = watchConnectionState(nc);
  let stopping = false;
  // Phase 3A.1: bounded, periodic, CACHED MySQL reachability -- never
  // queried synchronously from inside a probe handler (see
  // src/telemetry/health.ts's DependencyPing doc comment for why).
  const dbPing = startDependencyPing(async () => {
    await pool.query("SELECT 1");
  });
  // Phase 3A Part B: startup/readiness/liveness are kept distinct here
  // (see src/telemetry/health.ts). "Started" is marked only once, right
  // after the initial connect sequence this line follows already
  // succeeded -- it is never unmarked, unlike readiness below. Readiness
  // (Phase 3A.1) now also reflects MySQL reachability and goes false the
  // instant shutdown begins (`!stopping`), independent of when the
  // server itself actually closes.
  // Phase 3A.1 regression fix: defaults to an OS-assigned EPHEMERAL port
  // (0), never a fixed one, unless RELAYLAB_HEALTH_PORT is explicitly
  // set -- the Kubernetes ConfigMap always sets it to 9470 explicitly
  // (each pod has its own network namespace, so no collision there),
  // but a fixed default previously crashed this process outright
  // (unhandled EADDRINUSE) whenever this project's OWN test suite
  // spawned two real publisher processes on the SAME host, as several
  // existing tests deliberately do. See src/telemetry/health.ts's
  // 'error' handler for the second half of this fix.
  const health = startHealthServer(Number(process.env.RELAYLAB_HEALTH_PORT ?? 0), {
    isReady: () => !stopping && connectionState.isConnected() && dbPing.isUp(),
  });
  health.markStarted();

  console.log(`[publisher] started as "${OWNER_ID}"`);
  // Tracked so shutdown can wait for whatever is currently in flight
  // before releasing the pool/NATS connection out from under it, instead
  // of racing a SIGTERM/SIGINT against an in-progress batch.
  let currentWork: Promise<unknown> = Promise.resolve();

  const stop = async () => {
    if (stopping) return;
    stopping = true;
    console.log("[publisher] shutdown requested, waiting for in-flight work to finish");
    await currentWork.catch(() => {});
    console.log("[publisher] shutting down: draining NATS, stopping gauge refresher, closing MySQL pool");
    // A connection that survived a broker outage and exhausted its
    // reconnect budget is ALREADY, EXPECTEDLY closed (`nc.isClosed() ===
    // true`, confirmed via real diagnostic -- see nats-client.ts's
    // header comment) -- `drain()` on it throws `NatsError
    // CONNECTION_CLOSED` for a reason this code already knows about and
    // has nothing further to do for (there is no connection left to
    // drain). That expected case is skipped WITHOUT calling drain() at
    // all (never "blanket-catch and report success" -- this is a
    // positive check of a known state, not a swallowed failure). Any
    // OTHER drain() failure (the connection was still open/connecting
    // and drain() failed for a genuinely unexpected reason) is logged
    // loudly, not silently treated as a success.
    if (nc.isClosed()) {
      console.log("[publisher] NATS connection already closed (reconnect budget was exhausted) -- skipping drain(), nothing to release there");
    } else {
      await nc.drain().catch((err) => {
        console.error(`[publisher] nc.drain() failed unexpectedly during shutdown: ${classifyShutdownError(err)}`);
      });
    }
    // Stopped BEFORE pool.end() -- awaits any in-flight durable-gauge
    // query so nothing is still using `pool` when it is closed below
    // (an un-awaited stop() here previously ran AFTER pool.end(), which
    // could have left a refresh query racing a closed pool).
    await outboxGaugeRefresher?.stop();
    await pool.end();
    // Flushes any buffered spans, bounded by its own internal timeout --
    // see src/telemetry/init.ts. A hard kill (no chance to reach this
    // line) loses whatever is still buffered; that is a stated, accepted
    // limitation (docs/OBSERVABILITY.md), not a correctness issue, since
    // no business outcome depends on it. `completedWithinBound: false`
    // means the bound elapsed before the real flush finished -- NOT that
    // nothing was exported, and NOT that it is now safe to assume
    // completion. Logged honestly, not silently swallowed.
    const { completedWithinBound } = await shutdownTelemetry();
    if (!completedWithinBound) {
      console.error("[publisher] telemetry shutdown did not complete within its bound; continuing shutdown anyway, some spans may be lost");
    }
    const metricsShutdown = await shutdownMetrics();
    if (!metricsShutdown.completedWithinBound) {
      console.error("[publisher] metrics shutdown did not complete within its bound; continuing shutdown anyway");
    }
    dbPing.stop();
    await health.stop();
    // Deliberately NO process.exit() call here (Phase 1.2.1 Part A.1 fix
    // -- see docs/OBSERVABILITY.md "graceful shutdown"). Every resource
    // this process opened (NATS connection, MySQL pool, OTel exporter)
    // has now been explicitly closed above; the ONLY way this process
    // can still be alive after this point is if one of those did not
    // actually release everything it held, which is exactly the thing a
    // forced exit here would have hidden. `exitCode` is set so that if
    // nothing else is keeping the event loop alive, Node exits on its
    // own with a clean, explicit status -- this is what makes "the
    // process exited" here provable as NATURAL exit, not a masked one.
    // The externally enforced hard-stop deadline (if this somehow does
    // NOT drain naturally) belongs to whatever supervises this process
    // (see tests/integration/process-harness.ts's bounded SIGKILL), not
    // to this function pretending to guarantee it from the inside.
    process.exitCode = 0;
    // A forked child process (the ONLY way an IPC channel exists here --
    // see the "message" listener below and docs/decisions/0009) keeps an
    // open IPC channel that itself counts as an active handle and would
    // otherwise keep the event loop alive forever, masking whether every
    // OTHER resource was actually released. Closing it is part of "close
    // every resource this process opened" -- ordinary production
    // deployment never forks this process with an IPC channel, so
    // `process.connected` is simply false there and this is a no-op.
    if (process.connected) process.disconnect();
    console.log("[publisher] graceful shutdown steps complete; exiting naturally once the event loop drains");
    reportLingeringHandlesAfter("publisher");
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  // On Windows, child_process.kill(anySignal) does not deliver a
  // catchable signal -- Node's own docs note Windows has no POSIX signal
  // delivery, and in practice (verified empirically in this project's own
  // test suite, tests/integration/shutdown.test.ts) child.kill("SIGTERM")
  // forcibly terminates the process there, bypassing the handlers above
  // entirely. This IPC listener exists ONLY so that test harness can still
  // exercise the real graceful-shutdown code path above on a
  // non-POSIX dev machine; it does nothing unless something sends this
  // exact message over an IPC channel, which requires the process to have
  // been `fork()`ed (ordinary production deployment does not fork this
  // process with IPC, so this is inert there).
  process.on("message", (msg) => {
    if (msg === "shutdown") void stop();
  });

  let consecutiveFailures = 0;
  let wasLeader = false;
  // Phase 1.2.1 Part C: instance-local operational-health gauge -- see
  // docs/METRICS.md's dictionary entry. Registered ONCE; its callback
  // reads whatever `wasLeader` holds at scrape time.
  getObservableGauge(
    "relaylab_publisher_leadership_state",
    "Whether this publisher instance currently believes it holds the leader lock (1) or is standby (0)",
  ).addCallback((result: ObservableResult) => result.observe(wasLeader ? 1 : 0));
  // Section 5's explicit requirement: a healthy /metrics scrape does NOT
  // prove this process can reach NATS -- the metrics HTTP server starts
  // BEFORE connectNats ever runs (see the top of this file). This gauge
  // reflects the REAL, current connection state (event-driven via
  // nc.status(), not polled) -- `0` here plus a healthy scrape is
  // exactly "process alive, NOT able to process work," the distinction
  // section 5 asked for.
  getObservableGauge(
    "relaylab_nats_connected",
    "Whether this process currently has a live NATS connection (1) or not (0) -- a healthy /metrics scrape alone does NOT imply this",
  ).addCallback((result: ObservableResult) => result.observe(connectionState.isConnected() ? 1 : 0));
  // Outbox backlog gauges are DURABLE, GLOBAL state with exactly ONE
  // owning exposer at a time -- see docs/METRICS.md "Gauge ownership":
  // only the current LEADER runs this refresher; a standby publisher
  // neither queries nor exposes these series, so they are never
  // duplicated/summed across instances.
  let outboxGaugeRefresher: Refresher | undefined;

  while (!stopping) {
    const iteration = (async () => {
      try {
        // "Beyond budget" recovery policy (see nats-client.ts's header
        // comment for the diagnosed root cause, and
        // docs/OBSERVABILITY.md "NATS reconnect and recovery contract"
        // for the full documented contract): once nats.js's OWN
        // reconnect budget is exhausted, `nc` is PERMANENTLY closed and
        // nothing brings it back on its own. This process does not
        // exit -- it recreates the connection itself, with the SAME
        // bounded per-attempt backoff already used for every other
        // iteration failure below (never a second, separate unbounded
        // retry mechanism). `ensureEventsStream` is idempotent and safe
        // to re-run; it does not reset any server-side state. Checked
        // ONCE per iteration, inside the single sequential `while`
        // loop, so a reconnect can never race a concurrent one.
        if (nc.isClosed()) {
          console.error("[publisher] NATS connection permanently closed (reconnect budget exhausted) -- attempting application-level reconnect");
          const fresh = await connectNats(loadNatsConfig().servers);
          nc = fresh.nc;
          js = fresh.js;
          jsm = fresh.jsm;
          await ensureEventsStream(jsm);
          connectionState = watchConnectionState(nc);
          console.log("[publisher] application-level NATS reconnect succeeded");
        }

        const { acquired } = await tryAcquireOrRenewPublisherLeadership(pool, OWNER_ID, LEADER_LEASE_MS);
        if (!acquired) {
          if (wasLeader) console.log("[publisher] lost leadership, pausing");
          wasLeader = false;
          // Stopped and AWAITED (not fire-and-forget) the instant
          // leadership is lost -- see docs/METRICS.md "gauge ownership":
          // a standby instance must never still be mid-query or still
          // exposing outbox gauge values. The refresher reference is
          // discarded entirely (not reused) so that IF this instance
          // later re-acquires leadership, `startOutboxGaugeRefresher`
          // below constructs a BRAND NEW refresher with a BRAND NEW,
          // empty cache -- a stale value cached from the PREVIOUS
          // leadership period can never leak forward into the next one.
          await outboxGaugeRefresher?.stop();
          outboxGaugeRefresher = undefined;
          return;
        }
        if (!wasLeader) console.log("[publisher] acquired leadership");
        wasLeader = true;
        if (!outboxGaugeRefresher) outboxGaugeRefresher = startOutboxGaugeRefresher(pool);

        const n = await runPublisherOnce(pool, js, DEFAULT_PUBLISHER_OPTIONS);
        if (n > 0) console.log(`[publisher] published ${n} event(s)`);
        consecutiveFailures = 0;
      } catch (err) {
        consecutiveFailures++;
        // Phase 1.2.1 Part A.3: a raw `err.message` (or the error object
        // itself) is NEVER logged here -- a DB/NATS failure's message can
        // embed connection details. `classifyShutdownError` returns only
        // a constructor name, optionally with a known-safe `.code` (see
        // src/telemetry/init.ts).
        console.error(`[publisher] iteration failed: ${classifyShutdownError(err)}`);
      }
    })();
    currentWork = iteration;
    await iteration;

    if (stopping) break;
    // Bounded exponential backoff on repeated failures (DB/NATS
    // connectivity issues), separate from the steady-state poll interval.
    // publishOne's own maxRetries already bounds a single publish attempt;
    // this bounds the OUTER loop's reaction to sustained failure instead
    // of hot-looping at pollIntervalMs.
    const delay =
      consecutiveFailures > 0
        ? Math.min(MAX_BACKOFF_MS, DEFAULT_PUBLISHER_OPTIONS.pollIntervalMs * 2 ** consecutiveFailures)
        : DEFAULT_PUBLISHER_OPTIONS.pollIntervalMs;
    await new Promise((r) => setTimeout(r, delay));
  }
}

main().catch((err) => {
  console.error(`[publisher] fatal: ${classifyShutdownError(err)}`);
  process.exitCode = 1;
});
