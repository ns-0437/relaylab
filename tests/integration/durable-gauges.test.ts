// Phase 1.2.1 monitoring-reliability patch, section 1: overlapping
// durable-gauge refresh prevention. The ORIGINAL implementation used
// `setInterval(refresh, intervalMs)` racing an async `refresh()` --
// these tests drive the rewritten completion-based scheduler
// (`makeRefresherForTesting`, src/telemetry/durable-gauges.ts) with
// injected, controllable query functions (one of which uses a REAL
// `SELECT SLEEP(...)` against the real test database, so the "bounded
// query / real driver behavior" claim is exercised against the real
// mysql2 driver, not a mock).
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import { startMetrics, shutdownMetrics } from "../../src/telemetry/metrics.js";
import { makeRefresherForTesting, startConsumerGaugeRefresher } from "../../src/telemetry/durable-gauges.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";

const METRICS_PORT = 19601;
let pool: mysql.Pool;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
  delete process.env.RELAYLAB_OTEL_DISABLED;
  startMetrics("relaylab-durable-gauges-test", METRICS_PORT);
});

after(async () => {
  await pool.end();
  await shutdownMetrics();
});

async function scrape(): Promise<string> {
  const res = await fetch(`http://127.0.0.1:${METRICS_PORT}/metrics`);
  return res.text();
}

test(
  "1. a deliberately delayed REAL query (SELECT SLEEP) that lasts longer than the refresh interval never causes more than one in-flight refresh",
  { timeout: 15_000 },
  async () => {
    let callCount = 0;
    const refresher = makeRefresherForTesting(
      "outbox", // reusing an existing GaugeGroup label for this test
      pool,
      150, // refresh interval shorter than the query itself
      5000,
      async (p) => {
        callCount++;
        // A REAL query against the real test database that takes
        // ~400ms -- genuinely longer than the 150ms refresh interval
        // above, so if the scheduler were still interval-based (racing
        // setInterval against an in-flight promise), this would
        // reliably produce overlapping calls.
        await p.query("SELECT SLEEP(0.4) AS x");
        return { test_gauge_delayed: callCount };
      },
      [{ name: "relaylab_test_gauge_delayed", description: "test-only gauge" }],
    );
    try {
      await new Promise((r) => setTimeout(r, 1700));
      assert.equal(refresher.getMaxConcurrent(), 1, `expected at most one in-flight refresh at any time, saw max concurrency ${refresher.getMaxConcurrent()}`);
      assert.ok(callCount >= 2, `expected multiple completed refreshes in 1.7s given ~400ms+150ms cycles, saw ${callCount}`);
    } finally {
      await refresher.stop();
    }
  },
);

test("2. stopping during an in-flight refresh schedules no additional work, and stop() waits for it", async () => {
  let callCount = 0;
  let resolveQuery: (() => void) | undefined;
  const refresher = makeRefresherForTesting(
    "gaps",
    pool,
    50, // very short interval -- would schedule aggressively if not for the in-flight guard
    5000,
    async () => {
      callCount++;
      await new Promise<void>((resolve) => {
        resolveQuery = resolve;
      });
      return { test_gauge_stop: callCount };
    },
    [{ name: "relaylab_test_gauge_stop", description: "test-only gauge" }],
  );

  // Wait until the first refresh has actually started (its promise is constructed, resolveQuery captured).
  await new Promise((r) => setTimeout(r, 100));
  assert.equal(callCount, 1, "exactly one refresh must have started");
  assert.equal(refresher.getState(), "refreshing");

  const stopPromise = refresher.stop();
  // stop() must not resolve until the in-flight refresh settles.
  let stopResolved = false;
  stopPromise.then(() => {
    stopResolved = true;
  });
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(stopResolved, false, "stop() must still be waiting on the in-flight refresh");

  resolveQuery!(); // let the in-flight query finish
  await stopPromise;
  assert.equal(refresher.getState(), "stopped");

  const countAtStop = callCount;
  await new Promise((r) => setTimeout(r, 300)); // well past several would-be 50ms intervals
  assert.equal(callCount, countAtStop, "no further refresh may run after stop() resolves");
});

test("3. a refresh failure preserves the last successful sample and marks freshness/failure honestly", async () => {
  let shouldFail = false;
  let callCount = 0;
  const refresher = makeRefresherForTesting(
    "quarantine",
    pool,
    150,
    5000,
    async () => {
      callCount++;
      if (shouldFail) throw new Error("simulated DB failure");
      return { relaylab_test_gauge_failure: 42 };
    },
    [{ name: "relaylab_test_gauge_failure", description: "test-only gauge" }],
  );
  try {
    await new Promise((r) => setTimeout(r, 250)); // let one successful refresh land
    let body = await scrape();
    assert.equal(gaugeValue(body, "relaylab_test_gauge_failure"), 42, "expected the first successful value (42) to be exposed");
    const failuresBefore = extractFailureCount(body, "quarantine");

    shouldFail = true;
    await new Promise((r) => setTimeout(r, 450)); // let at least one failing refresh happen

    body = await scrape();
    assert.equal(gaugeValue(body, "relaylab_test_gauge_failure"), 42, "the STALE last-successful value (42) must still be exposed, not reset");
    const failuresAfter = extractFailureCount(body, "quarantine");
    assert.ok(failuresAfter > failuresBefore, `expected relaylab_gauge_refresh_failures_total{group="quarantine"} to have increased (before=${failuresBefore}, after=${failuresAfter})`);
  } finally {
    await refresher.stop();
  }
});

test("4. an initial failure (never yet succeeded) is NOT displayed as a confirmed zero -- the series is simply absent", async () => {
  const refresher = makeRefresherForTesting(
    "outbox",
    pool,
    150,
    5000,
    async () => {
      throw new Error("always fails");
    },
    [{ name: "relaylab_test_gauge_never_succeeded", description: "test-only gauge" }],
  );
  try {
    await new Promise((r) => setTimeout(r, 400)); // let at least one failing attempt happen
    const body = await scrape();
    assert.ok(
      !body.includes("relaylab_test_gauge_never_succeeded{") && !/relaylab_test_gauge_never_succeeded\s/.test(body),
      "a gauge that has never successfully refreshed must have NO data point at all -- never a fabricated 0",
    );
  } finally {
    await refresher.stop();
  }
});

test("section 6: TWO consumer-gauge-refresher instances for the SAME consumer_name make the unsupported topology visible via relaylab_consumer_gauge_exporter_count, instead of silently duplicating series", async () => {
  await truncateAll(pool);
  const consumerName = uniqueId("dup-exporter-consumer");
  const instanceA = `instance-a-${process.pid}`;
  const instanceB = `instance-b-${process.pid}`;

  // ONE instance first -- the supported, single-exporter topology.
  const refresherA = startConsumerGaugeRefresher(pool, consumerName, instanceA, 300, 3000);
  try {
    await new Promise((r) => setTimeout(r, 600));
    let body = await scrape();
    assert.equal(
      gaugeValue(body, "relaylab_consumer_gauge_exporter_count"),
      1,
      "exactly one exporter must be visible with only one instance running",
    );

    // Now a SECOND instance for the SAME consumer_name -- the
    // UNSUPPORTED topology this section asks to make visible.
    const refresherB = startConsumerGaugeRefresher(pool, consumerName, instanceB, 300, 3000);
    try {
      await new Promise((r) => setTimeout(r, 600));
      body = await scrape();
      assert.equal(
        gaugeValue(body, "relaylab_consumer_gauge_exporter_count"),
        2,
        "both instances' heartbeats must be visible as 2 distinct exporters -- the unsupported topology is DETECTABLE, not hidden",
      );

      // Confirm it is visible via direct inspection too, not only the metric.
      const [rows] = await pool.query<any[]>(`SELECT instance_id FROM consumer_gauge_exporters WHERE consumer_name = ? ORDER BY instance_id`, [consumerName]);
      assert.deepEqual(
        rows.map((r) => r.instance_id).sort(),
        [instanceA, instanceB].sort(),
      );
    } finally {
      await refresherB.stop();
    }

    // Detection is based on RECENT heartbeats, not permanent
    // registration -- proven directly at the DATA level (the actual
    // mechanism the gauge reads from), since OpenTelemetry JS's
    // ObservableGauge API has no "remove this callback" method: once
    // registered, a callback keeps being invoked for the lifetime of
    // the MeterProvider, even after the refresher that registered it has
    // been stopped. That is a property of running TWO refresher
    // instances inside ONE test PROCESS sharing ONE metrics registry --
    // it does not reflect production, where each real consumer instance
    // is its own OS process with its OWN separate Prometheus endpoint
    // (no shared registry, no leftover callback possible). The
    // underlying DB-level staleness logic -- what actually matters for
    // detecting the unsupported topology in production, where each
    // instance's /metrics IS its own refresher's cache -- is verified
    // directly here instead of through this test harness's shared-process
    // exposition artifact.
    await new Promise((r) => setTimeout(r, 2500));
    const [debugRows] = await pool.query<any[]>(
      `SELECT instance_id, TIMESTAMPDIFF(MICROSECOND, last_heartbeat_at, NOW(3)) / 1000 AS age_ms FROM consumer_gauge_exporters WHERE consumer_name = ?`,
      [consumerName],
    );
    const bStale = debugRows.find((r) => r.instance_id === instanceB);
    assert.ok(bStale && bStale.age_ms > 900, `instance B's heartbeat must have aged past the 900ms staleness window; rows: ${JSON.stringify(debugRows)}`);
    const [freshCountRows] = await pool.query<any[]>(
      `SELECT COUNT(*) AS c FROM consumer_gauge_exporters WHERE consumer_name = ? AND last_heartbeat_at > NOW(3) - INTERVAL 900000 MICROSECOND`,
      [consumerName],
    );
    assert.equal(
      freshCountRows[0]?.c,
      1,
      "the SAME query the refresher itself runs must count only A as a recent exporter once B's heartbeat has gone stale",
    );
  } finally {
    await refresherA.stop();
  }
});

function gaugeValue(body: string, metricName: string): number | undefined {
  const line = body.split("\n").find((l) => l.startsWith(metricName));
  if (!line) return undefined;
  const parts = line.trim().split(/\s+/);
  return Number(parts[parts.length - 1]);
}

function extractFailureCount(body: string, group: string): number {
  const line = body.split("\n").find((l) => l.startsWith("relaylab_gauge_refresh_failures_total") && l.includes(`group="${group}"`));
  if (!line) return 0;
  const parts = line.trim().split(/\s+/);
  return Number(parts[parts.length - 1]);
}
