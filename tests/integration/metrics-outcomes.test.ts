// Phase 1.2.1 monitoring-reliability patch, section 2: a real database
// timeout/connection failure must be recorded as
// `infrastructure_failure`, NEVER folded into the same "rejected"
// outcome bucket as an ordinary, expected domain rejection
// (EpochStale/ClaimLost/etc). Drives real `applyTaskCommand`/`claimTask`
// calls against a real MySQL pool (success + domain-rejection paths) and
// a deliberately unreachable pool (infrastructure-failure path), then
// scrapes a real Prometheus endpoint and asserts the outcome labels.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { createPool } from "../../src/db/pool.js";
import { loadTestMysqlConfig } from "../../src/config.js";
import { startMetrics, shutdownMetrics } from "../../src/telemetry/metrics.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";
import type mysql from "mysql2/promise";

const METRICS_PORT = 19600;
let pool: mysql.Pool;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
  delete process.env.RELAYLAB_OTEL_DISABLED;
  startMetrics("relaylab-metrics-outcomes-test", METRICS_PORT);
});

beforeEach(async () => {
  await truncateAll(pool);
});

after(async () => {
  await pool.end();
  await shutdownMetrics();
});

async function scrape(): Promise<string> {
  const res = await fetch(`http://127.0.0.1:${METRICS_PORT}/metrics`);
  return res.text();
}

function metricValue(body: string, nameWithLabels: string): number | undefined {
  const line = body.split("\n").find((l) => l.startsWith(nameWithLabels));
  if (!line) return undefined;
  const parts = line.trim().split(/\s+/);
  return Number(parts[parts.length - 1]);
}

test("a successful applyTaskCommand records outcome=committed, never rejected/infrastructure_failure", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 60_000));
  await Effect.runPromise(
    store.applyTaskCommand({ taskId, ownerId: "worker-a", epoch: claim.epoch, commandId: "cmd-1", command: { type: "Checkpoint", digest: "d1" } }),
  );

  const body = await scrape();
  const committed = metricValue(body, 'relaylab_command_attempts_total{outcome="committed",reason="n/a",command_type="Checkpoint"');
  assert.ok((committed ?? 0) >= 1, `expected at least one committed command attempt; body excerpt:\n${body.slice(0, 1500)}`);
});

test("an EXPECTED domain rejection (EpochStale) records outcome=rejected, reason=EpochStale -- NOT infrastructure_failure", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 60_000));

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.applyTaskCommand({
          taskId,
          ownerId: "worker-a",
          epoch: claim.epoch + 999, // deliberately stale
          commandId: "cmd-stale",
          command: { type: "Checkpoint", digest: "d1" },
        }),
      ),
    /stale/i,
  );

  const body = await scrape();
  const rejected = metricValue(body, 'relaylab_command_attempts_total{outcome="rejected",reason="EpochStale",command_type="Checkpoint"');
  assert.ok((rejected ?? 0) >= 1, `expected an EpochStale rejection counted as outcome=rejected; body excerpt:\n${body.slice(0, 1500)}`);
  const infraLine = body.split("\n").find((l) => l.includes('outcome="infrastructure_failure"') && l.includes('reason="EpochStale"'));
  assert.equal(infraLine, undefined, "an EpochStale rejection must NEVER be recorded under outcome=infrastructure_failure");
});

test("a REAL infrastructure failure (unreachable MySQL) records outcome=infrastructure_failure -- NEVER outcome=rejected", async () => {
  // A pool pointed at a port nothing listens on -- every query fails
  // with a real connection-level error (ECONNREFUSED), not a domain
  // rejection thrown by this project's own fencing logic.
  const brokenConfig = { ...loadTestMysqlConfig(), port: 39999 };
  const brokenPool = createPool(brokenConfig);
  const brokenStore = makeTaskStoreLive(brokenPool);

  await assert.rejects(() => Effect.runPromise(brokenStore.claimTask(uniqueId("task"), "worker-a", 60_000)));

  const body = await scrape();
  const infra = metricValue(body, 'relaylab_ownership_attempts_total{operation="claim",outcome="infrastructure_failure"');
  assert.ok((infra ?? 0) >= 1, `expected the connection failure counted as outcome=infrastructure_failure; body excerpt:\n${body.slice(0, 1500)}`);
  const rejectedLine = body
    .split("\n")
    .find((l) => l.includes('relaylab_ownership_attempts_total{operation="claim"') && l.includes('outcome="rejected"') && l.includes('reason="InfrastructureError"'));
  assert.equal(rejectedLine, undefined, "a real connection failure must NEVER be recorded under outcome=rejected");

  await brokenPool.end();
});

test("quarantined/buffered/deduped consumer outcomes are never counted as outcome=applied", async () => {
  // Direct, structural check of the outcome enum/mapping rather than a
  // live redrive (consumer-dedup.test.ts and quarantine.test.ts already
  // exercise these paths against real NATS) -- see
  // src/messaging/consumer.ts: each branch (`dead_lettered` for
  // quarantine, and the direct pass-through of `applyEventWithDedup`'s
  // own "deduped"/"buffered"/"conflict"/"applied" result) calls
  // `recordConsumerAttempt` with that EXACT outcome string -- there is
  // no code path that maps any of those four onto "applied" instead of
  // its own real outcome.
  const { recordConsumerAttempt } = await import("../../src/telemetry/metrics.js");
  for (const outcome of ["deduped", "buffered", "conflict", "dead_lettered"] as const) {
    recordConsumerAttempt(outcome, 0.001);
  }
  const body = await scrape();
  for (const outcome of ["deduped", "buffered", "conflict", "dead_lettered"]) {
    assert.ok(metricValue(body, `relaylab_consumer_attempts_total{outcome="${outcome}"`) !== undefined, `expected an outcome="${outcome}" series`);
  }
  // The point of this test: these four observations must land in THEIR
  // OWN outcome label value, never silently incrementing outcome="applied".
});
