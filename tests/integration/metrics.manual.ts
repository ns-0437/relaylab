// Phase 1.2.1 patch, Part F validation: real publisher/consumer processes
// with metrics ENABLED, scraped over real HTTP, asserting actual
// Prometheus-exposition-format output -- not a unit test of the
// instrument wiring, a real end-to-end scrape. Excluded from the main
// `test:integration` glob (filename does not end in `.test.ts`, same
// convention as telemetry-collector.manual.ts) since it needs two real
// child processes on fixed ports; run explicitly via
// `npm run test:metrics`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import path from "node:path";
import type mysql from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, requireNats, resetStream, truncateAll, uniqueId, type NatsTestHandles } from "./helpers.js";
import { ProcessHarness, testChildEnv, repoRootFrom } from "./process-harness.js";
import { loadTestMysqlConfig } from "../../src/config.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = repoRootFrom(here);
const publisherEntry = path.join(repoRoot, "src", "messaging", "publisher-main.ts");
const consumerEntry = path.join(repoRoot, "src", "messaging", "consumer-main.ts");
const testConfig = loadTestMysqlConfig();

const PUB_METRICS_PORT = 19464;
const CON_METRICS_PORT = 19465;

async function scrape(port: number): Promise<string> {
  const res = await fetch(`http://127.0.0.1:${port}/metrics`);
  assert.equal(res.status, 200, `expected the /metrics endpoint on :${port} to respond 200`);
  return res.text();
}

function metricLine(body: string, nameWithLabels: string): string | undefined {
  return body.split("\n").find((l) => l.startsWith(nameWithLabels));
}

test(
  "real publisher/consumer processes expose working Prometheus /metrics endpoints with expected series after real command/publish/consume activity",
  { timeout: 30_000 },
  async () => {
    let pool: mysql.Pool | undefined;
    let nats: NatsTestHandles | undefined;
    const harness = new ProcessHarness();
    try {
      pool = await requireMysqlPool();
      await applySchema(pool);
      await truncateAll(pool);
      nats = await requireNats();
      await resetStream(nats.jsm);

      const taskId = uniqueId("task");
      const store = makeTaskStoreLive(pool);
      const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 60_000));
      await Effect.runPromise(
        store.applyTaskCommand({
          taskId,
          ownerId: "worker-a",
          epoch: claim.epoch,
          commandId: "cmd-1",
          command: { type: "Checkpoint", digest: "metrics-smoke-test" },
        }),
      );

      const env = testChildEnv(testConfig.database, { RELAYLAB_OTEL_DISABLED: "0" });
      harness.spawn({
        label: "pub-metrics",
        entry: publisherEntry,
        cwd: repoRoot,
        env: { ...env, RELAYLAB_METRICS_PORT: String(PUB_METRICS_PORT) },
      });
      harness.spawn({
        label: "con-metrics",
        entry: consumerEntry,
        cwd: repoRoot,
        env: { ...env, RELAYLAB_METRICS_PORT: String(CON_METRICS_PORT) },
      });

      // Bounded polling for the event to actually be consumed, rather
      // than an arbitrary sleep.
      const deadline = Date.now() + 15_000;
      let consumed = false;
      while (Date.now() < deadline && !consumed) {
        const [rows] = await pool.query<any[]>(`SELECT event_count FROM task_consumer_state WHERE task_id = ?`, [taskId]);
        consumed = (rows[0]?.event_count ?? 0) >= 1;
        if (!consumed) await new Promise((r) => setTimeout(r, 300));
      }
      assert.ok(consumed, "the event must have been consumed before scraping metrics");

      const pubBody = await scrape(PUB_METRICS_PORT);
      const conBody = await scrape(CON_METRICS_PORT);

      // Command attempt (committed) -- recorded in the test process
      // itself (applyTaskCommand ran here, not in the child), so this is
      // NOT expected on either child's endpoint; what IS expected on the
      // publisher child is a publish attempt, and on the consumer child a
      // consumer attempt. This also double-checks that metrics are truly
      // PER-PROCESS (no cross-process aggregation happening silently).
      assert.ok(
        metricLine(pubBody, 'relaylab_publish_attempts_total{outcome="published"'),
        `expected a published publish-attempt counter on the publisher's /metrics; got:\n${pubBody.slice(0, 2000)}`,
      );
      assert.ok(
        metricLine(conBody, 'relaylab_consumer_attempts_total{outcome="applied"'),
        `expected an applied consumer-attempt counter on the consumer's /metrics; got:\n${conBody.slice(0, 2000)}`,
      );
      assert.ok(
        metricLine(pubBody, "relaylab_publish_attempt_duration_seconds_count"),
        "expected the publish-duration histogram to have recorded at least one observation",
      );
      assert.ok(
        metricLine(conBody, "relaylab_consumer_processing_duration_seconds_count"),
        "expected the consumer-duration histogram to have recorded at least one observation",
      );
      // Durable gauges: the publisher is the sole leader here (only one
      // publisher process running), so it must expose the outbox gauges;
      // the consumer must expose the gap/quarantine gauges. Neither
      // process exposes the OTHER's gauge group (ownership split -- see
      // docs/METRICS.md).
      assert.ok(metricLine(pubBody, "relaylab_outbox_unpublished_count"), "publisher (leader) must expose the outbox backlog gauge");
      assert.ok(!metricLine(pubBody, "relaylab_pending_gap_events_count"), "publisher must NOT expose the consumer's gauge group");
      assert.ok(metricLine(conBody, "relaylab_pending_gap_events_count"), "consumer must expose the gap-backlog gauge");
      assert.ok(!metricLine(conBody, "relaylab_outbox_unpublished_count"), "consumer must NOT expose the publisher's gauge group");
      assert.ok(
        /relaylab_publisher_leadership_state\{[^}]*\}\s+1/.test(pubBody),
        `the sole publisher must report leadership_state == 1; got:\n${pubBody.slice(0, 2000)}`,
      );
      assert.ok(metricLine(pubBody, "relaylab_gauge_last_refresh_success_timestamp_seconds"), "publisher must report a gauge refresh freshness timestamp");
    } finally {
      await harness.stopAll();
      await nats?.nc.drain();
      await pool?.end();
    }
  },
);
