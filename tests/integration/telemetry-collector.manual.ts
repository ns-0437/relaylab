// Phase 1.2 audit, Part B.4 (original) + Phase 1.2.1 patch section 4
// (strengthened evidence): the ONE test that verifies REAL OTLP receipt
// by a REAL Collector container, using REAL separate publisher and
// consumer processes (not in-process function calls) and STRUCTURED
// evidence (parsed OTLP-JSON span records from the Collector's own
// `file` exporter output), not a marker string matched against
// human-readable debug-exporter log lines.
//
// Requires `docker compose -f infra/compose.yaml --profile observability
// up -d otel-collector` to be running; fails with a clear, actionable
// message (never silently skips) if it is not reachable. Excluded from
// the main `test:integration` glob (filename does not end in
// `.test.ts`); run explicitly via `npm run test:telemetry-collector`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { requireMysqlPool, applySchema, truncateAll, uniqueId } from "./helpers.js";
import { ProcessHarness, testChildEnv, repoRootFrom } from "./process-harness.js";
import { loadTestMysqlConfig } from "../../src/config.js";
import { startTelemetry, shutdownTelemetry } from "../../src/telemetry/init.js";
import { Effect } from "effect";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = repoRootFrom(here);
const composeFile = path.join(repoRoot, "infra", "compose.yaml");
const evidenceFile = path.join(repoRoot, "infra", "otel-evidence", "spans.jsonl");
const publisherEntry = path.join(repoRoot, "src", "messaging", "publisher-main.ts");
const consumerEntry = path.join(repoRoot, "src", "messaging", "consumer-main.ts");

/** Resolves the Collector's ACTUAL container name/state via Compose itself, not an assumed `relaylab-otel-collector-1` string. */
function discoverCollectorContainer(): { name: string; state: string } {
  const out = execFileSync("docker", ["compose", "-f", composeFile, "--profile", "observability", "ps", "otel-collector", "--format", "json"], {
    encoding: "utf8",
  }).trim();
  if (!out) {
    throw new Error(
      `Compose reports no "otel-collector" service container at all.\n` +
        `Start it with:\n  docker compose -f infra/compose.yaml --profile observability up -d otel-collector`,
    );
  }
  // `docker compose ps --format json` emits one JSON object per line (or a single array, depending on version).
  const parsed = out.startsWith("[") ? JSON.parse(out) : out.split("\n").map((l) => JSON.parse(l));
  const entry = Array.isArray(parsed) ? parsed[0] : parsed;
  return { name: entry.Name, state: entry.State };
}

async function assertCollectorReachable(): Promise<void> {
  try {
    await fetch("http://127.0.0.1:4318/v1/traces", { method: "POST", body: "{}", headers: { "content-type": "application/json" } });
  } catch (err) {
    throw new Error(
      `Cannot reach the OTel Collector at http://127.0.0.1:4318 (${err instanceof Error ? err.message : err}).\n` +
        `Start it with:\n  docker compose -f infra/compose.yaml --profile observability up -d otel-collector\n` +
        `This test verifies REAL Collector receipt and must not be reported as passing without it.`,
    );
  }
}

interface SpanAttr {
  key: string;
  value: { stringValue?: string; intValue?: string };
}
interface RawSpan {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  links?: { traceId: string; spanId: string }[];
  attributes?: SpanAttr[];
}

function attr(span: RawSpan, key: string): string | undefined {
  const a = span.attributes?.find((x) => x.key === key);
  return a?.value.stringValue ?? a?.value.intValue;
}

/** Reads ONLY the bytes appended to the evidence file since `fromByteOffset`, parses each JSONL line, and flattens every span across every resourceSpans/scopeSpans entry. Scoping to new-bytes-only is what keeps this test from ever matching a stale span left over from a previous run. */
function readNewSpansSince(fromByteOffset: number): { spans: RawSpan[]; newOffset: number } {
  const stat = statSync(evidenceFile);
  if (stat.size < fromByteOffset) {
    // File was truncated/replaced since we started -- read from the top.
    fromByteOffset = 0;
  }
  const buf = readFileSync(evidenceFile);
  const newContent = buf.subarray(fromByteOffset).toString("utf8");
  const spans: RawSpan[] = [];
  for (const line of newContent.split("\n")) {
    if (!line.trim()) continue;
    const doc = JSON.parse(line);
    for (const rs of doc.resourceSpans ?? []) {
      for (const ss of rs.scopeSpans ?? []) {
        for (const span of ss.spans ?? []) spans.push(span);
      }
    }
  }
  return { spans, newOffset: stat.size };
}

async function waitForNewSpans(fromByteOffset: number, predicate: (spans: RawSpan[]) => boolean, timeoutMs: number): Promise<RawSpan[]> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { spans } = readNewSpansSince(fromByteOffset);
    if (predicate(spans)) return spans;
    await new Promise((r) => setTimeout(r, 300));
  }
  const { spans } = readNewSpansSince(fromByteOffset);
  throw new Error(`timed out after ${timeoutMs}ms waiting for the expected spans to appear; spans seen so far: ${JSON.stringify(spans.map((s) => s.name))}`);
}

test(
  "real OTLP-to-Collector, full path: separate publisher/consumer processes, structured span evidence (full trace/span IDs, parent relationship, consumer link, shared event identity)",
  async () => {
    await assertCollectorReachable();
    const collector = discoverCollectorContainer();
    assert.ok(collector.state.toLowerCase().includes("running"), `Collector container "${collector.name}" is not running (state: ${collector.state})`);

    const pool = await requireMysqlPool();
    await applySchema(pool);
    await truncateAll(pool);

    const testConfig = loadTestMysqlConfig();
    const harness = new ProcessHarness();
    const markerTaskId = uniqueId("otlp-fullpath-proof");

    // Scope all evidence-reading to bytes appended AFTER this point --
    // never match anything already in the file from a prior run.
    const startOffset = statSync(evidenceFile).size;

    delete process.env.RELAYLAB_OTEL_DISABLED;
    startTelemetry("relaylab-collector-proof-test");
    try {
      // 1. Create one command and its durable outbox event. This happens
      //    in THIS process, which is why telemetry must be started here
      //    too (distinct from the publisher/consumer, which are real
      //    separate child processes started below) -- otherwise
      //    `relaylab.command.process` would never be exported at all.
      const store = makeTaskStoreLive(pool);
      const claim = await Effect.runPromise(store.claimTask(markerTaskId, "worker-a", 60_000));
      const receipt = await Effect.runPromise(
        store.applyTaskCommand({
          taskId: markerTaskId,
          ownerId: "worker-a",
          epoch: claim.epoch,
          commandId: "cmd-1",
          command: { type: "Checkpoint", digest: "collector-fullpath-proof" },
        }),
      );
      assert.equal(receipt.replay, false);

      // 2. Publish and consume it via REAL, SEPARATE processes (not
      //    in-process function calls), with telemetry explicitly enabled
      //    and pointed at the real Collector.
      const env = { ...testChildEnv(testConfig.database, { RELAYLAB_OTEL_DISABLED: "0" }) };
      const publisher = harness.spawn({ label: "pub-collector-proof", entry: publisherEntry, cwd: repoRoot, env });
      const consumer = harness.spawn({ label: "con-collector-proof", entry: consumerEntry, cwd: repoRoot, env });

      await waitFor(async () => {
        const [rows] = await pool.query<any[]>(`SELECT event_count FROM task_consumer_state WHERE task_id = ?`, [markerTaskId]);
        return (rows[0]?.event_count ?? 0) >= 1;
      }, 15_000);

      // 3. Flush/shut down gracefully (harness tries IPC "shutdown" first,
      //    bounded-waits, force-kills only if still alive -- see
      //    process-harness.ts / docs/decisions/0009). This is also what
      //    makes each process's `shutdownTelemetry()` actually run and
      //    flush its buffered spans to the Collector before it exits.
      await harness.stopAll();
      void publisher;
      void consumer;

      // 4. Retrieve Collector-received span evidence: poll the structured
      //    evidence file (appended to asynchronously by the Collector's
      //    batch processor, hence polling rather than a single read) for
      //    all three expected span kinds belonging to THIS run's marker.
      const spans = await waitForNewSpans(
        startOffset,
        (candidate) => {
          const forThisTask = candidate.filter((s) => attr(s, "relaylab.task_id") === markerTaskId);
          const names = new Set(forThisTask.map((s) => s.name));
          return names.has("relaylab.command.process") && names.has("relaylab.outbox.publish_attempt") && names.has("relaylab.consumer.process_attempt");
        },
        20_000,
      );

      const forThisTask = spans.filter((s) => attr(s, "relaylab.task_id") === markerTaskId);
      const commandSpan = forThisTask.find((s) => s.name === "relaylab.command.process")!;
      const publishSpan = forThisTask.find((s) => s.name === "relaylab.outbox.publish_attempt")!;
      const consumeSpan = forThisTask.find((s) => s.name === "relaylab.consumer.process_attempt")!;
      assert.ok(commandSpan && publishSpan && consumeSpan, "all three span kinds must be present for this run's marker task_id");

      // 5. Verify full trace IDs, span IDs, parent relationship, and the
      //    consumer link -- actual values, not just "a span exists".
      assert.equal(publishSpan.traceId, commandSpan.traceId, "publish span must share the command span's trace ID");
      assert.equal(publishSpan.parentSpanId, commandSpan.spanId, "publish span's parent must be the command span");
      assert.notEqual(consumeSpan.traceId, publishSpan.traceId, "consumer span must be a NEW trace, not an extension of the publish trace");
      assert.equal(consumeSpan.links?.length, 1, "consumer span must carry exactly one link");
      assert.equal(consumeSpan.links![0]!.traceId, publishSpan.traceId, "the link must point at the publish span's trace");
      assert.equal(consumeSpan.links![0]!.spanId, publishSpan.spanId, "the link must point at the publish span itself");

      // 6. Verify the shared APPLICATION event identity (event_id) is
      //    identical across the publish and consumer spans -- this is
      //    the business identity the trace is following, independent of
      //    trace/span IDs.
      const publishEventId = attr(publishSpan, "relaylab.event_id");
      const consumeEventId = attr(consumeSpan, "relaylab.event_id");
      assert.ok(publishEventId, "publish span must carry relaylab.event_id");
      assert.equal(consumeEventId, publishEventId, "publish and consumer spans must reference the SAME application event_id");

      console.log(
        `[test] verified structured Collector evidence: command traceId=${commandSpan.traceId} spanId=${commandSpan.spanId}; ` +
          `publish traceId=${publishSpan.traceId} spanId=${publishSpan.spanId} parentSpanId=${publishSpan.parentSpanId}; ` +
          `consume traceId=${consumeSpan.traceId} spanId=${consumeSpan.spanId} link->${consumeSpan.links![0]!.traceId}/${consumeSpan.links![0]!.spanId}; ` +
          `shared event_id=${publishEventId}`,
      );
    } finally {
      await harness.stopAll();
      await shutdownTelemetry(5000);
      await pool.end();
    }
  },
);

async function waitFor(predicate: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 300));
  }
  throw new Error(`timed out after ${timeoutMs}ms`);
}
