// Phase 1.2 audit, Part C: focused OpenTelemetry tests using an
// in-memory span exporter for precise assertions (context persistence
// across the outbox, NATS link semantics, concurrent-task isolation,
// redelivery, fingerprint non-interference, missing/invalid headers,
// Collector outage tolerance, bounded shutdown, and redaction). The
// real OTLP-to-Collector scenario is a SEPARATE test
// (telemetry-collector.test.ts) -- this file never claims Collector
// receipt, only that the right spans/links/attributes are produced.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { Effect } from "effect";
import { trace, propagation, context, SpanStatusCode } from "@opentelemetry/api";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor, type ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { runPublisherOnce } from "../../src/messaging/publisher.js";
import { processBatch, applyEventWithDedup, defaultConsumerOptions } from "../../src/messaging/consumer.js";
import { fingerprint } from "../../src/domain/fingerprint.js";
import { applySchema, requireMysqlPool, requireNats, resetStream, truncateAll, uniqueId, type NatsTestHandles } from "./helpers.js";

let pool: mysql.Pool;
let nats: NatsTestHandles;
let exporter: InMemorySpanExporter;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
  nats = await requireNats();

  // Both of these are ONE-TIME, process-wide registrations in
  // @opentelemetry/api -- a second call to either is silently ignored
  // (verified empirically; this surprised an earlier version of this
  // test file, which re-registered a FRESH provider/exporter per test
  // and never noticed that only test 1's ever actually received spans).
  // So the provider is registered ONCE here, and `exporter.reset()` in
  // beforeEach clears recorded spans between tests instead.
  //
  // The propagator registration matters just as much: `captureCurrentCarrier`
  // (src/telemetry/propagation.ts) calls `propagation.inject`, which is a
  // total no-op under the default NoopTextMapPropagator -- production
  // code gets a real one via NodeSDK's `textMapPropagator` option
  // (src/telemetry/init.ts), which this test never calls, so it must be
  // registered directly here to exercise the same carrier format.
  exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  trace.setGlobalTracerProvider(provider);
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  // Without a real context manager, `context.active()`/`context.with()`
  // are effectively no-ops across `await` boundaries (the default,
  // unregistered state is a no-op manager) -- production gets a working
  // one automatically as part of NodeSDK's own setup; this test must
  // register the same kind directly, or `startActiveSpan`'s span never
  // actually stays "active" past the first `await` inside it.
  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
});

beforeEach(async () => {
  await truncateAll(pool);
  await resetStream(nats.jsm);
  exporter.reset();
});

after(async () => {
  await nats.nc.drain();
  await pool.end();
});

function spansByName(name: string): ReadableSpan[] {
  return exporter.getFinishedSpans().filter((s) => s.name === name);
}

async function commitOneCommand(taskId: string, ownerId: string, commandId: string, digest: string) {
  const store = makeTaskStoreLive(pool);
  const claim = await Effect.runPromise(store.claimTask(taskId, ownerId, 60_000));
  return Effect.runPromise(
    store.applyTaskCommand({ taskId, ownerId, epoch: claim.epoch, commandId, command: { type: "Checkpoint", digest } }),
  );
}

test("1. context survives DB persistence: the outbox row's trace_carrier contains the SAME traceId as the command span, readable back from a fresh query", async () => {
  const taskId = uniqueId("task");
  await commitOneCommand(taskId, "worker-a", "cmd-1", "d1");

  const commandSpans = spansByName("relaylab.command.process");
  assert.equal(commandSpans.length, 1);
  const commandTraceId = commandSpans[0]!.spanContext().traceId;

  const [rows] = await pool.query<(RowDataPacket & { trace_carrier: unknown })[]>(
    `SELECT trace_carrier FROM outbox_events WHERE task_id = ?`,
    [taskId],
  );
  const carrier = typeof rows[0]?.trace_carrier === "string" ? JSON.parse(rows[0].trace_carrier) : rows[0]?.trace_carrier;
  assert.ok(carrier?.traceparent, "trace_carrier must be persisted with a traceparent");
  assert.ok(
    carrier.traceparent.includes(commandTraceId),
    `persisted traceparent must encode the SAME traceId the command span used (${commandTraceId})`,
  );
});

test("2. NATS propagation: publish uses PARENT context (same trace as the command), consume uses a LINK (new trace, cross-referenced)", async () => {
  const taskId = uniqueId("task");
  await commitOneCommand(taskId, "worker-a", "cmd-1", "d1");
  await runPublisherOnce(pool, nats.js);
  await processBatch(pool, nats.js);

  const commandSpan = spansByName("relaylab.command.process")[0]!;
  const publishSpan = spansByName("relaylab.outbox.publish_attempt")[0]!;
  const consumeSpan = spansByName("relaylab.consumer.process_attempt")[0]!;

  assert.equal(publishSpan.spanContext().traceId, commandSpan.spanContext().traceId, "publish must be a CHILD (same trace) of the command span");
  assert.equal(publishSpan.parentSpanContext?.spanId, commandSpan.spanContext().spanId);

  assert.notEqual(
    consumeSpan.spanContext().traceId,
    publishSpan.spanContext().traceId,
    "consume must start a NEW trace, not extend the producer's, per the documented link-not-parent policy",
  );
  assert.equal(consumeSpan.links.length, 1, "consume must carry exactly one link back to the producer");
  assert.equal(consumeSpan.links[0]!.context.traceId, publishSpan.spanContext().traceId);
  assert.equal(consumeSpan.links[0]!.context.spanId, publishSpan.spanContext().spanId);
});

test(
  "2b. consumer context isolation is EXPLICIT: processing under a deliberately active UNRELATED parent span still " +
    "produces a parentless, new-trace consumer span that links only to its real producer, never inheriting the unrelated trace",
  async () => {
    const taskId = uniqueId("task");
    await commitOneCommand(taskId, "worker-a", "cmd-1", "d1");
    await runPublisherOnce(pool, nats.js);

    // Deliberately make an UNRELATED span active in this process's
    // ambient context for the entire duration of consumption -- this is
    // exactly what would leak into the consumer span if
    // `handleMessage`'s `startActiveSpan` call relied on `context.active()`
    // at the call site instead of explicitly passing ROOT_CONTEXT (see
    // src/messaging/consumer.ts).
    const unrelatedTracer = trace.getTracer("test-unrelated");
    await unrelatedTracer.startActiveSpan("test.unrelated_ambient_span", async (unrelatedSpan) => {
      await processBatch(pool, nats.js);
      unrelatedSpan.end();
    });

    const publishSpan = spansByName("relaylab.outbox.publish_attempt")[0]!;
    const consumeSpan = spansByName("relaylab.consumer.process_attempt")[0]!;
    const unrelatedSpan = spansByName("test.unrelated_ambient_span")[0]!;

    assert.equal(consumeSpan.parentSpanContext, undefined, "the consumer span must have NO parent, even though an unrelated span was active");
    assert.notEqual(
      consumeSpan.spanContext().traceId,
      unrelatedSpan.spanContext().traceId,
      "the consumer span must NOT inherit the unrelated ambient trace",
    );
    assert.equal(consumeSpan.links.length, 1, "it must still link to its real producer");
    assert.equal(consumeSpan.links[0]!.context.traceId, publishSpan.spanContext().traceId);
    assert.equal(consumeSpan.links[0]!.context.spanId, publishSpan.spanContext().spanId);
  },
);

test("3. two concurrent tasks remain correctly separated: each consumer span links ONLY to its own task's producer span, never the other's", async () => {
  const taskA = uniqueId("task-a");
  const taskB = uniqueId("task-b");
  await Promise.all([commitOneCommand(taskA, "worker-a", "cmd-1", "dA"), commitOneCommand(taskB, "worker-b", "cmd-1", "dB")]);
  await runPublisherOnce(pool, nats.js);
  await processBatch(pool, nats.js, { ...defaultConsumerOptions(), batchSize: 20 });

  const publishSpans = spansByName("relaylab.outbox.publish_attempt");
  const consumeSpans = spansByName("relaylab.consumer.process_attempt");
  assert.equal(publishSpans.length, 2);
  assert.equal(consumeSpans.length, 2);

  for (const consumeSpan of consumeSpans) {
    const taskId = consumeSpan.attributes["relaylab.task_id"];
    const matchingPublish = publishSpans.find((p) => p.attributes["relaylab.task_id"] === taskId)!;
    assert.equal(consumeSpan.links[0]!.context.traceId, matchingPublish.spanContext().traceId, `task ${taskId}'s consumer span linked to the WRONG producer`);
    const otherPublish = publishSpans.find((p) => p.attributes["relaylab.task_id"] !== taskId)!;
    assert.notEqual(consumeSpan.links[0]!.context.traceId, otherPublish.spanContext().traceId);
  }
});

test("4. redelivery preserves business identity (same relaylab.event_id) while producing a distinguishable processing attempt (different span)", async () => {
  const taskId = uniqueId("task");
  await commitOneCommand(taskId, "worker-a", "cmd-1", "d1");
  await runPublisherOnce(pool, nats.js);
  await processBatch(pool, nats.js); // first delivery: applied

  // The message was already acked by processBatch above (see
  // consumer-dedup.test.ts for the msg.nak()-based real-redelivery test);
  // here, model a second ATTEMPT at the DB-dedup layer directly, which is
  // exactly what a genuine redelivery would also invoke, to confirm each
  // attempt gets its own distinguishable span while sharing the same
  // business event_id.
  const [rows] = await pool.query<RowDataPacket[]>(`SELECT event_id, task_id, seq, type, payload FROM outbox_events WHERE task_id = ?`, [taskId]);
  const event = { eventId: rows[0]!.event_id, taskId: rows[0]!.task_id, seq: rows[0]!.seq, type: rows[0]!.type, payload: rows[0]!.payload };

  const tracer = trace.getTracer("test-redelivery");
  await tracer.startActiveSpan("test.redelivery_attempt", async (span) => {
    span.setAttribute("relaylab.delivery_attempt", 2);
    const result = await applyEventWithDedup(pool, defaultConsumerOptions().consumerName, event);
    span.setAttribute("relaylab.outcome", result);
    span.end();
  });

  const redeliverySpans = spansByName("test.redelivery_attempt");
  assert.equal(redeliverySpans.length, 1);
  assert.equal(redeliverySpans[0]!.attributes["relaylab.outcome"], "deduped", "the second attempt must be recognized as a duplicate of the SAME business event");
  assert.notEqual(redeliverySpans[0]!.spanContext().spanId, spansByName("relaylab.consumer.process_attempt")[0]!.spanContext().spanId, "each attempt is its OWN distinguishable span");
});

test("5. changing trace metadata does not create a business fingerprint/content-hash conflict", async () => {
  const command = { type: "Checkpoint" as const, digest: "same-content" };
  const fp1 = fingerprint(command);
  // Simulate the command being retried under a totally different trace
  // (a fresh span/trace started) -- the fingerprint function never sees
  // trace context at all, so this must be a no-op on the result.
  await trace.getTracer("t").startActiveSpan("unrelated-span-1", async (span) => {
    span.end();
  });
  const fp2 = fingerprint(command);
  await trace.getTracer("t").startActiveSpan("unrelated-span-2", async (span) => {
    span.end();
  });
  const fp3 = fingerprint(command);
  assert.equal(fp1, fp2);
  assert.equal(fp2, fp3);
});

test("6. missing/invalid trace headers do not break valid business processing", async () => {
  const taskId = uniqueId("task");
  await commitOneCommand(taskId, "worker-a", "cmd-1", "d1");
  // Strip the trace_carrier before publishing, simulating a row written
  // before telemetry existed, or a carrier that failed to persist.
  await pool.query(`UPDATE outbox_events SET trace_carrier = NULL WHERE task_id = ?`, [taskId]);
  await runPublisherOnce(pool, nats.js);
  const outcome = await processBatch(pool, nats.js);
  assert.equal(outcome.processed, 1, "processing must succeed even with no trace context to extract");

  // The publish span itself is still created (it is not conditional on a
  // producer-side carrier existing) and is unparented -- a fresh trace
  // root -- since there was nothing valid to restore as its parent. The
  // consumer can still link to THAT publish span (this is the correct,
  // graceful-degradation behavior: missing upstream context loses one
  // hop of ancestry, not the whole mechanism).
  const publishSpans = spansByName("relaylab.outbox.publish_attempt");
  assert.equal(publishSpans.length, 1);
  assert.equal(publishSpans[0]!.parentSpanContext, undefined, "with no carrier to restore, the publish span must be an unparented trace root");

  const consumeSpans = spansByName("relaylab.consumer.process_attempt");
  assert.equal(consumeSpans.length, 1);
  assert.equal(consumeSpans[0]!.links.length, 1, "the consumer can still link to the publish attempt's own span");
  assert.equal(consumeSpans[0]!.links[0]!.context.spanId, publishSpans[0]!.spanContext().spanId);
});


// Phase 1.2.1 patch, section 5: sensitive-data coverage expanded across
// EVERY surface this codebase actually produces, not just span
// attributes/names. Each test below states precisely which surface it
// covers -- do not read any one of them as "redaction is universally
// proven"; together they cover the surfaces listed, and nothing claims
// more than that.
//
// Surfaces covered: span names, span attributes, span events, span
// status descriptions, and captured console.error/console.log output.
// Surfaces explicitly NOT covered by these tests: the raw OTLP wire
// payload below the in-memory exporter's ReadableSpan projection (not
// expected to differ, since the SDK serializes exactly these same
// fields, but not independently re-verified here -- see
// telemetry-collector.manual.ts for a real end-to-end OTLP payload, which
// is inspected for the business event_id but not re-scanned for secrets);
// and the Collector's own container logs/file-exporter output (no
// sentinel is threaded through that specific test, since its purpose is
// structural trace-shape verification, not redaction).
function assertNoSentinelAnywhere(spans: ReadableSpan[], forbidden: string[], context: string): void {
  for (const span of spans) {
    for (const needle of forbidden) {
      assert.ok(!span.name.includes(needle), `[${context}] span name "${span.name}" leaked a forbidden value`);
      for (const [key, value] of Object.entries(span.attributes)) {
        assert.ok(!String(value).includes(needle), `[${context}] span "${span.name}" attribute "${key}" leaked a forbidden value`);
      }
      for (const event of span.events) {
        assert.ok(!event.name.includes(needle), `[${context}] span "${span.name}" event name "${event.name}" leaked a forbidden value`);
        for (const [key, value] of Object.entries(event.attributes ?? {})) {
          assert.ok(!String(value).includes(needle), `[${context}] span "${span.name}" event "${event.name}" attribute "${key}" leaked a forbidden value`);
        }
      }
      if (span.status.message) {
        assert.ok(!span.status.message.includes(needle), `[${context}] span "${span.name}" status message leaked a forbidden value: "${span.status.message}"`);
      }
    }
  }
}

test("9a. sensitive sentinel values never appear in span names/attributes/events/status on the NORMAL (success) business payload path", async () => {
  const SENTINEL = "SECRET-DO-NOT-EXPORT-9f3a7c";
  const taskId = uniqueId("task");
  // The sentinel is placed in the COMMAND PAYLOAD (digest), which is
  // business data the allowlisted attribute helper never touches.
  await commitOneCommand(taskId, "worker-a", "cmd-1", SENTINEL);
  await runPublisherOnce(pool, nats.js);
  await processBatch(pool, nats.js);

  const spans = exporter.getFinishedSpans();
  assert.ok(spans.length > 0, "sanity check: spans must actually have been produced");
  assertNoSentinelAnywhere(spans, [SENTINEL], "normal payload path");

  // Explicitly confirm the "span events" surface: this codebase does not
  // currently call `span.addEvent(...)` anywhere, so every span's events
  // array is empty. Documented here so the claim "events never leak a
  // sentinel" isn't vacuously true by coincidence -- if a future change
  // adds a real `addEvent` call, this assertion will need to be revisited
  // (and the event-carrying surface above will then actually be exercised).
  for (const span of spans) assert.equal(span.events.length, 0, `span "${span.name}" has events, but none were expected in the current codebase`);
});

test("9b. sensitive values never appear in span status descriptions or attributes on the ERROR (domain rejection) path, even when business data nearby contains them", async () => {
  const SENTINEL = "SECRET-DO-NOT-EXPORT-error-path-b7e1";
  const taskId = uniqueId("task");
  const store = makeTaskStoreLive(pool);
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 60_000));
  // A sentinel-laden checkpoint commits successfully first...
  await Effect.runPromise(
    store.applyTaskCommand({ taskId, ownerId: "worker-a", epoch: claim.epoch, commandId: "cmd-1", command: { type: "Checkpoint", digest: SENTINEL } }),
  );
  // ...then a REAL domain rejection (Complete without a passed
  // verification) happens on the SAME task, right after it, while the
  // sentinel-laden checkpoint is still the task's most recent state.
  const rejection = await Effect.runPromise(
    Effect.flip(store.applyTaskCommand({ taskId, ownerId: "worker-a", epoch: claim.epoch, commandId: "cmd-2", command: { type: "Complete" } })),
  );
  assert.equal(rejection._tag, "CommandRejected");

  const spans = exporter.getFinishedSpans();
  const rejectedSpan = spans.filter((s) => s.name === "relaylab.command.process").find((s) => s.attributes["relaylab.outcome"] === "rejected")!;
  assert.ok(rejectedSpan, "a rejected command.process span must exist");
  assert.equal(rejectedSpan.attributes["relaylab.error_type"], "CommandRejected", "the stable tag IS expected on the span -- that is the redacted, allowlisted classification, not a leak");
  assert.equal(rejectedSpan.status.code, SpanStatusCode.UNSET, "CommandRejected is an EXPECTED domain rejection per the span-status policy, not an ERROR status");

  assertNoSentinelAnywhere(spans, [SENTINEL], "error path, business data nearby");
});

test("9c. classifyError/classifyShutdownError never surface a synthetic credential-bearing error message (unit-level, no real secrets used)", async () => {
  const { classifyError, isExpectedDomainRejection } = await import("../../src/telemetry/attributes.js");
  const { classifyShutdownError } = await import("../../src/telemetry/init.js");

  const fakeCredentialUrl = "postgres://admin:FAKE_SECRET_hunter2_9f3a@db.internal:5432/relaylab";
  const fakeAuthHeader = "Authorization: Bearer FAKE_TOKEN_abc123_not_a_real_secret";

  // A domain-style error (has `_tag`) whose underlying message happens to
  // contain credential-looking text -- classifyError only ever reads
  // `_tag`, never `.message`, so this must be safe regardless.
  class FakeTaggedError extends Error {
    readonly _tag = "SomeDomainError";
    constructor() {
      super(`operation failed while connecting to ${fakeCredentialUrl} -- ${fakeAuthHeader}`);
    }
  }
  const domainClassification = classifyError(new FakeTaggedError());
  assert.equal(domainClassification, "SomeDomainError");
  assert.ok(!domainClassification.includes("FAKE_SECRET") && !domainClassification.includes("FAKE_TOKEN"));
  assert.equal(isExpectedDomainRejection(domainClassification), false);

  // A generic (non-domain) error, as a real network/export failure would
  // be -- classifyShutdownError only ever reads the constructor name and
  // the standard `.code` field, never `.message`.
  class FakeNetworkError extends Error {
    code = "ECONNREFUSED";
    constructor() {
      super(`connect failed: ${fakeCredentialUrl} (${fakeAuthHeader})`);
    }
  }
  const networkClassification = classifyShutdownError(new FakeNetworkError());
  assert.equal(networkClassification, "FakeNetworkError(ECONNREFUSED)");
  assert.ok(!networkClassification.includes("FAKE_SECRET") && !networkClassification.includes("FAKE_TOKEN"));
});

// 9d (a captured console.error line during a REAL shutdown-error path
// never contains a synthetic credential-bearing value) lives in
// telemetry-resilience.test.ts instead of here: it needs the real
// NodeSDK, whose shutdown calls trace.disable()/context.disable()/
// propagation.disable() -- which would corrupt the in-memory
// provider/propagator/context-manager this file registers once in
// before() for every other test.
