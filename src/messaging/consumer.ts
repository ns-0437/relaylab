import type mysql from "mysql2/promise";
import type { PoolConnection, RowDataPacket } from "mysql2/promise";
import type { JetStreamClient, JsMsg } from "nats";
import { withTransaction } from "../db/pool.js";
import { decodeEventEnvelope } from "../protocol/protocol.js";
import { eventsDurable, eventsStream } from "../config.js";
import { fingerprint } from "../domain/fingerprint.js";
import { hitFailpoint } from "../testing/failpoint.js";
import { SpanStatusCode, trace, ROOT_CONTEXT, type Link } from "@opentelemetry/api";
import { getTracer } from "../telemetry/init.js";
import { contextFromCarrier, hasValidSpanContext, type TraceCarrier } from "../telemetry/propagation.js";
import { setRelaylabAttributes } from "../telemetry/attributes.js";
import { traceLogSuffix } from "../telemetry/log.js";
import { writeQuarantineRecord, type StreamQuarantineIdentity } from "./quarantine.js";
import { recordConsumerAttempt } from "../telemetry/metrics.js";

export interface ConsumerOptions {
  consumerName: string;
  /**
   * The JetStream stream's own name and creation-timestamp "generation"
   * (see src/messaging/nats-client.ts `StreamIdentity`). Part of the
   * quarantine dedup key (src/messaging/quarantine.ts) -- see
   * docs/OBSERVABILITY.md "quarantine identity and the supported stream
   * lifecycle" for why a sequence number ALONE is not a safe identity
   * across a stream delete-and-recreate. `consumer-main.ts` always
   * populates these from a REAL `ensureEventsStream()` call;
   * `defaultConsumerOptions()`'s fallback values below are safe defaults
   * for tests that do not exercise this specific lifecycle concern, not
   * a claim of real stream identity.
   */
  streamName: string;
  streamGeneration: string;
  batchSize: number;
  fetchExpiresMs: number;
  maxDeliverBeforeDeadLetter: number;
  /**
   * Artificial per-message delay, applied AFTER the durable effect commits
   * but BEFORE ack. Zero in production. Exists only so
   * tests/integration/recovery-scenario.ts can SIGKILL a consumer midway
   * through a batch and get a reproducible partial-progress snapshot
   * instead of racing a kill signal against sub-millisecond local work.
   */
  demoPerMessageDelayMs: number;
  /**
   * Per-task cap on how many out-of-order ("gap") events can sit durably
   * buffered in task_consumer_pending waiting for the missing seq to show
   * up. This is NOT an in-memory buffer -- it is rows in MySQL, so a
   * process restart never loses them -- but it is still bounded: past this
   * cap, further early-arriving events are acked (so they don't jam
   * redelivery) but logged loudly as a stuck gap requiring operator
   * attention, rather than growing the table without limit. See
   * docs/decisions/0003-event-ordering.md.
   */
  maxPendingPerTask: number;
}

/**
 * Deliberately a FUNCTION, not a module-level constant. A Phase 1.1 audit
 * bug: this used to be exported as `export const DEFAULT_CONSUMER_OPTIONS
 * = defaultConsumerOptions()`, a value computed once at module import
 * time. `processBatch`'s default parameter referenced that cached
 * constant, so any test that imported consumer.js before setting
 * RELAYLAB_NAMESPACE (ordinary static ES module import ordering puts
 * import statements ahead of a later module's side-effecting top-level
 * code) got the WRONG (dev-namespace) consumerName baked in -- it would
 * then js.fetch() the test-namespaced STREAM but for a durable consumer
 * name that only exists on the DEV stream, producing a confusing "503 no
 * responders" with no obvious connection to the real cause. Always call
 * this function, never cache its result across an env var change.
 */
export function defaultConsumerOptions(): ConsumerOptions {
  return {
    consumerName: eventsDurable(),
    streamName: eventsStream(),
    // No real NATS call happens in this synchronous default -- callers
    // that need a REAL collision-safe identity (i.e. anything running
    // against a real, possibly-recreated stream) must override this from
    // a real `ensureEventsStream()` result, as consumer-main.ts does.
    streamGeneration: "unverified-default",
    batchSize: 20,
    fetchExpiresMs: 2000,
    maxDeliverBeforeDeadLetter: 10,
    demoPerMessageDelayMs: 0,
    maxPendingPerTask: 50,
  };
}

export interface ProcessOutcome {
  processed: number;
  deduped: number;
  deadLettered: number;
  buffered: number;
  conflicts: number;
}

/**
 * Pulls one batch and processes each message with the durable-dedup +
 * local-effect + commit-before-ack sequence described in invariant F:
 *
 *   1. BEGIN
 *   2. look up consumer_processed(consumer_name, event_id)
 *      - not found => new event, proceed to 3
 *      - found, same content_hash => redelivery of an already-seen event;
 *        ROLLBACK (nothing new to do) and ACK, so the broker stops
 *        redelivering (invariant F)
 *      - found, DIFFERENT content_hash => the same event_id was reused
 *        with different content. This is a protocol violation, not an
 *        ordinary duplicate -- recorded in consumer_event_conflicts and
 *        logged loudly, NOT silently applied or silently dropped (see
 *        applyEventWithDedup)
 *   3. INSERT consumer_processed, then either apply the event to
 *      task_consumer_state (if it is the next contiguous seq) or buffer it
 *      durably in task_consumer_pending (if it arrived out of order) --
 *      see the gap-handling policy on applyEventWithDedup
 *   4. COMMIT
 *   5. ONLY THEN msg.ack()
 *
 * If the process crashes between 4 and 5, JetStream redelivers after
 * ack_wait; step 2's content-hash lookup makes that redelivery a no-op for
 * the local effect, and step 5 runs again. No database transaction is ever
 * held open across the network ACK (invariant F).
 */
export async function processBatch(
  pool: mysql.Pool,
  js: JetStreamClient,
  // A default parameter EXPRESSION, evaluated fresh on every call -- see
  // the comment on defaultConsumerOptions() for why this must never be a
  // cached module-level constant.
  options: ConsumerOptions = defaultConsumerOptions(),
): Promise<ProcessOutcome> {
  const outcome: ProcessOutcome = { processed: 0, deduped: 0, deadLettered: 0, buffered: 0, conflicts: 0 };
  const iter = js.fetch(eventsStream(), options.consumerName, {
    batch: options.batchSize,
    expires: options.fetchExpiresMs,
  });

  for await (const msg of iter) {
    await handleMessage(pool, msg, options, outcome);
  }
  return outcome;
}

export type ApplyResult = "deduped" | "applied" | "buffered" | "conflict";

export interface InboundEvent {
  eventId: string;
  taskId: string;
  seq: number;
  type: string;
  payload: unknown;
}

async function ensureConsumerStateRow(conn: PoolConnection, consumerName: string, taskId: string): Promise<void> {
  await conn.query(
    `INSERT IGNORE INTO task_consumer_state (consumer_name, task_id, last_applied_seq, event_count, gap_detected, pending_count)
     VALUES (?, ?, 0, 0, 0, 0)`,
    [consumerName, taskId],
  );
}

async function markApplied(conn: PoolConnection, consumerName: string, taskId: string, seq: number): Promise<void> {
  await conn.query(
    `UPDATE task_consumer_state SET last_applied_seq = ?, event_count = event_count + 1 WHERE consumer_name = ? AND task_id = ?`,
    [seq, consumerName, taskId],
  );
}

/**
 * The actual durable-dedup + local-effect transaction (steps 1-3 of the
 * sequence documented on `processBatch`). Exported separately from message
 * handling so tests can drive it directly against a real redelivered
 * `JsMsg` without needing to wait out `ack_wait` -- see
 * tests/integration/consumer-dedup.test.ts, which forces redelivery with
 * `msg.nak()` and calls this function a second time on the same message to
 * prove the dedup record (not broker-level msgID dedup) is what prevents a
 * duplicate effect, and tests/integration/gap-handling.test.ts, which
 * drives out-of-order sequences directly.
 *
 * Gap policy (durable pending events with ordered application -- see
 * docs/decisions/0003-event-ordering.md):
 *   - seq === last_applied_seq + 1: applied immediately, THEN any
 *     contiguous pending rows (last_applied_seq+1, +2, ...) are drained
 *     and applied in order in the SAME transaction ("cascade apply").
 *   - seq > last_applied_seq + 1: durably buffered in
 *     task_consumer_pending (not applied, projection stays incomplete --
 *     gap_detected = 1), bounded by maxPendingPerTask.
 *   - seq <= last_applied_seq and event_id was never seen before: treated
 *     as a no-op (defensive; should not occur given the invariant that
 *     every applied seq has a consumer_processed row).
 */
export async function applyEventWithDedup(
  pool: mysql.Pool,
  consumerName: string,
  event: InboundEvent,
  maxPendingPerTask = 50,
): Promise<ApplyResult> {
  const contentHash = fingerprint({ taskId: event.taskId, seq: event.seq, type: event.type, payload: event.payload });

  return withTransaction(pool, async (conn) => {
    const [existingRows] = await conn.query<(RowDataPacket & { content_hash: string })[]>(
      `SELECT content_hash FROM consumer_processed WHERE consumer_name = ? AND event_id = ? FOR UPDATE`,
      [consumerName, event.eventId],
    );
    const existing = existingRows[0];
    if (existing) {
      if (existing.content_hash !== contentHash) {
        // event_id reused with different content: a protocol violation,
        // not an ordinary duplicate. Record evidence; do NOT apply either
        // version a second time, and do NOT silently drop the conflict.
        await conn.query(
          `INSERT IGNORE INTO consumer_event_conflicts (consumer_name, event_id, first_content_hash, conflicting_content_hash)
           VALUES (?, ?, ?, ?)`,
          [consumerName, event.eventId, existing.content_hash, contentHash],
        );
        return "conflict";
      }
      return "deduped";
    }

    await conn.query(
      `INSERT INTO consumer_processed (consumer_name, event_id, task_id, seq, content_hash) VALUES (?, ?, ?, ?, ?)`,
      [consumerName, event.eventId, event.taskId, event.seq, contentHash],
    );

    await ensureConsumerStateRow(conn, consumerName, event.taskId);
    const [stateRows] = await conn.query<(RowDataPacket & { last_applied_seq: number })[]>(
      `SELECT last_applied_seq FROM task_consumer_state WHERE consumer_name = ? AND task_id = ? FOR UPDATE`,
      [consumerName, event.taskId],
    );
    const lastApplied = stateRows[0]?.last_applied_seq ?? 0;

    if (event.seq <= lastApplied) {
      // Already applied in a prior committed transaction under a
      // different event_id delivery -- should not happen in practice (see
      // docstring), kept as a safe no-op rather than an assertion failure.
      return "deduped";
    }

    if (event.seq > lastApplied + 1) {
      const [countRows] = await conn.query<(RowDataPacket & { c: number })[]>(
        `SELECT COUNT(*) AS c FROM task_consumer_pending WHERE consumer_name = ? AND task_id = ?`,
        [consumerName, event.taskId],
      );
      const pendingCount = countRows[0]?.c ?? 0;
      if (pendingCount >= maxPendingPerTask) {
        console.error(
          `[consumer] task ${event.taskId} already has ${pendingCount} events buffered waiting on a gap after ` +
            `seq ${lastApplied}; refusing to buffer seq ${event.seq} further. This event's delivery is NOT lost ` +
            `(consumer_processed already recorded it above) but it will not auto-apply -- the gap at seq ` +
            `${lastApplied + 1} needs operator attention (query task_consumer_state WHERE gap_detected = 1).${traceLogSuffix()}`,
        );
        await conn.query(`UPDATE task_consumer_state SET gap_detected = 1 WHERE consumer_name = ? AND task_id = ?`, [
          consumerName,
          event.taskId,
        ]);
        return "buffered";
      }

      await conn.query(
        `INSERT IGNORE INTO task_consumer_pending (consumer_name, task_id, seq, event_id, payload) VALUES (?, ?, ?, ?, ?)`,
        [consumerName, event.taskId, event.seq, event.eventId, JSON.stringify(event.payload)],
      );
      await conn.query(
        `UPDATE task_consumer_state SET gap_detected = 1, pending_count = pending_count + 1
         WHERE consumer_name = ? AND task_id = ?`,
        [consumerName, event.taskId],
      );
      return "buffered";
    }

    // event.seq === lastApplied + 1: apply, then cascade-drain any
    // contiguous pending events in the SAME transaction.
    await markApplied(conn, consumerName, event.taskId, event.seq);
    let cursor = event.seq;
    for (;;) {
      const [pendingRows] = await conn.query<RowDataPacket[]>(
        `SELECT seq FROM task_consumer_pending WHERE consumer_name = ? AND task_id = ? AND seq = ? FOR UPDATE`,
        [consumerName, event.taskId, cursor + 1],
      );
      if (!pendingRows[0]) break;
      const nextSeq = cursor + 1;
      await conn.query(`DELETE FROM task_consumer_pending WHERE consumer_name = ? AND task_id = ? AND seq = ?`, [
        consumerName,
        event.taskId,
        nextSeq,
      ]);
      await markApplied(conn, consumerName, event.taskId, nextSeq);
      cursor = nextSeq;
    }

    const [remainingRows] = await conn.query<(RowDataPacket & { c: number })[]>(
      `SELECT COUNT(*) AS c FROM task_consumer_pending WHERE consumer_name = ? AND task_id = ?`,
      [consumerName, event.taskId],
    );
    const remaining = remainingRows[0]?.c ?? 0;
    await conn.query(
      `UPDATE task_consumer_state SET gap_detected = ?, pending_count = ? WHERE consumer_name = ? AND task_id = ?`,
      [remaining > 0 ? 1 : 0, remaining, consumerName, event.taskId],
    );

    // Section 6 crash-window B: writes are pending on this connection but
    // COMMIT (which withTransaction issues right after this callback
    // returns) has not run yet. No-op unless
    // RELAYLAB_FAILPOINT=consumer-before-commit.
    await hitFailpoint("consumer-before-commit");

    return "applied";
  });
}

/**
 * Extracts the W3C carrier from a NATS message's headers, if present.
 * Absent or malformed headers are handled safely by simply returning
 * null -- callers fall back to a parentless/linkless span rather than
 * failing message processing over a telemetry concern.
 */
function carrierFromNatsHeaders(msg: JsMsg): TraceCarrier | null {
  const h = msg.headers;
  if (!h) return null;
  const traceparent = h.get("traceparent");
  const tracestate = h.get("tracestate");
  if (!traceparent) return null;
  const carrier: TraceCarrier = { traceparent };
  if (tracestate) carrier.tracestate = tracestate;
  return carrier;
}

/**
 * Exported (like `applyEventWithDedup`) so tests can drive a single
 * message's handling directly against a real fetched `JsMsg` -- in
 * particular tests/integration/quarantine.test.ts, which needs to force
 * a quarantine-write failure and then redeliver the SAME message with
 * `msg.nak()` rather than waiting out the consumer's 8s `ack_wait`.
 */
export async function handleMessage(
  pool: mysql.Pool,
  msg: JsMsg,
  options: ConsumerOptions,
  outcome: ProcessOutcome,
): Promise<void> {
  // Each delivery ATTEMPT gets its own span, LINKED to (not a child of)
  // the producer's trace. A parent/child relationship would be wrong
  // here: an unbounded number of redeliveries could occur for one
  // message, and modeling each as a child would make a single trace grow
  // without bound and conflate "the same message, redelivered" with "a
  // normal nested call". A link keeps each attempt as its own
  // independent, reasonably-sized trace while still letting an operator
  // jump from a consumer span back to the producer span that created the
  // message (and vice versa) -- this follows the OTel messaging
  // semantic-conventions guidance to prefer links over parent context
  // for the "process" operation when delivery is not a strict 1:1
  // call/return. See docs/OBSERVABILITY.md "context propagation and
  // span relationships".
  const carrier = carrierFromNatsHeaders(msg);
  const producerCtx = contextFromCarrier(carrier);
  const links: Link[] = hasValidSpanContext(producerCtx) ? [{ context: trace.getSpanContext(producerCtx)! }] : [];

  const tracer = getTracer();
  // Explicitly rooted at ROOT_CONTEXT (the 4-arg startActiveSpan overload:
  // name, options, context, fn) rather than the 3-arg (name, options, fn)
  // form, which would derive this span's parent from `context.active()`
  // AT THE CALL SITE -- i.e. whatever happens to be ambiently active in
  // this process when handleMessage runs. Relying on that being empty
  // would make "each attempt starts a new root trace" an accident of
  // there being nothing else active, not a guarantee. Passing
  // ROOT_CONTEXT explicitly makes the documented contract (producer
  // attempts descend from the persisted command context; each consumer
  // attempt starts a NEW root trace and links to the producing publish
  // attempt) hold regardless of what else is running in this process --
  // including a deliberately active unrelated parent span, which is
  // exactly what tests/integration/telemetry.test.ts's isolation test
  // exercises. See docs/OBSERVABILITY.md "context propagation and span
  // relationships".
  await tracer.startActiveSpan("relaylab.consumer.process_attempt", { links }, ROOT_CONTEXT, async (span) => {
    // Phase 1.2.1 Part C: one relaylab_consumer_attempts_total/duration
    // observation per call to this function, i.e. per delivery attempt
    // (a redelivery of the same message is a SEPARATE attempt and
    // increments this again -- see docs/METRICS.md's dictionary entry).
    const startedAtMs = Date.now();
    const quarantineIdentity: StreamQuarantineIdentity = {
      consumerName: options.consumerName,
      streamName: options.streamName,
      streamGeneration: options.streamGeneration,
    };
    try {
      // Phase 1.2.1 Part A.2: malformed JSON and schema-invalid JSON are
      // DISTINCT failure modes, now distinguished explicitly (previously
      // JSON.parse's own throw was caught only by the OUTER catch below,
      // which never acked -- a malformed-JSON message redelivered
      // forever with no durable record at all, until JetStream's own
      // max_deliver silently dropped it with nothing to show for it).
      // Both are now durably quarantined BEFORE ack -- see
      // src/messaging/quarantine.ts and
      // infra/migrations/0004_consumer_quarantine.sql.
      let rawJson: unknown;
      try {
        rawJson = JSON.parse(new TextDecoder().decode(msg.data));
      } catch {
        console.error(`[consumer] malformed JSON payload (stream_seq=${msg.seq}), quarantining${traceLogSuffix()}`);
        setRelaylabAttributes(span, { outcome: "dead_lettered", errorType: "MalformedJson" });
        span.setStatus({ code: SpanStatusCode.ERROR, message: "MalformedJson" });
        // Persistence BEFORE ack: if this throws, propagate (outer catch
        // below) and do NOT ack -- redelivery will retry quarantining
        // rather than silently treating un-persisted evidence as safe.
        await writeQuarantineRecord(pool, quarantineIdentity, msg, "malformed_json");
        msg.ack();
        outcome.deadLettered++;
        recordConsumerAttempt("dead_lettered", (Date.now() - startedAtMs) / 1000);
        return;
      }

      const decoded = decodeEventEnvelope(rawJson);
      if (decoded._tag === "Left") {
        // Schema-invalid: well-formed JSON that doesn't match
        // EventEnvelope. `decoded.left` is an Effect Schema ParseError
        // whose formatted `.message` routinely EMBEDS the actual
        // (possibly sensitive) input values it rejected -- it is never
        // logged or attached to a span, only this fixed classification
        // string is.
        console.error(`[consumer] schema validation failed (stream_seq=${msg.seq}), quarantining${traceLogSuffix()}`);
        setRelaylabAttributes(span, { outcome: "dead_lettered", errorType: "SchemaValidationFailed" });
        span.setStatus({ code: SpanStatusCode.ERROR, message: "SchemaValidationFailed" });
        await writeQuarantineRecord(pool, quarantineIdentity, msg, "schema_invalid");
        msg.ack();
        outcome.deadLettered++;
        recordConsumerAttempt("dead_lettered", (Date.now() - startedAtMs) / 1000);
        return;
      }
      const event = decoded.right;
      setRelaylabAttributes(span, {
        taskId: event.taskId,
        eventId: event.eventId,
        eventType: event.type,
        seq: event.seq,
        deliveryAttempt: msg.info.redeliveryCount,
      });

      if (msg.info.redeliveryCount > options.maxDeliverBeforeDeadLetter) {
        console.error(`[consumer] event ${event.eventId} exceeded max redeliveries, quarantining${traceLogSuffix()}`);
        setRelaylabAttributes(span, { outcome: "dead_lettered", errorType: "MaxRedeliveriesExceeded" });
        span.setStatus({ code: SpanStatusCode.ERROR, message: "MaxRedeliveriesExceeded" });
        await writeQuarantineRecord(pool, quarantineIdentity, msg, "max_redeliveries_exceeded", {
          eventId: event.eventId,
          taskId: event.taskId,
        });
        msg.ack();
        outcome.deadLettered++;
        recordConsumerAttempt("dead_lettered", (Date.now() - startedAtMs) / 1000);
        return;
      }

      const result = await applyEventWithDedup(pool, options.consumerName, event, options.maxPendingPerTask);
      setRelaylabAttributes(span, { outcome: result });

      if (result === "deduped") outcome.deduped++;
      else if (result === "applied") outcome.processed++;
      else if (result === "buffered") outcome.buffered++;
      else if (result === "conflict") {
        console.error(`[consumer] event_id ${event.eventId} reused with different content -- see consumer_event_conflicts${traceLogSuffix()}`);
        outcome.conflicts++;
        // A conflict is a protocol-violation tripwire, not an expected
        // business outcome like deduped/buffered -- surfaced as an error
        // for visibility, distinct from an infrastructure failure.
        span.setStatus({ code: SpanStatusCode.ERROR, message: "EventIdentityConflict" });
      }

      if (options.demoPerMessageDelayMs > 0) {
        await new Promise((r) => setTimeout(r, options.demoPerMessageDelayMs));
      }
      // Section 6 crash-window C: the DB effect and consumer_processed record
      // have committed, but msg.ack() (below) has not run yet. No-op unless
      // RELAYLAB_FAILPOINT=consumer-after-commit-before-ack.
      await hitFailpoint("consumer-after-commit-before-ack");
      // Every outcome acks: redelivery of an already-durably-recorded event
      // (deduped, buffered, or conflict) must never loop forever (see
      // docs/decisions/0003-event-ordering.md "can redelivery create a
      // permanent retry loop?").
      msg.ack();
      recordConsumerAttempt(result, (Date.now() - startedAtMs) / 1000);
    } catch (err) {
      span.setStatus({ code: SpanStatusCode.ERROR, message: err instanceof Error ? err.constructor.name : "UnknownError" });
      // Deliberately NOT recorded in relaylab_consumer_attempts_total --
      // this is an infrastructure-level throw (e.g. a DB error), not one
      // of the finite bounded outcomes the dictionary defines for that
      // counter. See docs/METRICS.md for this stated, honest limitation:
      // an unhandled-exception path is visible in the span/trace (ERROR
      // status) but not broken out as its own metrics outcome bucket.
      throw err;
    } finally {
      span.end();
    }
  });
}
