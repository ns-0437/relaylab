import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import type { JetStreamClient } from "nats";
import { headers as natsHeaders } from "nats";
import { context as otelContext, SpanStatusCode } from "@opentelemetry/api";
import { eventsSubject } from "../config.js";
import { hitFailpoint } from "../testing/failpoint.js";
import { getTracer } from "../telemetry/init.js";
import { contextFromCarrier, captureCurrentCarrier, type TraceCarrier } from "../telemetry/propagation.js";
import { setRelaylabAttributes } from "../telemetry/attributes.js";
import { recordPublishAttempt } from "../telemetry/metrics.js";

export interface PublisherOptions {
  pollIntervalMs: number;
  batchSize: number;
  maxRetries: number;
}

export const DEFAULT_PUBLISHER_OPTIONS: PublisherOptions = {
  pollIntervalMs: 250,
  batchSize: 50,
  maxRetries: 5,
};

interface OutboxRow extends RowDataPacket {
  id: number;
  event_id: string;
  task_id: string;
  seq: number;
  type: string;
  payload: unknown;
  trace_carrier: TraceCarrier | null;
  created_at: Date;
}

/**
 * Publishes one batch of unpublished outbox rows. This function itself
 * does NOT check publisher leadership -- that is the caller's job
 * (publisher-main.ts gates calls to this function on
 * tryAcquireOrRenewPublisherLeadership; see publisher-lock.ts). Tests call
 * this function directly, bypassing the lock, which is intentional: it
 * lets tests exercise "what if two publish batches race" without needing
 * two full processes for every test.
 *
 * Running two LEADERS concurrently (i.e. bypassing the lock) would not
 * corrupt data -- mark-sent is a conditional UPDATE, so double-publish is
 * possible but duplicate consumer effects are not (see consumer.ts) -- but
 * would not give the per-task ordering guarantee this module is meant to
 * provide. See publisher-lock.ts for exactly what the lock does and does
 * not guarantee.
 */
export async function runPublisherOnce(
  pool: mysql.Pool,
  js: JetStreamClient,
  options: PublisherOptions = DEFAULT_PUBLISHER_OPTIONS,
): Promise<number> {
  const [rows] = await pool.query<OutboxRow[]>(
    `SELECT * FROM outbox_events WHERE published_at IS NULL ORDER BY id ASC LIMIT ?`,
    [options.batchSize],
  );
  let published = 0;
  for (const row of rows) {
    await publishOne(pool, js, row, options.maxRetries);
    published++;
  }
  return published;
}

async function publishOne(pool: mysql.Pool, js: JetStreamClient, row: OutboxRow, maxRetries: number): Promise<void> {
  const envelope = {
    version: 1 as const,
    eventId: row.event_id,
    taskId: row.task_id,
    seq: row.seq,
    type: row.type,
    payload: row.payload,
    createdAtMs: row.created_at.getTime(),
  };
  const data = new TextEncoder().encode(JSON.stringify(envelope));

  // Restores the trace that originally committed this outbox row (see
  // task-store.ts's captureCurrentCarrier call) as the PARENT context for
  // this publish span -- there is exactly one logical publish lineage per
  // outbox row (even across internal retries below), so parent/child is
  // the right relationship here, unlike the consumer side (see
  // consumer.ts), which uses a link instead because many independent
  // delivery ATTEMPTS can occur for one message.
  // mysql2 returns a JSON column already parsed, but guard against a
  // string (e.g. a future driver/version change) rather than assume.
  const rowCarrier = typeof row.trace_carrier === "string" ? safeParseCarrier(row.trace_carrier) : row.trace_carrier;
  const parentCtx = contextFromCarrier(rowCarrier);
  const tracer = getTracer();

  await otelContext.with(parentCtx, () =>
    tracer.startActiveSpan("relaylab.outbox.publish_attempt", async (span) => {
      setRelaylabAttributes(span, { taskId: row.task_id, eventId: row.event_id, eventType: row.type, seq: row.seq });
      // Injected AFTER the publish span is created (and thus active),
      // not the command span's original carrier -- the consumer should
      // link back to THIS publish attempt (the thing that actually put
      // the message on the wire), not to the command that happened
      // earlier and may be long finished. See src/messaging/consumer.ts
      // "carrierFromNatsHeaders" and docs/OBSERVABILITY.md.
      const hdrs = natsHeaders();
      const publishCarrier = captureCurrentCarrier();
      if (publishCarrier?.traceparent) hdrs.set("traceparent", publishCarrier.traceparent);
      if (publishCarrier?.tracestate) hdrs.set("tracestate", publishCarrier.tracestate);
      // Phase 1.2.1 Part C: one `relaylab_publish_attempts_total`/duration
      // observation per outbox row per `publishOne` call -- covering the
      // WHOLE internal retry loop below, not one observation per retry
      // (see docs/METRICS.md's dictionary entry: "once per outbox row per
      // call", matching the span's own granularity).
      const startedAtMs = Date.now();
      try {
        let lastErr: unknown;
        for (let attempt = 0; attempt <= maxRetries; attempt++) {
          span.setAttribute("relaylab.delivery_attempt", attempt + 1);
          try {
            // msgID enables broker-side dedup as a bounded-window optimization;
            // it is NOT the correctness mechanism (that is the consumer's
            // durable application-level dedup in consumer.ts).
            await js.publish(eventsSubject(), data, { msgID: row.event_id, headers: hdrs });
            // Section 6 crash-window A: broker ack received, mark-sent not yet
            // committed. No-op unless RELAYLAB_FAILPOINT=after-publish-ack-before-mark-sent.
            await hitFailpoint("after-publish-ack-before-mark-sent");
            await pool.query(`UPDATE outbox_events SET published_at = NOW(3) WHERE event_id = ? AND published_at IS NULL`, [
              row.event_id,
            ]);
            setRelaylabAttributes(span, { outcome: "published" });
            recordPublishAttempt("published", (Date.now() - startedAtMs) / 1000);
            return;
          } catch (err) {
            lastErr = err;
            await sleep(Math.min(1000, 50 * 2 ** attempt));
          }
        }
        setRelaylabAttributes(span, { outcome: "publish_failed" });
        span.setStatus({ code: SpanStatusCode.ERROR, message: "publish_failed" });
        recordPublishAttempt("failed", (Date.now() - startedAtMs) / 1000);
        throw new Error(`publish failed for event ${row.event_id} after ${maxRetries + 1} attempts: ${String(lastErr)}`);
      } finally {
        span.end();
      }
    }),
  );
}

function safeParseCarrier(raw: string): TraceCarrier | null {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
