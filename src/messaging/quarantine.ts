// Phase 1.2.1 patch (Part A.2, strengthened by the monitoring-
// reliability patch section 3): durable dead-letter quarantine. See
// infra/migrations/0004_consumer_quarantine.sql +
// infra/migrations/0005_quarantine_stream_identity.sql for the table and
// its identity/retention rationale, and docs/OBSERVABILITY.md "dead-
// letter quarantine" for the full data-handling policy.
import type mysql from "mysql2/promise";
import type { ResultSetHeader, RowDataPacket } from "mysql2/promise";
import type { JsMsg } from "nats";
import { recordQuarantineAttempt } from "../telemetry/metrics.js";

export type QuarantineReason = "malformed_json" | "schema_invalid" | "max_redeliveries_exceeded";

const MAX_EXCERPT_BYTES = 2000;

export interface QuarantineDetails {
  eventId?: string;
  taskId?: string;
}

/**
 * The identity a quarantine row's dedup key is built from. Section 3 of
 * the monitoring-reliability patch asked: "can two streams share a
 * consumer name? can a stream be deleted and recreated while quarantine
 * records remain? can development tests reset stream sequences
 * independently of MySQL?" -- the answer to all three, in this project,
 * is YES (nothing couples NATS stream lifecycle to MySQL table
 * lifecycle; `tests/integration/helpers.ts`'s `truncateAll` and
 * `resetStream` are two entirely separate operations against two
 * entirely separate systems). A PRIOR stream incarnation's quarantine
 * rows are therefore NOT guaranteed to be absent when a NEW incarnation
 * starts handing out LOW sequence numbers again from 1 -- `nats_stream_seq`
 * ALONE is not a safe identity across that boundary.
 *
 * `streamGeneration` (the stream's own NATS-assigned creation timestamp,
 * `StreamInfo.created` -- see src/messaging/nats-client.ts
 * `StreamIdentity`) is deliberately the SIMPLEST thing that closes this
 * gap without inventing a new generation-tracking subsystem: it already
 * exists, is already queried once at consumer startup, and changes
 * exactly when it needs to (stream deleted and recreated), never
 * otherwise (ordinary purges/redeliveries never change it).
 */
export interface StreamQuarantineIdentity {
  consumerName: string;
  streamName: string;
  streamGeneration: string;
}

export type QuarantineWriteResult = "inserted" | "already_present";

interface ExistsRow extends RowDataPacket {
  n: number;
}

/**
 * Writes one durable quarantine record BEFORE the caller acks the
 * message -- callers MUST NOT ack if this throws (a DB failure, OR an
 * anomalous INSERT IGNORE outcome -- see below -- means the quarantine
 * evidence was NOT safely stored, so the message should redeliver and
 * this gets attempted again, not silently treated as already handled).
 *
 * **Auditing `INSERT IGNORE`** (section 3 of the monitoring-reliability
 * patch): `INSERT IGNORE` in MySQL suppresses EVERY error that would
 * otherwise abort the statement, converting it to a warning -- not only
 * the intended duplicate-primary-key case, but also, depending on
 * `sql_mode`, things like silent data truncation. The ORIGINAL code
 * could not tell these apart: `affectedRows === 0` was simply assumed to
 * mean "already present." This version distinguishes them explicitly:
 *   - `affectedRows === 1` => a genuinely NEW row was inserted. If
 *     `warningStatus` is ALSO nonzero (e.g. a value was silently
 *     truncated/converted), that is logged loudly even though the row
 *     did get stored, since it means the stored evidence may not be
 *     byte-for-byte what was intended.
 *   - `affectedRows === 0` => EITHER a genuine duplicate-key collision
 *     (the expected, safe case for a redelivery -- confirmed with a
 *     follow-up existence check against the SAME composite key) OR an
 *     anomaly (the row does NOT exist despite `affectedRows === 0`,
 *     meaning `INSERT IGNORE` swallowed some OTHER problem and the
 *     intended record was never actually stored). The anomaly case
 *     THROWS -- the caller must not ack a message whose quarantine
 *     evidence was silently lost.
 */
export async function writeQuarantineRecord(
  pool: mysql.Pool,
  identity: StreamQuarantineIdentity,
  msg: JsMsg,
  reason: QuarantineReason,
  details: QuarantineDetails = {},
): Promise<QuarantineWriteResult> {
  const truncated = msg.data.byteLength > MAX_EXCERPT_BYTES;
  const excerpt = Buffer.from(msg.data.subarray(0, MAX_EXCERPT_BYTES));
  const { consumerName, streamName, streamGeneration } = identity;

  let header: ResultSetHeader;
  try {
    [header] = await pool.query<ResultSetHeader>(
      `INSERT IGNORE INTO consumer_quarantine
         (consumer_name, stream_name, stream_generation, nats_stream_seq, reason, event_id, task_id, delivery_attempt, payload_excerpt, payload_truncated)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [consumerName, streamName, streamGeneration, msg.seq, reason, details.eventId ?? null, details.taskId ?? null, msg.info.redeliveryCount, excerpt, truncated ? 1 : 0],
    );
  } catch (err) {
    recordQuarantineAttempt(reason, "write_failed");
    throw err;
  }

  if (header.affectedRows >= 1) {
    if (header.warningStatus !== 0) {
      // The row WAS stored, but MySQL also raised a warning (e.g. a
      // silently truncated/converted value) -- surfaced loudly, never
      // silently swallowed, even though this is not a write FAILURE.
      console.error(
        `[quarantine] INSERT IGNORE reported ${header.warningStatus} warning(s) while still storing the row ` +
          `(consumer=${consumerName}, stream=${streamName}, seq=${msg.seq}) -- inspect with SHOW WARNINGS / the row itself.`,
      );
    }
    recordQuarantineAttempt(reason, "recorded");
    return "inserted";
  }

  // affectedRows === 0: confirm this is the EXPECTED duplicate-key case,
  // not an anomaly that silently dropped the intended record.
  const [existing] = await pool.query<ExistsRow[]>(
    `SELECT 1 AS n FROM consumer_quarantine
     WHERE consumer_name = ? AND stream_name = ? AND stream_generation = ? AND nats_stream_seq = ?`,
    [consumerName, streamName, streamGeneration, msg.seq],
  );
  if (existing.length === 0) {
    recordQuarantineAttempt(reason, "write_failed");
    throw new Error(
      `[quarantine] INSERT IGNORE reported 0 affected rows but no matching row exists ` +
        `(consumer=${consumerName}, stream=${streamName}, generation=${streamGeneration}, seq=${msg.seq}) -- ` +
        `the intended quarantine record was NOT durably stored. Refusing to treat this as a safe duplicate.`,
    );
  }
  recordQuarantineAttempt(reason, "recorded");
  return "already_present";
}
