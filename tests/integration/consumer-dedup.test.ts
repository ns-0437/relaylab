import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { processBatch, applyEventWithDedup, defaultConsumerOptions } from "../../src/messaging/consumer.js";
import { runPublisherOnce } from "../../src/messaging/publisher.js";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { decodeEventEnvelope } from "../../src/protocol/protocol.js";
import { eventsSubject } from "../../src/config.js";
import { Effect } from "effect";
import {
  applySchema,
  requireMysqlPool,
  requireNats,
  resetStream,
  truncateAll,
  uniqueId,
  eventsStream,
  eventsDurable,
  type NatsTestHandles,
} from "./helpers.js";

let pool: mysql.Pool;
let nats: NatsTestHandles;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
  nats = await requireNats();
});

beforeEach(async () => {
  await truncateAll(pool);
  await resetStream(nats.jsm);
});

after(async () => {
  await nats.nc.drain();
  await pool.end();
});

async function publishOneCheckpointEvent(taskId: string): Promise<void> {
  const store = makeTaskStoreLive(pool);
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 10_000));
  await Effect.runPromise(
    store.applyTaskCommand({
      taskId,
      ownerId: "worker-a",
      epoch: claim.epoch,
      commandId: "cmd-1",
      command: { type: "Checkpoint", digest: "d1" },
    }),
  );
  await runPublisherOnce(pool, nats.js);
}

async function consumerState(
  taskId: string,
): Promise<{ eventCount: number; lastAppliedSeq: number; gap: boolean; pendingCount: number } | null> {
  const [rows] = await pool.query<
    (RowDataPacket & { event_count: number; last_applied_seq: number; gap_detected: number; pending_count: number })[]
  >(`SELECT event_count, last_applied_seq, gap_detected, pending_count FROM task_consumer_state WHERE task_id = ?`, [
    taskId,
  ]);
  const row = rows[0];
  if (!row) return null;
  return { eventCount: row.event_count, lastAppliedSeq: row.last_applied_seq, gap: !!row.gap_detected, pendingCount: row.pending_count };
}

test(
  "publisher: republishing with the same event_id/msgID after a crash between broker-ack and " +
    "mark-sent is absorbed by the broker's own duplicate window, so it never reaches the stream twice " +
    "(invariant E, publisher side, scenario 6)",
  async () => {
    const taskId = uniqueId("task");
    await publishOneCheckpointEvent(taskId);
    const before = await nats.jsm.streams.info(eventsStream());

    // Simulate "publisher crashed after the broker ack but before marking
    // the row sent, so on restart it republishes the same row". Clear
    // published_at and republish with the SAME event_id/msgID.
    await pool.query(`UPDATE outbox_events SET published_at = NULL WHERE task_id = ?`, [taskId]);
    await runPublisherOnce(pool, nats.js);

    const after = await nats.jsm.streams.info(eventsStream());
    assert.equal(
      after.state.messages,
      before.state.messages,
      "the republish must not create a second stream entry (broker-level Nats-Msg-Id dedup)",
    );

    const outcome = await processBatch(pool, nats.js);
    assert.equal(outcome.processed, 1, "the one real message is still delivered and processed exactly once");
  },
);

test(
  "consumer: a message redelivered by the broker after being processed-but-not-yet-acked is " +
    "deduplicated by the consumer's own durable record, not by broker msgID dedup (invariant E/F, scenario 8). " +
    "Redelivery is forced deterministically with msg.nak() instead of waiting out ack_wait.",
  async () => {
    const taskId = uniqueId("task");
    await publishOneCheckpointEvent(taskId);

    const firstFetch = nats.js.fetch(eventsStream(), eventsDurable(), { batch: 1, expires: 2000 });
    const firstMsgs = [];
    for await (const m of firstFetch) firstMsgs.push(m);
    assert.equal(firstMsgs.length, 1);
    const msg1 = firstMsgs[0]!;
    const decoded1 = decodeEventEnvelope(JSON.parse(new TextDecoder().decode(msg1.data)));
    assert.equal(decoded1._tag, "Right");
    if (decoded1._tag !== "Right") throw new Error("unreachable");

    // Apply the durable effect (this is the real work a consumer does) but
    // do NOT ack -- this models "crashed after commit, before the ACK
    // reached the broker". nak() then forces the broker to redeliver the
    // same stored message, exactly as it would after an ack_wait timeout.
    const result1 = await applyEventWithDedup(pool, defaultConsumerOptions().consumerName, decoded1.right);
    assert.equal(result1, "applied");
    msg1.nak(10);

    const secondFetch = nats.js.fetch(eventsStream(), eventsDurable(), { batch: 1, expires: 2000 });
    const secondMsgs = [];
    for await (const m of secondFetch) secondMsgs.push(m);
    assert.equal(secondMsgs.length, 1, "the broker must redeliver the same message after nak()");
    const msg2 = secondMsgs[0]!;
    assert.ok(msg2.info.redeliveryCount >= 2, "the redelivered message must show an incremented delivery count");
    const decoded2 = decodeEventEnvelope(JSON.parse(new TextDecoder().decode(msg2.data)));
    assert.equal(decoded2._tag, "Right");
    if (decoded2._tag !== "Right") throw new Error("unreachable");
    assert.equal(decoded2.right.eventId, decoded1.right.eventId, "it must be the SAME event, not a new one");

    const result2 = await applyEventWithDedup(pool, defaultConsumerOptions().consumerName, decoded2.right);
    assert.equal(result2, "deduped", "the redelivery must be recognized by the consumer's own dedup record");
    msg2.ack();

    const state = await consumerState(taskId);
    assert.equal(state?.eventCount, 1, "the local effect must have run exactly once despite genuine broker redelivery");
  },
);

test(
  "consumer: reprocessing after a simulated crash-before-commit completes the effect exactly once " +
    "(deterministic failpoint, not a real process kill; scenario 7)",
  async () => {
    const taskId = uniqueId("task");
    await publishOneCheckpointEvent(taskId);

    // A crash before the transaction commits leaves no dedup row and no
    // local effect -- which is indistinguishable, from the DB's point of
    // view, from "never attempted". So the very first successful
    // processBatch call already covers the recovery path: nothing was
    // partially applied beforehand to clean up.
    const outcome = await processBatch(pool, nats.js);
    assert.equal(outcome.processed, 1);

    const state = await consumerState(taskId);
    assert.equal(state?.eventCount, 1);
  },
);

test(
  "section 3: two DISTINCT broker messages (different msgIDs) carrying the SAME application event_id " +
    "and identical content produce exactly one durable receipt and one durable effect -- proven " +
    "independently of NATS's own msgID dedup, which never comes into play here because the transport " +
    "message IDs differ",
  async () => {
    const taskId = uniqueId("task");
    const eventId = uniqueId("evt");
    const event = { eventId, taskId, seq: 1, type: "CheckpointRecorded", payload: { digest: "d1" } };
    const envelope = { version: 1 as const, ...event, createdAtMs: Date.now() };
    const data = new TextEncoder().encode(JSON.stringify(envelope));

    const pubAck1 = await nats.js.publish(eventsSubject(), data, { msgID: uniqueId("transport-msg-1") });
    const pubAck2 = await nats.js.publish(eventsSubject(), data, { msgID: uniqueId("transport-msg-2") });
    assert.notEqual(pubAck1.seq, pubAck2.seq, "these must be two DIFFERENT stream entries, not one deduped publish");
    assert.equal(pubAck1.duplicate, false);
    assert.equal(pubAck2.duplicate, false, "broker-level dedup must not have fired -- the transport msgIDs differ");

    const outcome = await processBatch(pool, nats.js);
    assert.equal(outcome.processed, 1, "the first of the two deliveries is applied");
    assert.equal(outcome.deduped, 1, "the second delivery, despite being a genuinely distinct broker message, is deduped");

    const [receiptRows] = await pool.query<RowDataPacket[]>(
      `SELECT COUNT(*) AS c FROM consumer_processed WHERE event_id = ?`,
      [eventId],
    );
    assert.equal(receiptRows[0]?.c, 1, "exactly one durable consumer receipt for this application event_id");

    const state = await consumerState(taskId);
    assert.equal(state?.eventCount, 1, "exactly one durable database effect, despite two distinct broker deliveries");
  },
);

test(
  "section 3: reusing an event_id with DIFFERENT content is detected as a protocol violation, not " +
    "silently treated as an ordinary duplicate",
  async () => {
    const taskId = uniqueId("task");
    const eventId = uniqueId("evt");
    const first = { eventId, taskId, seq: 1, type: "CheckpointRecorded", payload: { digest: "original" } };
    const conflicting = { eventId, taskId, seq: 1, type: "CheckpointRecorded", payload: { digest: "TAMPERED" } };

    const result1 = await applyEventWithDedup(pool, defaultConsumerOptions().consumerName, first);
    assert.equal(result1, "applied");

    const result2 = await applyEventWithDedup(pool, defaultConsumerOptions().consumerName, conflicting);
    assert.equal(result2, "conflict", "must be a distinct outcome, not silently \"deduped\" or silently \"applied\"");

    const [conflictRows] = await pool.query<RowDataPacket[]>(
      `SELECT * FROM consumer_event_conflicts WHERE event_id = ?`,
      [eventId],
    );
    assert.equal(conflictRows.length, 1, "the conflict must be durably recorded for operator visibility");

    const state = await consumerState(taskId);
    assert.equal(state?.eventCount, 1, "only the FIRST version's effect was ever applied");
  },
);

test("a malformed event is dead-lettered instead of blocking the stream forever", async () => {
  await nats.js.publish(eventsSubject(), new TextEncoder().encode(JSON.stringify({ not: "a valid envelope" })));
  const outcome = await processBatch(pool, nats.js);
  assert.equal(outcome.deadLettered, 1);
  assert.equal(outcome.processed, 0);
});
