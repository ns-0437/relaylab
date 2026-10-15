// Phase 1.2.1 patch, Part A.2: durable dead-letter quarantine. A review
// found the prior "dead-lettering" behavior only logged a line and
// acked -- no durable record of what was rejected or why. These tests
// drive real NATS messages (some deliberately malformed/invalid) through
// the real consumer handling path and verify consumer_quarantine rows
// against real MySQL. See src/messaging/quarantine.ts and
// infra/migrations/0004_consumer_quarantine.sql.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { defaultConsumerOptions, handleMessage, type ProcessOutcome } from "../../src/messaging/consumer.js";
import { writeQuarantineRecord } from "../../src/messaging/quarantine.js";
import { ensureEventsStream } from "../../src/messaging/nats-client.js";
import { eventsSubject, eventsStream, eventsDurable } from "../../src/config.js";
import { applySchema, requireMysqlPool, requireNats, resetStream, truncateAll, uniqueId, type NatsTestHandles } from "./helpers.js";

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

function freshOutcome(): ProcessOutcome {
  return { processed: 0, deduped: 0, deadLettered: 0, buffered: 0, conflicts: 0 };
}

async function fetchOne() {
  const iter = nats.js.fetch(eventsStream(), eventsDurable(), { batch: 1, expires: 3000 });
  for await (const msg of iter) return msg;
  throw new Error("expected exactly one message, got none");
}

interface QuarantineRow extends RowDataPacket {
  reason: string;
  event_id: string | null;
  task_id: string | null;
  delivery_attempt: number;
  payload_excerpt: Buffer | null;
}

async function quarantineRows(): Promise<QuarantineRow[]> {
  const [rows] = await pool.query<QuarantineRow[]>(`SELECT * FROM consumer_quarantine ORDER BY nats_stream_seq`);
  return rows;
}

test("malformed JSON is durably quarantined (not just logged) BEFORE ack, with no application event_id/task_id available", async () => {
  await nats.js.publish(eventsSubject(), Buffer.from("this is not { valid json"));
  const msg = await fetchOne();
  const outcome = freshOutcome();

  await handleMessage(pool, msg, defaultConsumerOptions(), outcome);

  assert.equal(outcome.deadLettered, 1);
  const rows = await quarantineRows();
  assert.equal(rows.length, 1);
  assert.equal(rows[0]!.reason, "malformed_json");
  assert.equal(rows[0]!.event_id, null, "malformed JSON never decodes far enough to have an event_id");
  assert.equal(rows[0]!.task_id, null);
  assert.ok(rows[0]!.payload_excerpt?.toString("utf8").includes("this is not"), "the raw bytes must be stored for diagnosis");
});

test("schema-invalid (well-formed JSON, wrong shape) is durably quarantined with a DIFFERENT reason than malformed JSON", async () => {
  await nats.js.publish(eventsSubject(), Buffer.from(JSON.stringify({ foo: "bar", notAnEventEnvelope: true })));
  const msg = await fetchOne();
  const outcome = freshOutcome();

  await handleMessage(pool, msg, defaultConsumerOptions(), outcome);

  assert.equal(outcome.deadLettered, 1);
  const rows = await quarantineRows();
  assert.equal(rows.length, 1);
  assert.equal(rows[0]!.reason, "schema_invalid");
  assert.equal(rows[0]!.event_id, null);
});

test("an event that exceeds maxDeliverBeforeDeadLetter is durably quarantined WITH its event_id/task_id (it decoded successfully)", async () => {
  const taskId = uniqueId("task");
  const eventId = uniqueId("event");
  const envelope = { version: 1, eventId, taskId, seq: 1, type: "Checkpoint", payload: { digest: "d1" }, createdAtMs: Date.now() };
  await nats.js.publish(eventsSubject(), Buffer.from(JSON.stringify(envelope)));
  const msg = await fetchOne();
  const outcome = freshOutcome();

  // maxDeliverBeforeDeadLetter: -1 makes even a first delivery
  // (redeliveryCount 0) exceed the bound, so this test does not have to
  // wait out real redeliveries to exercise the branch.
  await handleMessage(pool, msg, { ...defaultConsumerOptions(), maxDeliverBeforeDeadLetter: -1 }, outcome);

  assert.equal(outcome.deadLettered, 1);
  const rows = await quarantineRows();
  assert.equal(rows.length, 1);
  assert.equal(rows[0]!.reason, "max_redeliveries_exceeded");
  assert.equal(rows[0]!.event_id, eventId);
  assert.equal(rows[0]!.task_id, taskId);
});

test("if durable quarantine-write fails, the message is NOT acked -- redelivery retries quarantining rather than treating unstored evidence as safe", async () => {
  await nats.js.publish(eventsSubject(), Buffer.from("still not { valid json"));
  const msg1 = await fetchOne();
  const outcome = freshOutcome();

  // Simulate a quarantine-storage failure by making the table
  // momentarily unavailable to INSERTs.
  await pool.query(`RENAME TABLE consumer_quarantine TO consumer_quarantine_disabled_for_test`);
  try {
    await assert.rejects(
      () => handleMessage(pool, msg1, defaultConsumerOptions(), outcome),
      "a quarantine-write failure must propagate, not be swallowed",
    );
  } finally {
    await pool.query(`RENAME TABLE consumer_quarantine_disabled_for_test TO consumer_quarantine`);
  }

  assert.equal(outcome.deadLettered, 0, "the failed attempt must not have counted as successfully handled");
  assert.equal((await quarantineRows()).length, 0, "no row can exist -- the insert never happened");

  // Force redelivery of the SAME physical message (same nats_stream_seq)
  // rather than waiting out the consumer's real ack_wait.
  msg1.nak(10);
  const msg2 = await fetchOne();
  assert.ok(msg2.info.redeliveryCount >= 1, "this must be a genuine redelivery of the same message");

  await handleMessage(pool, msg2, defaultConsumerOptions(), outcome);
  assert.equal(outcome.deadLettered, 1, "the retried attempt, with the table restored, must now succeed and ack");
  const rows = await quarantineRows();
  assert.equal(rows.length, 1, "exactly one durable record for this one physical message, despite two delivery attempts");
  assert.equal(rows[0]!.reason, "malformed_json");
});

test("redelivery of an ALREADY-quarantined message (quarantine row committed, but the broker never received the ack) deduplicates via nats_stream_seq instead of erroring or double-recording", async () => {
  const options = defaultConsumerOptions();
  await nats.js.publish(eventsSubject(), Buffer.from("not json at all"));
  const msg1 = await fetchOne();

  // Simulates handleMessage having reached "quarantine row committed"
  // on a PRIOR attempt, then crashing/losing its connection before the
  // broker ack was received -- write the row directly (what
  // handleMessage would have done), then nak() WITHOUT ever acking, so
  // the broker genuinely redelivers the identical physical message.
  await writeQuarantineRecord(
    pool,
    { consumerName: options.consumerName, streamName: options.streamName, streamGeneration: options.streamGeneration },
    msg1,
    "malformed_json",
  );
  assert.equal((await quarantineRows()).length, 1);
  msg1.nak(10);

  const msg2 = await fetchOne();
  assert.equal(msg2.seq, msg1.seq, "must be a genuine redelivery of the SAME physical message");
  const outcome = freshOutcome();
  await handleMessage(pool, msg2, options, outcome);

  const rows = await quarantineRows();
  assert.equal(rows.length, 1, "INSERT IGNORE on the (consumer_name, stream_name, stream_generation, nats_stream_seq) primary key must prevent a duplicate row");
  assert.equal(outcome.deadLettered, 1, "the redelivered attempt is still counted as handled/acked, even though its insert was a no-op");
});

test("section 3: a quarantine record from a DIFFERENT stream generation sharing the SAME low sequence number does NOT collide -- both are stored", async () => {
  const identityGenA = { consumerName: "consumer-x", streamName: "stream-x", streamGeneration: "2026-01-01T00:00:00.000Z" };
  const identityGenB = { consumerName: "consumer-x", streamName: "stream-x", streamGeneration: "2026-02-02T00:00:00.000Z" };

  await nats.js.publish(eventsSubject(), Buffer.from("collision-probe-a"));
  const msgA = await fetchOne();
  await nats.js.publish(eventsSubject(), Buffer.from("collision-probe-b"));
  const msgB = await fetchOne();

  // Force both messages to report the SAME low sequence number, as a
  // genuinely recreated stream would hand out to its first message --
  // this is exactly the collision scenario section 3 describes. A Proxy
  // (not a shallow `{...msg}` spread, which loses JsMsg's
  // getter-backed `.data`/`.info`) overrides ONLY `seq`, delegating
  // everything else to the real message.
  const fakeSeq = 1;
  function withFakeSeq(msg: typeof msgA): typeof msgA {
    return new Proxy(msg, { get: (target, prop) => (prop === "seq" ? fakeSeq : (target as any)[prop]) });
  }
  const msgAWithFakeSeq = withFakeSeq(msgA);
  const msgBWithFakeSeq = withFakeSeq(msgB);

  const resultA = await writeQuarantineRecord(pool, identityGenA, msgAWithFakeSeq, "malformed_json");
  const resultB = await writeQuarantineRecord(pool, identityGenB, msgBWithFakeSeq, "malformed_json");

  assert.equal(resultA, "inserted");
  assert.equal(resultB, "inserted", "a DIFFERENT stream_generation must NOT be treated as a duplicate of generation A's seq=1 row");

  const [rows] = await pool.query<(RowDataPacket & { stream_generation: string; payload_excerpt: Buffer })[]>(
    `SELECT stream_generation, payload_excerpt FROM consumer_quarantine WHERE consumer_name = 'consumer-x' AND stream_name = 'stream-x' ORDER BY stream_generation`,
  );
  assert.equal(rows.length, 2, "both generations' seq=1 rows must durably coexist, neither silently dropped by the other");
  assert.equal(rows[0]!.payload_excerpt.toString("utf8"), "collision-probe-a");
  assert.equal(rows[1]!.payload_excerpt.toString("utf8"), "collision-probe-b");
});

test("section 3: writeQuarantineRecord reports whether the row was newly inserted vs already present", async () => {
  await nats.js.publish(eventsSubject(), Buffer.from("report-probe"));
  const msg = await fetchOne();
  const identity = { consumerName: "consumer-report", streamName: "stream-report", streamGeneration: "gen-1" };

  const first = await writeQuarantineRecord(pool, identity, msg, "malformed_json");
  assert.equal(first, "inserted");

  const second = await writeQuarantineRecord(pool, identity, msg, "malformed_json");
  assert.equal(second, "already_present", "a genuine duplicate-key collision must be reported as such, not conflated with a fresh insert");
});

test("section 3: a real ensureEventsStream() identity is what consumer-main.ts-style code actually threads through to the quarantine key", async () => {
  const identity = await ensureEventsStream(nats.jsm);
  assert.ok(identity.name.length > 0);
  assert.ok(identity.generation.length > 0, "the stream's real creation timestamp must be non-empty");

  await nats.js.publish(eventsSubject(), Buffer.from("real-identity-probe"));
  const msg = await fetchOne();
  const result = await writeQuarantineRecord(
    pool,
    { consumerName: eventsDurable(), streamName: identity.name, streamGeneration: identity.generation },
    msg,
    "malformed_json",
  );
  assert.equal(result, "inserted");
  const [rows] = await pool.query<(RowDataPacket & { stream_name: string; stream_generation: string })[]>(
    `SELECT stream_name, stream_generation FROM consumer_quarantine WHERE consumer_name = ?`,
    [eventsDurable()],
  );
  assert.equal(rows[0]!.stream_name, identity.name);
  assert.equal(rows[0]!.stream_generation, identity.generation);
});
