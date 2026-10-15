// Section 5 of the Phase 1.1 audit: event-gap behavior must be explicit
// and bounded, never silently advance a "complete" projection past a
// missing event, and never create a permanent retry loop. See
// src/messaging/consumer.ts (applyEventWithDedup) and
// docs/decisions/0003-event-ordering.md for the chosen policy: durable
// pending events with ordered (cascade) application once the gap closes.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { applyEventWithDedup, defaultConsumerOptions, type InboundEvent } from "../../src/messaging/consumer.js";
import { loadTestMysqlConfig } from "../../src/config.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";

let pool: mysql.Pool;
const consumerName = () => defaultConsumerOptions().consumerName;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
});

beforeEach(async () => {
  await truncateAll(pool);
});

after(async () => {
  await pool.end();
});

function event(taskId: string, seq: number, digest = `d${seq}`): InboundEvent {
  return { eventId: uniqueId(`evt-seq${seq}`), taskId, seq, type: "CheckpointRecorded", payload: { digest } };
}

async function state(taskId: string) {
  const [rows] = await pool.query<
    (RowDataPacket & { event_count: number; last_applied_seq: number; gap_detected: number; pending_count: number })[]
  >(`SELECT event_count, last_applied_seq, gap_detected, pending_count FROM task_consumer_state WHERE task_id = ?`, [
    taskId,
  ]);
  const row = rows[0];
  return row && {
    eventCount: row.event_count,
    lastAppliedSeq: row.last_applied_seq,
    gap: !!row.gap_detected,
    pendingCount: row.pending_count,
  };
}

async function pendingRows(taskId: string) {
  const [rows] = await pool.query<RowDataPacket[]>(
    `SELECT seq FROM task_consumer_pending WHERE consumer_name = ? AND task_id = ? ORDER BY seq`,
    [consumerName(), taskId],
  );
  return rows.map((r) => r.seq as number);
}

test("1, 3, 2: seq 3 is acknowledged-equivalent (durably buffered, not lost) but NOT applied; arrival of 2 restores correctness", async () => {
  const taskId = uniqueId("task");
  const e1 = event(taskId, 1);
  const e3 = event(taskId, 3);
  const e2 = event(taskId, 2);

  const r1 = await applyEventWithDedup(pool, consumerName(), e1);
  assert.equal(r1, "applied");
  let s = await state(taskId);
  assert.equal(s?.eventCount, 1);
  assert.equal(s?.lastAppliedSeq, 1);
  assert.equal(s?.gap, false, "no gap yet -- only seq 1 has arrived");

  const r3 = await applyEventWithDedup(pool, consumerName(), e3);
  assert.equal(r3, "buffered", "seq 3 must NOT be applied while seq 2 is missing");
  s = await state(taskId);
  assert.equal(s?.eventCount, 1, "the projection must not silently advance past the missing seq 2");
  assert.equal(s?.lastAppliedSeq, 1, "projection stays at 1, explicitly incomplete");
  assert.equal(s?.gap, true, "the projection must show itself as incomplete");
  assert.deepEqual(await pendingRows(taskId), [3], "seq 3 is durably stored for later, not dropped");

  const r2 = await applyEventWithDedup(pool, consumerName(), e2);
  assert.equal(r2, "applied");
  s = await state(taskId);
  assert.equal(s?.lastAppliedSeq, 3, "arrival of seq 2 must cascade-apply the buffered seq 3 too");
  assert.equal(s?.eventCount, 3);
  assert.equal(s?.gap, false, "the gap must clear once the sequence is contiguous again");
  assert.deepEqual(await pendingRows(taskId), [], "the pending buffer must be drained");
});

test("duplicate events around the gap do not double-count and do not disturb the buffered item", async () => {
  const taskId = uniqueId("task");
  const e1 = event(taskId, 1);
  const e3 = event(taskId, 3);

  await applyEventWithDedup(pool, consumerName(), e1);
  await applyEventWithDedup(pool, consumerName(), e3);

  // Redeliveries of both the applied event and the buffered event.
  const dup1 = await applyEventWithDedup(pool, consumerName(), e1);
  assert.equal(dup1, "deduped");
  const dup3 = await applyEventWithDedup(pool, consumerName(), e3);
  assert.equal(dup3, "deduped", "a redelivered buffered event must not be buffered a second time or counted twice");

  const s = await state(taskId);
  assert.equal(s?.eventCount, 1);
  assert.equal(s?.pendingCount, 1);
  assert.deepEqual(await pendingRows(taskId), [3]);
});

test("restart while the gap exists: the buffered event and gap flag survive (they live in MySQL, not process memory), and a fresh connection completes the sequence correctly", async () => {
  const taskId = uniqueId("task");
  await applyEventWithDedup(pool, consumerName(), event(taskId, 1));
  await applyEventWithDedup(pool, consumerName(), event(taskId, 3));

  // Simulate a consumer process restart: open a BRAND NEW pool (new TCP
  // connections, no shared in-process state) and confirm the gap state is
  // read back durably from MySQL, not lost.
  const freshPool = mysql.createPool({ ...loadTestMysqlConfig(), waitForConnections: true, connectionLimit: 5 });
  try {
    const [rows] = await freshPool.query<RowDataPacket[]>(
      `SELECT gap_detected, last_applied_seq FROM task_consumer_state WHERE task_id = ?`,
      [taskId],
    );
    assert.equal(rows[0]?.gap_detected, 1, "the gap must be visible to a completely new connection, proving durability");
    assert.equal(rows[0]?.last_applied_seq, 1);

    const [pendingAfterRestart] = await freshPool.query<RowDataPacket[]>(
      `SELECT seq FROM task_consumer_pending WHERE consumer_name = ? AND task_id = ?`,
      [consumerName(), taskId],
    );
    assert.equal(pendingAfterRestart.length, 1, "the buffered seq 3 must still be there after the simulated restart");

    const result = await applyEventWithDedup(freshPool, consumerName(), event(taskId, 2));
    assert.equal(result, "applied");
    const finalState = await state(taskId);
    assert.equal(finalState?.lastAppliedSeq, 3, "the new connection completes the cascade correctly");
    assert.equal(finalState?.gap, false);
  } finally {
    await freshPool.end();
  }
});

test("a gap that never closes is bounded: buffering is capped per task, logged loudly, and never grows without limit", async () => {
  const taskId = uniqueId("task");
  const cap = 5;
  await applyEventWithDedup(pool, consumerName(), event(taskId, 1));

  // seq 2 never arrives. Seq 3..3+cap+2 all arrive "early" and can never
  // apply. Confirm the pending table stops growing at the cap rather than
  // accepting every one of them.
  const results: string[] = [];
  for (let seq = 3; seq < 3 + cap + 3; seq++) {
    results.push(await applyEventWithDedup(pool, consumerName(), event(taskId, seq), cap));
  }
  assert.ok(
    results.every((r) => r === "buffered"),
    "every early-arriving event must still be acked-equivalent (handled, not left to redeliver forever)",
  );

  const pending = await pendingRows(taskId);
  assert.ok(pending.length <= cap, `pending buffer must be bounded at ${cap}, got ${pending.length}`);

  const s = await state(taskId);
  assert.equal(s?.gap, true, "a permanently-stuck gap must remain visible for operator attention");
  assert.equal(s?.lastAppliedSeq, 1, "the projection correctly never advances past the permanent gap");

  // Operational visibility: a stuck task is findable with a plain query,
  // which is the reconciliation entry point an operator (or future
  // tooling) uses -- see docs/decisions/0003-event-ordering.md.
  const [stuckTasks] = await pool.query<RowDataPacket[]>(
    `SELECT task_id FROM task_consumer_state WHERE gap_detected = 1 AND task_id = ?`,
    [taskId],
  );
  assert.equal(stuckTasks.length, 1);
});
