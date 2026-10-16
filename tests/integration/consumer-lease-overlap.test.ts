// Phase 3A.2: a focused, deterministic regression for the actual
// consumer lease guarantee -- see docs/KUBERNETES.md "Consumer
// exclusivity: what the lease actually guarantees" and the corrected
// comments in src/messaging/consumer-main.ts.
//
// The lease (tryAcquireOrRenewExclusiveLock, checked once per poll
// iteration BEFORE calling processBatch) coordinates NORMAL polling: in
// the ordinary case, a second instance's acquire attempt fails and it
// skips its iteration entirely. But the lease is never passed into
// processBatch, and processBatch's own duration is not bounded by the
// lease -- a previously-authorized in-flight call can genuinely outlive
// its lease and finish after a second instance has already taken over.
// This is reused, EXISTING test infrastructure (the same
// applyEventWithDedup-called-directly-twice pattern already established
// by consumer-dedup.test.ts, which proves durable dedup against a
// REDELIVERED message) applied to the DIFFERENT, more direct case this
// patch is actually about: two calls that are not sequential-with-a-nak
// in between, but genuinely CONCURRENT -- the exact shape of "an old
// holder's in-flight operation overlapping a new holder's," reproduced
// at the one point that actually matters (the durable write), without
// needing to orchestrate real OS-process lease-expiry timing.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { applyEventWithDedup, defaultConsumerOptions } from "../../src/messaging/consumer.js";
import { runPublisherOnce } from "../../src/messaging/publisher.js";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { decodeEventEnvelope } from "../../src/protocol/protocol.js";
import { eventsStream, eventsDurable } from "../../src/config.js";
import { Effect } from "effect";
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

async function consumerState(taskId: string): Promise<{ eventCount: number; lastAppliedSeq: number } | null> {
  const [rows] = await pool.query<(RowDataPacket & { event_count: number; last_applied_seq: number })[]>(
    `SELECT event_count, last_applied_seq FROM task_consumer_state WHERE task_id = ?`,
    [taskId],
  );
  const row = rows[0];
  if (!row) return null;
  return { eventCount: row.event_count, lastAppliedSeq: row.last_applied_seq };
}

test(
  "consumer lease overlap: two GENUINELY CONCURRENT applyEventWithDedup calls for the SAME event " +
    "(modeling a previously-authorized in-flight operation overlapping a new lock holder, per the " +
    "documented lease guarantee) still produce EXACTLY ONE durable application -- overlapping " +
    "EXECUTION is not the same thing as a duplicate durable EFFECT",
  async () => {
    const taskId = uniqueId("task");
    await publishOneCheckpointEvent(taskId);

    const fetch = nats.js.fetch(eventsStream(), eventsDurable(), { batch: 1, expires: 2000 });
    const msgs = [];
    for await (const m of fetch) msgs.push(m);
    assert.equal(msgs.length, 1);
    const decoded = decodeEventEnvelope(JSON.parse(new TextDecoder().decode(msgs[0]!.data)));
    assert.equal(decoded._tag, "Right");
    if (decoded._tag !== "Right") throw new Error("unreachable");

    // Deliberately Promise.all, not sequential: this is what "an old
    // holder's in-flight operation outlives its lease and overlaps a
    // new holder's" actually looks like at the durable-write layer --
    // two callers both believing (at the moment each started) that they
    // were the sole authorized instance, racing on the exact same event.
    const consumerName = defaultConsumerOptions().consumerName;
    const [resultA, resultB] = await Promise.all([
      applyEventWithDedup(pool, consumerName, decoded.right),
      applyEventWithDedup(pool, consumerName, decoded.right),
    ]);

    // Distinguishing overlapping EXECUTION (both calls genuinely ran
    // concurrently, both returned, neither threw) from a duplicate
    // durable EFFECT (which would show up as event_count=2 below).
    const outcomes = [resultA, resultB].sort();
    assert.deepEqual(
      outcomes,
      ["applied", "deduped"],
      `expected exactly one caller to apply and the other to see it already deduped, got ${JSON.stringify([resultA, resultB])}`,
    );

    const state = await consumerState(taskId);
    assert.ok(state, "task_consumer_state row must exist after a successful apply");
    assert.equal(state!.eventCount, 1, "exactly ONE durable application, despite two concurrent callers racing on the same event");
    assert.equal(state!.lastAppliedSeq, 1, "last_applied_seq must reflect exactly one application, not two");

    const [processedRows] = await pool.query<RowDataPacket[]>(
      `SELECT COUNT(*) AS c FROM consumer_processed WHERE consumer_name = ? AND event_id = ?`,
      [consumerName, decoded.right.eventId],
    );
    assert.equal(processedRows[0]!.c, 1, "exactly one consumer_processed row for this event_id, not two");
  },
);
