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
import { tryAcquireOrRenewExclusiveLock } from "../../src/messaging/publisher-lock.js";
import { Effect } from "effect";
import { applySchema, requireMysqlPool, requireNats, resetStream, truncateAll, uniqueId, type NatsTestHandles } from "./helpers.js";

/**
 * Retries ONLY on MySQL's own transient ER_LOCK_DEADLOCK -- the
 * documented, expected response to an InnoDB deadlock is to retry the
 * transaction, never to treat it as a correctness failure. This is a
 * pre-existing InnoDB hazard of applyEventWithDedup's own SELECT ... FOR
 * UPDATE-then-INSERT pattern on a not-yet-existing row (two genuinely
 * concurrent callers can both take a gap lock before either inserts),
 * shared by the transactional-dedup test below; it is a property of
 * driving the REAL function with REAL concurrency, not a defect in this
 * test's synchronization. Retrying here is test-local robustness against
 * a transient DB condition, never a substitute for the barrier/epoch
 * synchronization that establishes the actual scenario under test.
 */
async function withDeadlockRetry<T>(fn: () => Promise<T>, attempts = 5): Promise<T> {
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      const isDeadlock = err && typeof err === "object" && "code" in err && err.code === "ER_LOCK_DEADLOCK";
      if (!isDeadlock || i === attempts - 1) throw err;
    }
  }
  throw new Error("unreachable");
}

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
  "transactional dedup under concurrent calls (NOT a real lease takeover -- no lock is acquired or " +
    "expired anywhere in this test; see 'real lease takeover' below for that): two GENUINELY " +
    "CONCURRENT applyEventWithDedup calls for the SAME event still produce EXACTLY ONE durable " +
    "application -- overlapping EXECUTION is not the same thing as a duplicate durable EFFECT",
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
    // Each call is wrapped in withDeadlockRetry: two genuinely concurrent
    // SELECT ... FOR UPDATE + INSERT calls on a not-yet-existing unique
    // row can trigger a transient InnoDB gap-lock deadlock (found by
    // running this exact test repeatedly) -- a pre-existing MySQL
    // concurrency hazard of applyEventWithDedup's own locking pattern,
    // not a flaw in this test's synchronization. Retrying on
    // ER_LOCK_DEADLOCK is MySQL's own documented response to it.
    const consumerName = defaultConsumerOptions().consumerName;
    const [resultA, resultB] = await Promise.all([
      withDeadlockRetry(() => applyEventWithDedup(pool, consumerName, decoded.right)),
      withDeadlockRetry(() => applyEventWithDedup(pool, consumerName, decoded.right)),
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

test(
  "real lease takeover: Holder A acquires the consumer lease, is paused at a barrier before its " +
    "durable operation, the lease is forced to expire deterministically (no real sleep), Holder B " +
    "acquires a strictly newer epoch, and ONLY THEN is Holder A released -- its previously-authorized " +
    "operation overlaps Holder B's own, on the SAME event, and the durable effect still happens exactly " +
    "once. This uses the REAL tryAcquireOrRenewExclusiveLock implementation end to end (unlike the " +
    "test above, which never touches the lock at all). It does NOT claim strict exclusion: the accepted " +
    "contract is coordination of normal polling plus transactional protection of the durable effect " +
    "tested here, not fencing of an in-flight operation that outlives its lease.",
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

    // An isolated lock_name, distinct from 'default'/'consumer-default',
    // so this test can force an expiry on its OWN row without touching
    // the publisher's or the real consumer's lock state.
    const lockName = uniqueId("lock");
    await pool.query(`INSERT IGNORE INTO publisher_leader (lock_name, epoch) VALUES (?, 0)`, [lockName]);

    const aAcquire = await tryAcquireOrRenewExclusiveLock(pool, "holder-a", 10_000, lockName);
    assert.equal(aAcquire.acquired, true, "Holder A must successfully acquire the fresh lock");

    // Holder A is authorized, but paused here -- BEFORE its durable
    // operation runs -- at an explicit barrier (a Promise released only
    // by this test's own later call to releaseA(), never a timer).
    let releaseA!: () => void;
    const barrierA = new Promise<void>((resolve) => {
      releaseA = resolve;
    });
    const consumerName = defaultConsumerOptions().consumerName;
    const aOperation = (async () => {
      await barrierA;
      return withDeadlockRetry(() => applyEventWithDedup(pool, consumerName, decoded.right));
    })();

    // Deterministic expiry: a direct SQL write of a past timestamp, not
    // a real sleep -- this is the exact moment Holder A's lease becomes
    // stale, under this test's own control.
    await pool.query(`UPDATE publisher_leader SET lease_expires_at = DATE_SUB(NOW(3), INTERVAL 1 HOUR) WHERE lock_name = ?`, [
      lockName,
    ]);

    const bAcquire = await tryAcquireOrRenewExclusiveLock(pool, "holder-b", 10_000, lockName);
    assert.equal(bAcquire.acquired, true, "Holder B must acquire after Holder A's lease is forced expired");
    assert.ok(
      bAcquire.epoch > aAcquire.epoch,
      `Holder B's epoch (${bAcquire.epoch}) must be strictly newer than Holder A's (${aAcquire.epoch})`,
    );

    // Holder B begins its own durable operation on the SAME event while
    // Holder A is still paused at the barrier above.
    const bOperation = withDeadlockRetry(() => applyEventWithDedup(pool, consumerName, decoded.right));

    // NOW release Holder A: its previously-authorized operation runs
    // and genuinely overlaps Holder B's in-flight one.
    releaseA();

    const [resultA, resultB] = await Promise.all([aOperation, bOperation]);
    const outcomes = [resultA, resultB].sort();
    assert.deepEqual(
      outcomes,
      ["applied", "deduped"],
      `expected exactly one holder to apply and the other to see it already deduped, got ${JSON.stringify([resultA, resultB])}`,
    );

    const state = await consumerState(taskId);
    assert.ok(state, "task_consumer_state row must exist after a successful apply");
    assert.equal(
      state!.eventCount,
      1,
      "exactly ONE durable application, despite a real epoch takeover overlapping Holder A's previously-authorized operation",
    );

    const [processedRows] = await pool.query<RowDataPacket[]>(
      `SELECT COUNT(*) AS c FROM consumer_processed WHERE consumer_name = ? AND event_id = ?`,
      [consumerName, decoded.right.eventId],
    );
    assert.equal(processedRows[0]!.c, 1, "exactly one consumer_processed row for this event_id, not two");
  },
);
