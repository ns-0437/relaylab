import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { Effect } from "effect";
import type mysql from "mysql2/promise";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";

let pool: mysql.Pool;

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

test("exactly one of two concurrent clients wins ownership of the same task (invariant A)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");

  const [a, b] = await Promise.allSettled([
    Effect.runPromise(store.claimTask(taskId, "worker-a", 10_000)),
    Effect.runPromise(store.claimTask(taskId, "worker-b", 10_000)),
  ]);

  const fulfilled = [a, b].filter((r) => r.status === "fulfilled");
  const rejected = [a, b].filter((r) => r.status === "rejected");
  assert.equal(fulfilled.length, 1, "exactly one claim must succeed");
  assert.equal(rejected.length, 1, "exactly one claim must fail with ClaimLost");

  const task = await Effect.runPromise(store.getTask(taskId));
  assert.equal(task.epoch, 1, "epoch increments exactly once across the race");
  assert.ok(task.ownerId === "worker-a" || task.ownerId === "worker-b");
});

test("a lease expiring lets a new worker take over, and the old worker's later command is rejected (invariant B)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");
  const leaseMs = 150;

  const first = await Effect.runPromise(store.claimTask(taskId, "worker-old", leaseMs));
  assert.equal(first.epoch, 1);

  await new Promise((r) => setTimeout(r, leaseMs + 150));

  const second = await Effect.runPromise(store.claimTask(taskId, "worker-new", 10_000));
  assert.equal(second.epoch, 2, "epoch must increase on takeover");
  assert.equal(second.ownerId, "worker-new");

  // The old worker, still believing it holds epoch 1, tries to act.
  const err = await Effect.runPromise(
    Effect.flip(
      store.applyTaskCommand({
        taskId,
        ownerId: "worker-old",
        epoch: first.epoch,
        commandId: "cmd-1",
        command: { type: "Checkpoint", digest: "stale-write" },
      }),
    ),
  );
  assert.equal(err._tag, "EpochStale");

  const task = await Effect.runPromise(store.getTask(taskId));
  assert.notEqual(task.checkpointDigest, "stale-write", "stale write must not land");
});

test("an expired lease cannot be renewed as if it were still valid", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");
  const leaseMs = 150;

  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", leaseMs));
  await new Promise((r) => setTimeout(r, leaseMs + 150));

  const err = await Effect.runPromise(Effect.flip(store.renewLease(taskId, "worker-a", claim.epoch, 10_000)));
  assert.equal(err._tag, "ClaimLost");
});

test("unrelated tasks remain independent: handing off task A does not block task B (invariant H)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskA = uniqueId("task-a");
  const taskB = uniqueId("task-b");

  await Effect.runPromise(store.claimTask(taskA, "worker-1", 10));
  await new Promise((r) => setTimeout(r, 60));
  // Hand off task A (claim after its lease expired).
  const handoffStart = Date.now();
  await Effect.runPromise(store.claimTask(taskA, "worker-2", 10_000));
  const handoffMs = Date.now() - handoffStart;

  // Task B should be claimable immediately, unaffected by A's handoff.
  const bStart = Date.now();
  await Effect.runPromise(store.claimTask(taskB, "worker-3", 10_000));
  const bMs = Date.now() - bStart;

  assert.ok(bMs < 200, `task B claim should be fast and independent, took ${bMs}ms`);
  assert.ok(handoffMs < 500, `task A handoff should also be fast, took ${handoffMs}ms`);
});
