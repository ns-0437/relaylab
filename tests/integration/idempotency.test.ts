import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { Effect } from "effect";
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
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

async function outboxCount(taskId: string): Promise<number> {
  const [rows] = await pool.query<(RowDataPacket & { c: number })[]>(
    `SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ?`,
    [taskId],
  );
  return Number(rows[0]?.c ?? 0);
}

async function receiptCount(taskId: string): Promise<number> {
  const [rows] = await pool.query<(RowDataPacket & { c: number })[]>(
    `SELECT COUNT(*) AS c FROM command_receipts WHERE task_id = ?`,
    [taskId],
  );
  return Number(rows[0]?.c ?? 0);
}

test("retrying the same command produces one transition, one receipt, one outbox event (invariant C/D)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 10_000));

  const args = {
    taskId,
    ownerId: "worker-a",
    epoch: claim.epoch,
    commandId: "cmd-checkpoint-1",
    command: { type: "Checkpoint" as const, digest: "digest-abc" },
  };

  const r1 = await Effect.runPromise(store.applyTaskCommand(args));
  const r2 = await Effect.runPromise(store.applyTaskCommand(args));
  const r3 = await Effect.runPromise(store.applyTaskCommand(args));

  assert.equal(r1.replay, false);
  assert.equal(r2.replay, true);
  assert.equal(r3.replay, true);
  assert.deepEqual(r1.result, r2.result);

  assert.equal(await receiptCount(taskId), 1);
  assert.equal(await outboxCount(taskId), 1);

  const task = await Effect.runPromise(store.getTask(taskId));
  assert.equal(task.checkpointDigest, "digest-abc");
  assert.equal(task.seq, 1, "a replayed command must not advance seq again");
});

test("reusing a command ID with a different payload is rejected (invariant C)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 10_000));

  await Effect.runPromise(
    store.applyTaskCommand({
      taskId,
      ownerId: "worker-a",
      epoch: claim.epoch,
      commandId: "cmd-1",
      command: { type: "Checkpoint", digest: "digest-a" },
    }),
  );

  const err = await Effect.runPromise(
    Effect.flip(
      store.applyTaskCommand({
        taskId,
        ownerId: "worker-a",
        epoch: claim.epoch,
        commandId: "cmd-1",
        command: { type: "Checkpoint", digest: "digest-DIFFERENT" },
      }),
    ),
  );
  assert.equal(err._tag, "CommandConflict");

  // The original write must be untouched.
  const task = await Effect.runPromise(store.getTask(taskId));
  assert.equal(task.checkpointDigest, "digest-a");
  assert.equal(await receiptCount(taskId), 1);
});

test("a domain rejection leaves no partial receipt, state update, or outbox event (invariant D/J)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 10_000));

  // Completing without a passed verification is a domain rejection raised
  // mid-transaction, after the fencing checks but before any writes.
  const err = await Effect.runPromise(
    Effect.flip(
      store.applyTaskCommand({
        taskId,
        ownerId: "worker-a",
        epoch: claim.epoch,
        commandId: "cmd-complete",
        command: { type: "Complete" },
      }),
    ),
  );
  assert.equal(err._tag, "CommandRejected");

  assert.equal(await receiptCount(taskId), 0, "no receipt should exist for a rejected command");
  assert.equal(await outboxCount(taskId), 0, "no outbox event should exist for a rejected command");
  const task = await Effect.runPromise(store.getTask(taskId));
  assert.equal(task.status, "owned", "task state must be untouched");
  assert.equal(task.seq, 0);
});
