// Section 2 of the Phase 1.1 audit: command retry semantics, specifically
// the required 8-step regression scenario, plus claim/renew idempotency
// (docs/decisions/0004-receipt-authorization.md, 0007-claim-idempotency.md).
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

test("required 8-step regression: retry of a committed command after ownership transfer, by the original owner, returns the original receipt untouched", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");

  // 1. Worker A owns the task.
  const claimA = await Effect.runPromise(store.claimTask(taskId, "worker-a", 150));

  // 2. A command commits successfully.
  const command = { type: "Checkpoint" as const, digest: "digest-A1" };
  const receipt1 = await Effect.runPromise(
    store.applyTaskCommand({ taskId, ownerId: "worker-a", epoch: claimA.epoch, commandId: "cmd-A1", command }),
  );
  assert.equal(receipt1.replay, false);
  const stateAfterCommit = await Effect.runPromise(store.getTask(taskId));
  assert.equal(stateAfterCommit.seq, 1);
  assert.equal(await outboxCount(taskId), 1);

  // 3. Ownership transfers to B (A's lease expires, B takes over).
  await new Promise((r) => setTimeout(r, 220));
  const claimB = await Effect.runPromise(store.claimTask(taskId, "worker-b", 10_000));
  assert.ok(claimB.epoch > claimA.epoch, "takeover must increment epoch");

  // 4. A retries the EXACT original command (same commandId, same payload,
  //    same ownerId -- A still believes it is "worker-a" issuing this retry).
  const receipt2 = await Effect.runPromise(
    store.applyTaskCommand({ taskId, ownerId: "worker-a", epoch: claimA.epoch, commandId: "cmd-A1", command }),
  );

  // 5. The original receipt/result is returned.
  assert.equal(receipt2.replay, true);
  assert.deepEqual(receipt2.result, receipt1.result);

  // 6. Task state, sequence, and outbox count remain unchanged.
  const stateAfterRetry = await Effect.runPromise(store.getTask(taskId));
  assert.equal(stateAfterRetry.seq, 1, "the retry must not advance seq again");
  assert.equal(stateAfterRetry.checkpointDigest, "digest-A1");
  assert.equal(await outboxCount(taskId), 1, "the retry must not produce a second outbox event");

  // 7. A new (never-before-seen) command from A's stale epoch is rejected.
  const staleErr = await Effect.runPromise(
    Effect.flip(
      store.applyTaskCommand({
        taskId,
        ownerId: "worker-a",
        epoch: claimA.epoch,
        commandId: "cmd-A2-new",
        command: { type: "Checkpoint", digest: "should-not-land" },
      }),
    ),
  );
  assert.equal(staleErr._tag, "EpochStale");

  // 8. Reusing the old command ID with changed content is rejected.
  const conflictErr = await Effect.runPromise(
    Effect.flip(
      store.applyTaskCommand({
        taskId,
        ownerId: "worker-a",
        epoch: claimA.epoch,
        commandId: "cmd-A1",
        command: { type: "Checkpoint", digest: "TAMPERED" },
      }),
    ),
  );
  assert.equal(conflictErr._tag, "CommandConflict");
});

test("a retry attempted by a DIFFERENT owner than the one who originally committed is rejected as unauthorized, not silently served", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");
  const claimA = await Effect.runPromise(store.claimTask(taskId, "worker-a", 150));
  const command = { type: "Checkpoint" as const, digest: "digest-A1" };
  await Effect.runPromise(
    store.applyTaskCommand({ taskId, ownerId: "worker-a", epoch: claimA.epoch, commandId: "cmd-A1", command }),
  );

  await new Promise((r) => setTimeout(r, 220));
  const claimB = await Effect.runPromise(store.claimTask(taskId, "worker-b", 10_000));

  // B (the new owner) tries to "replay" A's old command id + identical
  // payload, presenting itself (not A) as the owner. This must be
  // authorized against who ORIGINALLY committed it, not silently served
  // just because the content happens to match.
  const err = await Effect.runPromise(
    Effect.flip(
      store.applyTaskCommand({
        taskId,
        ownerId: "worker-b",
        epoch: claimB.epoch,
        commandId: "cmd-A1",
        command,
      }),
    ),
  );
  assert.equal(err._tag, "NotAuthorized");
});

test("claimTask is idempotent for the rightful current owner: retrying a still-valid claim succeeds again with the SAME epoch (docs/decisions/0007)", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");

  const claim1 = await Effect.runPromise(store.claimTask(taskId, "worker-a", 10_000));
  // Simulate the response being lost and the caller retrying its own
  // still-valid claim.
  const claim2 = await Effect.runPromise(store.claimTask(taskId, "worker-a", 10_000));

  assert.equal(claim2.epoch, claim1.epoch, "re-claiming while still valid must NOT be treated as a takeover");
  assert.ok(claim2.leaseExpiresAtMs >= claim1.leaseExpiresAtMs, "the lease should still be extended");
});

test("claimTask by a DIFFERENT owner while the current owner's lease is still valid is rejected, not merged", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task");
  await Effect.runPromise(store.claimTask(taskId, "worker-a", 10_000));

  const err = await Effect.runPromise(Effect.flip(store.claimTask(taskId, "worker-b", 10_000)));
  assert.equal(err._tag, "ClaimLost");
});
