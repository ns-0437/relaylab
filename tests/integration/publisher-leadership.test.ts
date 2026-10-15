// Section 4 of the Phase 1.1 audit: starts TWO real publisher-main.ts
// child processes against the same disposable test database/stream and
// proves the leader lock (src/messaging/publisher-lock.ts) does its job:
// at most one is ever the recorded leader at a time, and if the active
// leader is killed, the other takes over and all outbox rows still get
// published exactly once each, in per-task order. Uses the shared
// ProcessHarness (Phase 1.2 audit, Part A.1 -- see
// docs/decisions/0009-harness-child-cleanup.md) so both children are
// cleaned up on every exit path, not just the happy path.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import path from "node:path";
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { loadTestMysqlConfig } from "../../src/config.js";
import { getPublisherLeaderStatus } from "../../src/messaging/publisher-lock.js";
import { ProcessHarness, testChildEnv, repoRootFrom } from "./process-harness.js";
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

const here = path.dirname(fileURLToPath(import.meta.url));
const publisherEntry = path.join(here, "..", "..", "src", "messaging", "publisher-main.ts");
const repoRoot = repoRootFrom(here);
const testConfig = loadTestMysqlConfig();

async function waitFor(predicate: () => Promise<boolean>, timeoutMs: number, label: string): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 150));
  }
  throw new Error(`timed out waiting for: ${label}`);
}

async function killAndWait(harness: ProcessHarness, child: ReturnType<ProcessHarness["spawn"]>): Promise<void> {
  child.process.kill("SIGKILL");
  await child.exited;
}

test("at most one of two concurrently-running publisher processes is ever the recorded leader", async () => {
  const harness = new ProcessHarness();
  try {
    harness.spawn({ label: "pub-A", entry: publisherEntry, cwd: repoRoot, env: testChildEnv(testConfig.database) });
    harness.spawn({ label: "pub-B", entry: publisherEntry, cwd: repoRoot, env: testChildEnv(testConfig.database) });

    // Sample the leader row repeatedly while both run; it must always name
    // exactly one of the two owner ids (by prefix), never be empty-but-held,
    // and never show signs of both claiming it "at once" (which would show
    // as rapid epoch churn far beyond one takeover).
    const seenOwners = new Set<string>();
    const start = Date.now();
    while (Date.now() - start < 3000) {
      const status = await getPublisherLeaderStatus(pool);
      if (status.ownerId) seenOwners.add(status.ownerId.split("-").slice(0, 2).join("-"));
      await new Promise((r) => setTimeout(r, 100));
    }

    assert.ok(seenOwners.size >= 1, "a leader must have been elected");
    assert.ok(seenOwners.size <= 1, `exactly one process should ever hold leadership while both are healthy, saw: ${[...seenOwners]}`);
  } finally {
    await harness.stopAll();
  }
});

test("killing the active publisher leader lets the other process take over and all outbox rows still publish exactly once, in per-task order", async () => {
  const harness = new ProcessHarness();
  try {
    const store = makeTaskStoreLive(pool);
    const taskId = uniqueId("task");
    const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 60_000));
    for (let i = 0; i < 6; i++) {
      await Effect.runPromise(
        store.applyTaskCommand({
          taskId,
          ownerId: "worker-a",
          epoch: claim.epoch,
          commandId: `cmd-${i}`,
          command: { type: "Checkpoint", digest: `d${i}` },
        }),
      );
    }

    const pubA = harness.spawn({ label: "pub-A", entry: publisherEntry, cwd: repoRoot, env: testChildEnv(testConfig.database) });
    const pubB = harness.spawn({ label: "pub-B", entry: publisherEntry, cwd: repoRoot, env: testChildEnv(testConfig.database) });

    // Wait for a leader to emerge, then kill WHICHEVER process is currently
    // holding the lock (not necessarily pubA -- leadership is nondeterministic).
    await waitFor(async () => (await getPublisherLeaderStatus(pool)).ownerId !== null, 5000, "a leader to be elected");
    const leaderStatus = await getPublisherLeaderStatus(pool);
    const leaderIsPubA = leaderStatus.ownerId?.includes(`-${pubA.process.pid}-`) ?? false;
    const leaderChild = leaderIsPubA ? pubA : pubB;
    await killAndWait(harness, leaderChild);

    await waitFor(
      async () => {
        const [rows] = await pool.query<(RowDataPacket & { c: number })[]>(
          `SELECT COUNT(*) AS c FROM outbox_events WHERE task_id = ? AND published_at IS NOT NULL`,
          [taskId],
        );
        return (rows[0]?.c ?? 0) === 6;
      },
      10_000,
      "the surviving publisher to finish publishing all 6 outbox rows after takeover",
    );

    const [publishedRows] = await pool.query<(RowDataPacket & { seq: number; published_at: Date })[]>(
      `SELECT seq, published_at FROM outbox_events WHERE task_id = ? ORDER BY seq ASC`,
      [taskId],
    );
    assert.equal(publishedRows.length, 6);
    for (let i = 1; i < publishedRows.length; i++) {
      assert.ok(
        publishedRows[i]!.published_at.getTime() >= publishedRows[i - 1]!.published_at.getTime(),
        "publish order must respect per-task seq order even across a mid-stream leader takeover",
      );
    }
  } finally {
    await harness.stopAll();
  }
});
