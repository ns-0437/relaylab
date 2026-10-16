import { test } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import { acquireSingletonLock } from "../../src/api/singleton-lock.js";

test("an acquisition response arriving after the conservative deadline grants no ownership", async () => {
  const pool = { query: async (sql: string) => {
    if (sql.trimStart().startsWith("UPDATE")) {
      await new Promise((resolve) => setTimeout(resolve, 40));
      return [{ affectedRows: 1 }];
    }
    return [[{ epoch: 1 }]];
  } } as unknown as mysql.Pool;
  const lock = await acquireSingletonLock(pool, "delayed", () => {}, { leaseMs: 10, renewIntervalMs: 3, watchdogIntervalMs: 2 });
  assert.equal(lock, null);
});

test("a changed renewal epoch permanently loses ownership even if reacquisition succeeds", async () => {
  let reads = 0;
  const pool = { query: async (query: string | { sql: string }) => {
    const sql = typeof query === "string" ? query : query.sql;
    return sql.trimStart().startsWith("UPDATE") ? [{ affectedRows: 1 }] : [[{ epoch: ++reads }]];
  } } as unknown as mysql.Pool;
  let lost!: () => void;
  const loss = new Promise<void>((resolve) => { lost = resolve; });
  const lock = await acquireSingletonLock(pool, "epoch-change", lost, { leaseMs: 2000, renewIntervalMs: 10, watchdogIntervalMs: 100 });
  assert.ok(lock);
  const guard = setTimeout(() => lost(), 3000);
  try { await loss; assert.equal(lock.isLost(), true); }
  finally { clearTimeout(guard); await lock.stop(); }
});
