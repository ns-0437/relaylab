process.env.RELAYLAB_MYSQL_DATABASE = process.env.RELAYLAB_TEST_MYSQL_DATABASE ?? "relaylab_test";
process.env.RELAYLAB_SHUTDOWN_TIMEOUT_MS = "50";
import { test } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import { createApiServer } from "../../src/api/server.js";
import { startRun, awaitActiveRun } from "../../src/api/executor.js";

test("shutdown reports its deadline while a controlled admission barrier remains pending", async () => {
  const handle = await createApiServer({ lockTimings: { leaseMs: 3000, renewIntervalMs: 500, watchdogIntervalMs: 200 } });
  let poolClosed!: () => void;
  const closed = new Promise<void>((resolve) => { poolClosed = resolve; });
  const originalEnd = handle.pool.end.bind(handle.pool);
  handle.pool.end = async () => { await originalEnd(); poolClosed(); };
  await new Promise<void>((resolve) => handle.server.listen(0, "127.0.0.1", resolve));
  let rejectInsert!: (error: Error) => void;
  const pool = { query: () => new Promise((_resolve, reject) => { rejectInsert = reject; }) } as unknown as mysql.Pool;
  const admission = startRun(pool, "deterministic", false);
  const rejected = assert.rejects(admission, /database error/);
  try {
    const result = await handle.shutdown();
    assert.equal(result.timedOut, true);
    assert.equal((await handle.shutdown()).timedOut, true);
  } finally {
    rejectInsert(Object.assign(new Error("injected"), { code: "ER_TEST" }));
    await rejected;
    await awaitActiveRun();
  }
  // pool.end is called only after execution settles. Observe its completion.
  await closed;
});
