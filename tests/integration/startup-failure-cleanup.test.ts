// Demo UI milestone, task 3: proves a startup failure AFTER the
// singleton lock is already acquired (reconcileOnStartup throwing)
// still releases both the lock and the pool -- a subsequent instance
// can start cleanly right after, never left orphaned. A SEPARATE file
// (own process) so this is unaffected by shutdown-timeout.test.ts's
// permanent, module-global admission block.
process.env.RELAYLAB_MYSQL_DATABASE = process.env.RELAYLAB_TEST_MYSQL_DATABASE ?? "relaylab_test";
process.env.RELAYLAB_API_PORT = "0";

import { test } from "node:test";
import assert from "node:assert/strict";
import { createApiServer } from "../../src/api/server.js";
import { createPool } from "../../src/db/pool.js";
import { loadMysqlConfig } from "../../src/config.js";
import type { SingletonLockTimings } from "../../src/api/singleton-lock.js";
import { applySchema, truncateAll } from "./helpers.js";

const FAST_LOCK: SingletonLockTimings = { leaseMs: 3_000, renewIntervalMs: 500, watchdogIntervalMs: 200 };

test("a startup failure after the lock is acquired still releases the lock and the pool -- a subsequent instance can start cleanly", async () => {
  // Controlled fault injection (same pattern as the existing
  // failed-insertion test in api.test.ts): demo_runs is temporarily
  // renamed out from under startup, so reconcileOnStartup -- which runs
  // AFTER the singleton lock is already held -- fails. Restored in a
  // `finally` regardless of outcome.
  const adminPool = createPool(loadMysqlConfig());
  try {
    await applySchema(adminPool);
    await truncateAll(adminPool);
    await adminPool.query(`RENAME TABLE demo_runs TO demo_runs_disabled_for_startup_test`);
    try {
      await assert.rejects(() => createApiServer({ lockTimings: FAST_LOCK }), "startup must fail when reconciliation cannot reach demo_runs");
    } finally {
      await adminPool.query(`RENAME TABLE demo_runs_disabled_for_startup_test TO demo_runs`);
    }
  } finally {
    await adminPool.end();
  }

  // The failed startup must have released BOTH the lock and the pool --
  // confirmed by a genuinely new instance starting cleanly immediately
  // after, with no lingering "already holds the singleton lock" error.
  const recovered = await createApiServer({ lockTimings: FAST_LOCK });
  await recovered.shutdown();
});
