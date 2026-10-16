// Demo UI milestone, task 2: focused regression coverage for
// src/api/singleton-lock.ts's lease-expiry tracking -- proves that (a) a
// sustained run of renewal ERRORS (never an explicit "not acquired"
// answer) still declares loss once the conservatively-tracked expiry
// deadline passes, and (b) a genuine takeover by another owner while
// this instance has active work in progress is detected and reported
// via the SAME onLost callback. Uses tiny, injected timings (milliseconds,
// not the real 20s/7s/2s defaults) so this runs in well under a second --
// never against the real server's own timing constants.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import { acquireSingletonLock, DEMO_API_LOCK_NAME, type SingletonLockTimings } from "../../src/api/singleton-lock.js";
import { tryAcquireOrRenewExclusiveLock } from "../../src/messaging/publisher-lock.js";
import { applySchema, requireMysqlPool, truncateAll } from "./helpers.js";

let pool: mysql.Pool;
const FAST: SingletonLockTimings = { leaseMs: 300, renewIntervalMs: 60, watchdogIntervalMs: 20 };

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

test("renewal failure through expiry: sustained renewal ERRORS (never an explicit loss answer) still declare loss once the tracked deadline passes", async () => {
  // A pool whose query() always throws AFTER the lock is already held --
  // simulates a run of transient DB errors on every renewal attempt,
  // never a confirmed "another owner holds it" response.
  const realQuery = pool.query.bind(pool);
  let failing = false;
  const faultyPool = {
    ...pool,
    query: ((...args: Parameters<typeof pool.query>) => {
      if (failing) return Promise.reject(new Error("simulated transient renewal error"));
      return realQuery(...args);
    }) as typeof pool.query,
  } as mysql.Pool;

  let lostCalls = 0;
  const lock = await acquireSingletonLock(faultyPool, "owner-expiry-test", () => {
    lostCalls++;
  }, FAST);
  assert.ok(lock, "initial acquisition must succeed");
  assert.equal(lock!.isLost(), false);

  failing = true; // every renewal from here on errors, never returns an explicit "not acquired"
  // Wait comfortably past leaseMs (300ms) -- long enough for the watchdog
  // (20ms ticks) to have observed the tracked deadline pass, even though
  // no renewal ever got a real response.
  await new Promise((r) => setTimeout(r, 500));

  assert.equal(lock!.isLost(), true, "the watchdog must declare loss once the tracked expiry deadline passes, independent of ever receiving an explicit non-acquisition response");
  assert.equal(lostCalls, 1, "onLost must fire exactly once");

  // Storage is still unavailable: explicit release must report failure,
  // while timers have stopped and the already-expired lease stays lost.
  await assert.rejects(lock!.stop(), /simulated transient renewal error/);
});

test("takeover during active work: another owner genuinely acquiring the same lock row is detected and reported via onLost", async () => {
  let lostCalls = 0;
  // A long lease here (owner A's own renewal loop would otherwise keep
  // re-extending a short one forever, as it legitimately should for a
  // healthy process -- a takeover can only ever happen once the row's
  // lease has actually gone stale, which for a REAL process means it
  // stalled or crashed between renewals). The row is force-expired below
  // to simulate exactly that stall, independent of this process's own
  // (still-running) renewal timer.
  const lock = await acquireSingletonLock(pool, "owner-A-active", () => {
    lostCalls++;
  }, { ...FAST, leaseMs: 10_000 });
  assert.ok(lock, "owner A must acquire the lock initially");
  assert.equal(lock!.isLost(), false);

  // Force the row's lease into the past -- simulates owner A having
  // stalled (e.g. a GC pause or a crash) between renewals, which is the
  // only real-world way a SECOND owner's acquisition attempt can ever
  // succeed while A's in-process renewal timer is still running.
  await pool.query(`UPDATE publisher_leader SET lease_expires_at = DATE_SUB(NOW(3), INTERVAL 1 SECOND) WHERE lock_name = ?`, [DEMO_API_LOCK_NAME]);

  const takeover = await tryAcquireOrRenewExclusiveLock(pool, "owner-B-takeover", 10_000, DEMO_API_LOCK_NAME);
  assert.equal(takeover.acquired, true, "owner B must be able to take over once A's lease row has gone stale");

  // Owner A's own next renewal tick (within FAST.renewIntervalMs) must now
  // see the CONFIRMED "not acquired" response (B now owns the row) and
  // declare loss immediately -- this is the "confirmed lock loss must
  // also cancel active execution" path (the cancellation itself is wired
  // in server.ts; this proves the underlying detection fires on takeover).
  await new Promise((r) => setTimeout(r, FAST.renewIntervalMs + FAST.watchdogIntervalMs + 50));
  assert.equal(lock!.isLost(), true, "owner A must detect the takeover and declare loss");
  assert.equal(lostCalls, 1, "onLost must fire exactly once");

  await lock!.stop();
});

test("a confirmed lock loss never re-enters a valid state -- isLost() stays true and stop() is safe to call afterward", async () => {
  let lostCalls = 0;
  const lock = await acquireSingletonLock(pool, "owner-no-reacquire", () => {
    lostCalls++;
  }, { ...FAST, leaseMs: 10_000 });
  assert.ok(lock);

  await pool.query(`UPDATE publisher_leader SET lease_expires_at = DATE_SUB(NOW(3), INTERVAL 1 SECOND) WHERE lock_name = ?`, [DEMO_API_LOCK_NAME]);
  await tryAcquireOrRenewExclusiveLock(pool, "owner-stealer", 10_000, DEMO_API_LOCK_NAME);
  await new Promise((r) => setTimeout(r, FAST.renewIntervalMs * 3));

  assert.equal(lock!.isLost(), true);
  assert.equal(lostCalls, 1);
  await lock!.stop(); // must not throw, must not attempt to reacquire
  assert.equal(lock!.isLost(), true, "must remain lost after stop()");
});
