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

  await lock!.stop();
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

test("a delayed-but-successful renewal response arriving AFTER expiry was already declared must be discarded, never reviving validity", async () => {
  // The renewal UPDATE's RESPONSE is delayed (not the call itself) --
  // simulating network/query latency on the return trip. A short lease
  // means the watchdog declares loss WHILE that delayed call is still
  // in flight; once it finally resolves (successfully -- this is the
  // SAME owner, so the DB still lets it "renew" even past expiry), the
  // result must be discarded, never un-declaring loss or extending
  // leaseValidUntil.
  const realQuery = pool.query.bind(pool);
  let renewalCalls = 0;
  const slowPool = {
    ...pool,
    query: (async (sql: unknown, params?: unknown) => {
      const isRenewalWrite = typeof sql === "string" && sql.includes("SET epoch");
      const call = (realQuery as (sql: unknown, params?: unknown) => Promise<unknown>);
      if (!isRenewalWrite) return call(sql, params);
      renewalCalls++;
      if (renewalCalls === 1) return call(sql, params); // the initial acquisition -- let it through fast
      const result = await call(sql, params);
      await new Promise((r) => setTimeout(r, 300)); // the delayed RESPONSE, arriving late
      return result;
    }) as typeof pool.query,
  } as mysql.Pool;

  let lostCalls = 0;
  const lock = await acquireSingletonLock(slowPool, "owner-delayed-response", () => {
    lostCalls++;
  }, { leaseMs: 100, renewIntervalMs: 50, watchdogIntervalMs: 20 });
  assert.ok(lock, "initial acquisition must succeed");

  // By ~150ms the watchdog must have declared loss (deadline ~100ms
  // past acquisition), well before the first renewal's delayed (300ms)
  // response arrives at ~350ms.
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(lock!.isLost(), true, "the watchdog must have declared loss from the short lease expiring, independent of the in-flight renewal");
  assert.equal(lostCalls, 1);

  // Now wait for the delayed renewal's late-arriving (successful)
  // response to actually resolve, and confirm it changed nothing.
  await new Promise((r) => setTimeout(r, 250));
  assert.equal(lock!.isLost(), true, "a delayed successful response must never revive validity once loss was already declared");
  assert.equal(lostCalls, 1, "onLost must still have fired exactly once -- the stale response must not trigger a second declaration or any other side effect");

  await lock!.stop();
});

test("reacquisition whose epoch has changed is never treated as uninterrupted ownership, even when the response itself reports acquired:true", async () => {
  let lostCalls = 0;
  // A long lease (this owner's own renewal ticks would otherwise keep
  // legitimately succeeding with an unchanged epoch) -- the epoch is
  // changed out from under it directly, simulating the row having gone
  // through a genuine expire-and-reclaim cycle this process never
  // observed directly (a crash/stall gap bridged by some other owner,
  // then reclaimed again before this owner's next renewal).
  const lock = await acquireSingletonLock(pool, "owner-epoch-check", () => {
    lostCalls++;
  }, { ...FAST, leaseMs: 10_000 });
  assert.ok(lock, "initial acquisition must succeed");
  assert.equal(lock!.isLost(), false);

  await pool.query(`UPDATE publisher_leader SET epoch = epoch + 5 WHERE lock_name = ?`, [DEMO_API_LOCK_NAME]);

  // This owner's own next renewal tick still matches owner_id and the
  // lease is still far from expiring, from ITS OWN original tracking --
  // so the DB call itself reports acquired:true. The returned epoch no
  // longer matches what this lock tracked at acquisition time, though,
  // which must be treated as lost ownership continuity regardless.
  await new Promise((r) => setTimeout(r, FAST.renewIntervalMs + FAST.watchdogIntervalMs + 50));
  assert.equal(lock!.isLost(), true, "a changed epoch must be treated as lost, even though the renewal response itself said acquired:true");
  assert.equal(lostCalls, 1);

  await lock!.stop();
});
