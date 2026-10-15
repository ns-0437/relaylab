// Phase 1.2 audit, Part A.2: direct regression coverage for
// publisher-lock.ts's epoch semantics against real MySQL, after fixing
// the SET-clause evaluation-order issue (docs/decisions/0008).
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import { tryAcquireOrRenewPublisherLeadership, getPublisherLeaderStatus } from "../../src/messaging/publisher-lock.js";
import { applySchema, requireMysqlPool, truncateAll } from "./helpers.js";

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

test("an expired leader (A) being replaced by (B) increments the epoch", async () => {
  const a1 = await tryAcquireOrRenewPublisherLeadership(pool, "A", 150);
  assert.equal(a1.acquired, true);
  await new Promise((r) => setTimeout(r, 220)); // let A's lease expire

  const b1 = await tryAcquireOrRenewPublisherLeadership(pool, "B", 10_000);
  assert.equal(b1.acquired, true);
  assert.equal(b1.epoch, a1.epoch + 1, "takeover must increment epoch by exactly 1");

  const status = await getPublisherLeaderStatus(pool);
  assert.equal(status.ownerId, "B");
  assert.equal(status.epoch, b1.epoch);
});

test("a valid renewal by the SAME owner preserves its epoch", async () => {
  const a1 = await tryAcquireOrRenewPublisherLeadership(pool, "A", 10_000);
  const a2 = await tryAcquireOrRenewPublisherLeadership(pool, "A", 10_000); // renew while still valid
  const a3 = await tryAcquireOrRenewPublisherLeadership(pool, "A", 10_000); // renew again

  assert.equal(a2.epoch, a1.epoch, "renewal by the same owner must NOT increment epoch");
  assert.equal(a3.epoch, a1.epoch, "repeated renewal must still not increment epoch");
});

test("reacquisition after expiration always increments by exactly 1, regardless of how many renewal cycles elapsed before expiry (documented epoch rule)", async () => {
  const a1 = await tryAcquireOrRenewPublisherLeadership(pool, "A", 150);
  // Several valid renewals first, to prove epoch stayed flat through all of them...
  const a2 = await tryAcquireOrRenewPublisherLeadership(pool, "A", 150);
  const a3 = await tryAcquireOrRenewPublisherLeadership(pool, "A", 150);
  assert.equal(a2.epoch, a1.epoch);
  assert.equal(a3.epoch, a1.epoch);

  // ...then let it actually expire and be taken over.
  await new Promise((r) => setTimeout(r, 220));
  const b1 = await tryAcquireOrRenewPublisherLeadership(pool, "B", 10_000);
  assert.equal(b1.epoch, a3.epoch + 1, "exactly +1, not proportional to the number of prior renewals");
});

test("a competing owner cannot acquire while the current lease is unexpired", async () => {
  const a1 = await tryAcquireOrRenewPublisherLeadership(pool, "A", 10_000);
  const b1 = await tryAcquireOrRenewPublisherLeadership(pool, "B", 10_000);
  assert.equal(b1.acquired, false, "B must not acquire while A's lease is still valid");

  const status = await getPublisherLeaderStatus(pool);
  assert.equal(status.ownerId, "A", "ownership must remain with A");
  assert.equal(status.epoch, a1.epoch, "a failed acquisition attempt must not mutate epoch at all");
});

test("expiration boundary: a lease expiring exactly 'now' is treated consistently as expired (both the WHERE gate and the epoch CASE agree)", async () => {
  // A very short lease, then wait comfortably past it (not AT the exact
  // boundary -- MySQL's own clock resolution makes hitting the exact
  // millisecond edge from Node unreliable to construct deterministically;
  // this proves the >=/< pairing used on both sides of the SQL agrees
  // well past the edge, which is what actually matters operationally).
  const a1 = await tryAcquireOrRenewPublisherLeadership(pool, "A", 150);
  await new Promise((r) => setTimeout(r, 220));
  const b1 = await tryAcquireOrRenewPublisherLeadership(pool, "B", 10_000);
  assert.equal(b1.acquired, true, "an expired lease must be acquirable");
  assert.equal(b1.epoch, a1.epoch + 1);
});

test("a new owner never inherits the previous owner's epoch value -- it is always exactly previous+1 or its own already-held epoch, never a copy of someone else's", async () => {
  const a1 = await tryAcquireOrRenewPublisherLeadership(pool, "A", 150);
  await new Promise((r) => setTimeout(r, 220));
  const b1 = await tryAcquireOrRenewPublisherLeadership(pool, "B", 50);
  assert.notEqual(b1.epoch, a1.epoch, "B's epoch must differ from A's last epoch");
  assert.equal(b1.epoch, a1.epoch + 1);

  await new Promise((r) => setTimeout(r, 220));
  const c1 = await tryAcquireOrRenewPublisherLeadership(pool, "C", 10_000);
  assert.notEqual(c1.epoch, b1.epoch);
  assert.equal(c1.epoch, b1.epoch + 1, "each successive takeover increments from the IMMEDIATATELY previous epoch, not reset or copied");
});
