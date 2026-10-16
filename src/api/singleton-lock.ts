// Demo UI milestone: a small database-backed singleton lock for the
// local demo API -- reuses the EXISTING publisher_leader table/epoch
// mechanism (src/messaging/publisher-lock.ts) under its own lock_name
// ("demo-api"), exactly the pattern migration 0012 already established
// for the consumer lock. Never a new lock mechanism, never a
// distributed scheduler -- this exists purely so a second instance of
// this single-process demo, started against the SAME database, cannot
// reconcile/modify/execute runs the first instance already owns.
import type mysql from "mysql2/promise";
import { tryAcquireOrRenewExclusiveLock } from "../messaging/publisher-lock.js";

export const DEMO_API_LOCK_NAME = "demo-api";
const LEASE_MS = 20_000;
const RENEW_INTERVAL_MS = 7_000;

export interface SingletonLock {
  ownerId: string;
  /** Stops renewal and releases this process's reference to the lock. Does NOT revoke the lease early (the lease simply expires) -- this demo has no "release now" primitive and does not need one; a short lease plus no further renewal is sufficient. */
  stopRenewing: () => void;
}

/**
 * Attempts to acquire the singleton lock ONCE. Returns `null` (never
 * throws for a normal contention case) if another instance currently
 * holds it -- the caller decides what that means (this server refuses
 * to start at all; see server.ts).
 */
export async function acquireSingletonLock(pool: mysql.Pool, ownerId: string, onLost: () => void): Promise<SingletonLock | null> {
  let first = await tryAcquireOrRenewExclusiveLock(pool, ownerId, LEASE_MS, DEMO_API_LOCK_NAME);
  if (!first.acquired) {
    // Self-healing against a MISSING row, not a genuinely held lock:
    // migration 0014 seeds this row once, but some OTHER integration
    // test sharing this same database can legitimately TRUNCATE
    // publisher_leader between runs (truncateAll, used throughout this
    // project's test suite) -- migrations never re-run once recorded
    // as applied, so the row can go missing without this ever being a
    // real contention case. INSERT IGNORE is a no-op if another
    // instance's row already exists (the normal contention case,
    // correctly left alone), and only actually helps when the row was
    // truly absent.
    await pool.query(`INSERT IGNORE INTO publisher_leader (lock_name, epoch) VALUES (?, 0)`, [DEMO_API_LOCK_NAME]);
    first = await tryAcquireOrRenewExclusiveLock(pool, ownerId, LEASE_MS, DEMO_API_LOCK_NAME);
  }
  if (!first.acquired) return null;

  const interval = setInterval(() => {
    tryAcquireOrRenewExclusiveLock(pool, ownerId, LEASE_MS, DEMO_API_LOCK_NAME)
      .then((result) => {
        if (!result.acquired) {
          clearInterval(interval);
          onLost();
        }
      })
      .catch(() => {
        // A transient DB error renewing the lock is NOT the same as
        // confirmed lock loss -- do not treat it as such. The next
        // renewal attempt will either succeed or, if another instance
        // has genuinely taken over by then, correctly report loss.
      });
  }, RENEW_INTERVAL_MS);
  // Never blocks process exit on its own.
  interval.unref();

  return { ownerId, stopRenewing: () => clearInterval(interval) };
}
