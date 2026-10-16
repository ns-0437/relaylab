// Demo UI milestone: a small database-backed singleton lock for the
// local demo API -- reuses the EXISTING publisher_leader table/epoch
// mechanism (src/messaging/publisher-lock.ts) under its own lock_name
// ("demo-api"), exactly the pattern migration 0012 already established
// for the consumer lock. Never a new lock mechanism, never a
// distributed scheduler -- this exists purely so a second instance of
// this single-process demo, started against the SAME database, cannot
// reconcile/modify/execute runs the first instance already owns.
import type mysql from "mysql2/promise";
import { performance } from "node:perf_hooks";
import { tryAcquireOrRenewExclusiveLock } from "../messaging/publisher-lock.js";

export const DEMO_API_LOCK_NAME = "demo-api";
const DEFAULT_LEASE_MS = 20_000;
const DEFAULT_RENEW_INTERVAL_MS = 7_000;
/**
 * How often the watchdog checks the tracked expiry deadline, independent
 * of whether a renewal CALL is succeeding -- deliberately shorter than
 * RENEW_INTERVAL_MS so a run of transient errors is caught well before
 * the real server-side lease could have expired, not merely on the next
 * (possibly also-failing) renewal attempt's own response.
 */
const DEFAULT_WATCHDOG_INTERVAL_MS = 2_000;

export interface SingletonLockTimings {
  leaseMs: number;
  renewIntervalMs: number;
  watchdogIntervalMs: number;
}
const DEFAULT_TIMINGS: SingletonLockTimings = { leaseMs: DEFAULT_LEASE_MS, renewIntervalMs: DEFAULT_RENEW_INTERVAL_MS, watchdogIntervalMs: DEFAULT_WATCHDOG_INTERVAL_MS };

export interface SingletonLock {
  ownerId: string;
  /** True once this lock is CONFIRMED lost (explicit "not acquired" response) or its conservatively-tracked expiry deadline has passed without a successful renewal -- never re-enters `false` once set; this lock object never silently reacquires and continues as if nothing happened. */
  isLost: () => boolean;
  /** Stops the renewal/watchdog timers and awaits any renewal call currently in flight -- used by graceful shutdown so a renewal write is never left dangling against a pool that is about to close. */
  stop: () => Promise<void>;
}

/**
 * Attempts to acquire the singleton lock ONCE. Returns `null` (never
 * throws for a normal contention case) if another instance currently
 * holds it -- the caller decides what that means (this server refuses
 * to start at all; see server.ts).
 *
 * `onLost` fires EXACTLY ONCE, either on a CONFIRMED "not acquired"
 * response from a renewal, or when the conservative expiry deadline
 * passes with no successful renewal in between (a run of transient DB
 * errors, for example) -- the second case is what actually fixes the
 * real gap found here: a renewal that only ever *errors* (never
 * receiving an explicit "not acquired" answer) previously left this
 * lock appearing valid indefinitely.
 */
export async function acquireSingletonLock(
  pool: mysql.Pool,
  ownerId: string,
  onLost: () => void,
  timings: SingletonLockTimings = DEFAULT_TIMINGS,
): Promise<SingletonLock | null> {
  const { leaseMs, renewIntervalMs, watchdogIntervalMs } = timings;
  let startedAt = performance.now();
  let first = await tryAcquireOrRenewExclusiveLock(pool, ownerId, leaseMs, DEMO_API_LOCK_NAME);
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
    startedAt = performance.now();
    first = await tryAcquireOrRenewExclusiveLock(pool, ownerId, leaseMs, DEMO_API_LOCK_NAME);
  }
  if (!first.acquired || performance.now() >= startedAt + leaseMs) return null;

  let lost = false;
  let leaseValidUntil = startedAt + leaseMs;
  const epoch = first.epoch;
  let renewing: Promise<void> | null = null;

  const declareLost = () => {
    if (lost) return; // fire onLost exactly once -- never re-enters valid afterward
    lost = true;
    clearInterval(renewTimer);
    clearInterval(watchdogTimer);
    onLost();
  };

  const renewOnce = async (): Promise<void> => {
    const requestStartedAt = performance.now();
    const previousDeadline = leaseValidUntil;
    if (requestStartedAt >= previousDeadline) { declareLost(); return; }
    try {
      const result = await tryAcquireOrRenewExclusiveLock(pool, ownerId, leaseMs, DEMO_API_LOCK_NAME);
      if (lost || !result.acquired || result.epoch !== epoch || performance.now() >= previousDeadline) {
        // CONFIRMED loss -- another owner (or a re-seeded fresh row) now
        // holds it. Never reacquire and continue; declare loss now.
        declareLost();
        return;
      }
      // Anchor before the DB call: response latency must not extend validity.
      leaseValidUntil = requestStartedAt + leaseMs;
    } catch {
      // A transient DB error is NOT confirmed loss -- `leaseValidUntil`
      // is deliberately left UNCHANGED here. If renewals keep failing
      // long enough for that already-tracked deadline to pass, the
      // watchdog below declares loss on its own, independent of ever
      // receiving an explicit "not acquired" answer.
    }
  };

  // Prevents overlapping renewals: if a renewal is already in flight
  // when the next tick fires, this tick is skipped entirely rather than
  // starting a second concurrent write.
  const renewTimer = setInterval(() => {
    if (lost || renewing) return;
    renewing = renewOnce().finally(() => {
      renewing = null;
    });
  }, renewIntervalMs);
  renewTimer.unref();

  const watchdogTimer = setInterval(() => {
    if (lost) return;
    if (performance.now() >= leaseValidUntil) declareLost();
  }, watchdogIntervalMs);
  watchdogTimer.unref();

  return {
    ownerId,
    isLost: () => {
      if (!lost && performance.now() >= leaseValidUntil) declareLost();
      return lost;
    },
    stop: async () => {
      clearInterval(renewTimer);
      clearInterval(watchdogTimer);
      if (renewing) await renewing.catch(() => {});
      // Release only this acquisition. Never clear a successor's lease.
      await pool.query({
        sql: "UPDATE publisher_leader SET owner_id = NULL, lease_expires_at = NULL WHERE lock_name = ? AND owner_id = ? AND epoch = ?",
        timeout: 2000,
      }, [DEMO_API_LOCK_NAME, ownerId, epoch]);
    },
  };
}
