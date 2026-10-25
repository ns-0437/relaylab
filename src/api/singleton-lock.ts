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
  // Anchored BEFORE each call, not after it resolves -- see the
  // `renewOnce` comment below for why this is the conservative choice.
  // Recomputed for whichever attempt actually ends up succeeding.
  let acquiredAt = Date.now();
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
    acquiredAt = Date.now();
    first = await tryAcquireOrRenewExclusiveLock(pool, ownerId, leaseMs, DEMO_API_LOCK_NAME);
  }
  // Same conservative anchoring as every renewal below: if the INITIAL
  // acquisition's own response was itself delayed long enough that the
  // lease it reports would already be expired by the time we know
  // about it, this process never genuinely held a USABLE lease -- treat
  // it the same as not acquired, rather than returning a lock object
  // that would just self-declare lost on its very first check.
  if (!first.acquired || Date.now() >= acquiredAt + leaseMs) return null;

  let lost = false;
  let leaseValidUntil = acquiredAt + leaseMs;
  let currentEpoch = first.epoch;
  let renewing: Promise<void> | null = null;

  const declareLost = () => {
    if (lost) return; // fire onLost exactly once -- never re-enters valid afterward
    lost = true;
    clearInterval(renewTimer);
    clearInterval(watchdogTimer);
    onLost();
  };

  /** Synchronous, checked from several call sites (not only the watchdog's own ticks) -- declares loss immediately if the tracked deadline has already passed, rather than waiting for the next scheduled tick to notice. */
  const checkExpiryNow = (): void => {
    if (!lost && Date.now() > leaseValidUntil) declareLost();
  };

  const renewOnce = async (): Promise<void> => {
    // Checked BEFORE issuing the call, not only by the watchdog's own
    // ticks -- a renewal that is already past its deadline must not
    // even attempt to extend what the DB may already consider expired.
    checkExpiryNow();
    if (lost) return;

    // Anchored to when THIS call starts, not when its response arrives
    // -- see the acquisition-time comment above. The DB computes the
    // new lease from ITS OWN clock at execution time, which is always
    // at or after this call started but strictly before the response
    // can reach us; using the response-completion time to compute
    // `leaseValidUntil` would let network/query latency on the return
    // trip make this process believe the lease lasts LONGER than the
    // database will actually honor.
    const callStartedAt = Date.now();
    let result: { acquired: boolean; epoch: number };
    try {
      result = await tryAcquireOrRenewExclusiveLock(pool, ownerId, leaseMs, DEMO_API_LOCK_NAME);
    } catch {
      // A transient DB error is NOT confirmed loss -- `leaseValidUntil`
      // is deliberately left UNCHANGED here. If renewals keep failing
      // long enough for that already-tracked deadline to pass, the
      // watchdog (or the next renewal's own checkExpiryNow) declares
      // loss on its own, independent of ever receiving an explicit
      // "not acquired" answer.
      return;
    }

    // A response that arrives after loss was ALREADY declared (by the
    // watchdog, or by another path, while this call was in flight) is
    // stale -- discarded outright, never used to extend validity or
    // otherwise treated as if ownership had been continuous.
    if (lost) return;

    if (!result.acquired) {
      // CONFIRMED loss -- another owner (or a re-seeded fresh row) now
      // holds it. Never reacquire and continue; declare loss now.
      declareLost();
      return;
    }
    if (result.epoch !== currentEpoch) {
      // The row's epoch only changes when a lease is claimed after
      // having been allowed to EXPIRE (see publisher-lock.ts's epoch
      // CASE) -- even if the `owner_id` this acquisition reports back
      // is still OUR OWN, a changed epoch proves this row went through
      // a real expiry-and-reclaim cycle, not an uninterrupted renewal.
      // Ownership continuity is not guaranteed across that gap (another
      // owner could have held it in between), so this is treated as
      // lost, never as if nothing happened.
      declareLost();
      return;
    }
    currentEpoch = result.epoch;
    leaseValidUntil = callStartedAt + leaseMs;
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

  const watchdogTimer = setInterval(checkExpiryNow, watchdogIntervalMs);
  watchdogTimer.unref();

  return {
    ownerId,
    isLost: () => {
      // Checked synchronously on every call, not only on a watchdog
      // tick's own cadence -- admission/status checks (server.ts calls
      // this directly) must never observe a stale "not lost" answer
      // merely because the next scheduled tick hasn't fired yet.
      checkExpiryNow();
      return lost;
    },
    stop: async () => {
      clearInterval(renewTimer);
      clearInterval(watchdogTimer);
      if (renewing) await renewing.catch(() => {});
      if (lost) return;
      // Released explicitly (still-owned, same epoch only) so a clean
      // restart need not wait out the lease. The next acquirer gets a
      // NEW epoch, since this owner no longer matches the row.
      lost = true;
      await pool
        .query(`UPDATE publisher_leader SET owner_id = NULL, lease_expires_at = NULL WHERE lock_name = ? AND owner_id = ? AND epoch = ?`, [DEMO_API_LOCK_NAME, ownerId, currentEpoch])
        .catch(() => {});
    },
  };
}
