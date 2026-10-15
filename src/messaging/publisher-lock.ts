import type mysql from "mysql2/promise";
import type { ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { getTracer } from "../telemetry/init.js";
import { setRelaylabAttributes } from "../telemetry/attributes.js";

/**
 * Single-row leader lock for the outbox publisher (section 4 of the
 * Phase 1.1 audit). Claimed and renewed with exactly the same
 * epoch+lease+authoritative-clock pattern as task ownership in
 * task-store.ts, reusing the SAME mutual-exclusion mechanism (one UPDATE
 * statement, gated by InnoDB row locking) rather than introducing a new
 * kind of lock. See docs/decisions/0006-publisher-leader-lock.md for the
 * full rationale and its explicit limits.
 *
 * What this DOES guarantee: at most one process's `tryAcquireOrRenew`
 * call can succeed for a given lock_name/epoch at a time, and a process
 * that stops renewing (crashed, or lost its DB connection) loses
 * leadership automatically once its lease expires -- another process's
 * next `tryAcquireOrRenew` call will then succeed with a new epoch.
 *
 * What this does NOT guarantee: that a publish the outgoing leader
 * already started (an in-flight `js.publish()` call to NATS) is
 * cancelled or fenced. A lease expiring does not reach into a live
 * network call in another process and stop it. If the old leader's
 * in-flight publish completes after a new leader has taken over, the
 * result is, at worst, the SAME outbox row being published twice
 * (broker-deduped by msgID, and harmless even if that dedup ever failed,
 * because the consumer's own durable dedup -- consumer.ts -- does not
 * trust the broker for correctness). This lock's job is to make
 * concurrent *polling and batch ordering* single-threaded, not to fence
 * side effects at the network boundary; Phase 1 has no side effects that
 * need that stronger guarantee yet (see invariant J).
 */
export async function tryAcquireOrRenewPublisherLeadership(
  pool: mysql.Pool,
  ownerId: string,
  leaseMs: number,
  lockName = "default",
): Promise<{ acquired: boolean; epoch: number }> {
  const tracer = getTracer();
  return tracer.startActiveSpan("relaylab.publisher_lock.acquire_or_renew", async (span) => {
    try {
      const outcome = await acquireOrRenew();
      setRelaylabAttributes(span, { outcome: outcome.acquired ? "acquired" : "lost", epoch: outcome.epoch });
      return outcome;
    } finally {
      span.end();
    }
  });

  async function acquireOrRenew(): Promise<{ acquired: boolean; epoch: number }> {
  // Column order in SET matters here, and was wrong until the Phase 1.2
  // audit caught it: MySQL evaluates a multi-column UPDATE's SET list
  // left to right, and a later expression that references a column
  // assigned EARLIER in the same list sees the NEW (just-written) value,
  // not the original row value (verified empirically against this
  // project's own MySQL instance; see docs/decisions/0008-set-clause-evaluation-order.md).
  // `epoch`'s CASE is listed FIRST, before `owner_id`/`lease_expires_at`
  // are reassigned, so `owner_id = ?` and `lease_expires_at >= NOW(3)`
  // inside it genuinely read the PRE-UPDATE row -- i.e. "was this already
  // owned, with a valid lease, by the SAME caller" -- rather than
  // comparing the new owner_id to itself (always true) and only
  // incidentally getting the right answer because the WHERE clause
  // happened to rule out the one case where that would matter.
  const [result] = await pool.query<ResultSetHeader>(
    `UPDATE publisher_leader
       SET epoch = CASE WHEN owner_id = ? AND lease_expires_at >= NOW(3) THEN epoch ELSE epoch + 1 END,
           owner_id = ?,
           lease_expires_at = DATE_ADD(NOW(3), INTERVAL ? MICROSECOND)
     WHERE lock_name = ?
       AND (owner_id IS NULL OR owner_id = ? OR lease_expires_at IS NULL OR lease_expires_at < NOW(3))`,
    [ownerId, ownerId, leaseMs * 1000, lockName, ownerId],
  );
  if (result.affectedRows !== 1) {
    return { acquired: false, epoch: -1 };
  }
  const [rows] = await pool.query<(RowDataPacket & { epoch: number })[]>(
    `SELECT epoch FROM publisher_leader WHERE lock_name = ?`,
    [lockName],
  );
  return { acquired: true, epoch: rows[0]?.epoch ?? -1 };
  }
}

/**
 * Phase 3A.1: the SAME generic, already-parameterized lock function
 * above (it only ever operated on `lock_name`, never assumed to be the
 * publisher specifically), reused under a neutral name for consumer-main.ts
 * -- see docs/KUBERNETES.md "Consumer exclusivity." Deliberately an
 * alias, not a copy: there is exactly one DB-lock implementation in this
 * project, used under two different `lockName` values ("default" for
 * the publisher, "consumer-default" for the consumer -- see
 * infra/migrations/0011_consumer_leader_lock.sql).
 */
export const tryAcquireOrRenewExclusiveLock = tryAcquireOrRenewPublisherLeadership;

export interface PublisherLeaderStatus {
  ownerId: string | null;
  epoch: number;
  leaseExpiresAtMs: number | null;
}

export async function getPublisherLeaderStatus(pool: mysql.Pool, lockName = "default"): Promise<PublisherLeaderStatus> {
  const [rows] = await pool.query<(RowDataPacket & { owner_id: string | null; epoch: number; lease_expires_at: Date | null })[]>(
    `SELECT owner_id, epoch, lease_expires_at FROM publisher_leader WHERE lock_name = ?`,
    [lockName],
  );
  const row = rows[0];
  if (!row) throw new Error(`publisher_leader row "${lockName}" does not exist -- did migrations run?`);
  return {
    ownerId: row.owner_id,
    epoch: row.epoch,
    leaseExpiresAtMs: row.lease_expires_at ? row.lease_expires_at.getTime() : null,
  };
}
