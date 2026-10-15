// Phase 1.2.1 monitoring-reliability patch, section 1: durable-state
// gauges backed by bounded periodic DB queries, never a per-scrape
// rescan. See docs/METRICS.md "Part C" dictionary entries for outbox/
// gap/quarantine gauges, and "Gauge ownership and aggregation" for why
// each gauge group has exactly one owning process role.
//
// Pattern: a COMPLETION-BASED scheduler (never `setInterval` racing an
// async function -- see "Overlapping refresh prevention" below) queries
// the DB on a bounded cadence and updates a small in-memory cache; a
// registered ObservableGauge callback (invoked by the Prometheus
// exporter at SCRAPE time, which may be more or less frequent than the
// refresh interval) reads ONLY the cache -- it never touches the
// database. On a refresh failure, the cache is left exactly as it was
// (stale, not zeroed) and a separate failure counter + "last successful
// refresh" gauge make the staleness visible instead of silently
// reporting an empty/zero queue as if the query had succeeded.
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { getObservableGauge, recordGaugeRefreshFailure, type ObservableResult, type GaugeGroup } from "./metrics.js";
import { classifyShutdownError } from "./init.js";

interface Cache {
  values: Record<string, number>;
  lastSuccessMs: number | undefined;
}

export interface Refresher {
  stop: () => Promise<void>;
  /** Test/diagnostic only: current scheduler state. */
  getState: () => "idle" | "refreshing" | "stopped";
  /** Test/diagnostic only: how many refresh() calls have ever been in flight simultaneously (should never exceed 1). */
  getMaxConcurrent: () => number;
}

/**
 * Overlapping-refresh prevention (Phase 1.2.1 monitoring-reliability
 * patch, section 1). The ORIGINAL implementation was:
 *
 *   void refresh();
 *   const timer = setInterval(refresh, intervalMs);
 *
 * `setInterval` fires on a fixed wall-clock cadence REGARDLESS of
 * whether the previous `refresh()` call's promise has settled -- a
 * query slower than `intervalMs` (a real possibility under DB load, a
 * lock wait, or a network hiccup) would start a SECOND overlapping
 * query before the first one returned, and if they completed
 * out-of-order, an OLDER (slower) refresh's result could silently
 * overwrite a NEWER (faster) one's already-cached value.
 *
 * Fixed with COMPLETION-BASED scheduling (the simpler of the two
 * documented-acceptable approaches -- see this function's instruction):
 * the NEXT refresh is only ever scheduled (via `setTimeout`, not
 * `setInterval`) AFTER the current one's promise has settled. This
 * makes "at most one in-flight refresh" and "no older refresh can ever
 * overwrite newer cached state" both structurally true by construction
 * (there is only ever one `refresh()` call object alive at a time) --
 * not something that needs a separate lock flag or generation counter
 * to enforce.
 *
 * Bounded query strategy: each query is issued with mysql2's own
 * `timeout` option (`pool.query({ sql, timeout: queryTimeoutMs }, ...)`).
 * **This does NOT cancel the query on the MySQL SERVER** -- mysql2's own
 * documentation states plainly that reaching this timeout "will be
 * destroyed" (the CLIENT connection, not the server-side query). The
 * server may continue executing the query to completion, consuming
 * server resources, until it independently notices the client
 * disconnected. `Promise.race`-ing a query against a timer (the pattern
 * this project's OWN telemetry shutdown code uses deliberately, and
 * explicitly does NOT claim cancels the underlying operation -- see
 * src/telemetry/init.ts) has the exact same property here: the
 * connection is destroyed and returned to the pool as unusable (mysql2
 * discards it, a fresh connection is created on the pool's next
 * acquire), but the in-flight SQL statement on the server is NOT proven
 * stopped by this mechanism. This is a stated, accepted limitation, not
 * a claim of query cancellation -- see docs/METRICS.md "bounded query
 * strategy" for the same statement in the design doc.
 */
function makeRefresher(
  group: GaugeGroup,
  pool: mysql.Pool,
  intervalMs: number,
  queryTimeoutMs: number,
  query: (pool: mysql.Pool, queryTimeoutMs: number) => Promise<Record<string, number>>,
  gaugeNames: { name: string; description: string; unit?: string }[],
): Refresher {
  const cache: Cache = { values: {}, lastSuccessMs: undefined };
  let state: "idle" | "refreshing" | "stopped" = "idle";
  let timer: ReturnType<typeof setTimeout> | undefined;
  let inFlight: Promise<void> | undefined;
  let concurrentCount = 0;
  let maxConcurrent = 0;

  for (const g of gaugeNames) {
    getObservableGauge(g.name, g.description, g.unit).addCallback((result: ObservableResult) => {
      const v = cache.values[g.name];
      if (v !== undefined) result.observe(v);
      // If undefined (never successfully refreshed yet, OR -- section 1's
      // "initial failure is not displayed as a confirmed zero" -- the
      // very first refresh attempt already failed), deliberately observes
      // NOTHING this scrape -- an absent series is Prometheus's own
      // honest "no data," never a fabricated 0 standing in for "we don't
      // actually know yet." See docs/METRICS.md "missing metrics must
      // not silently appear as healthy zeroes".
    });
  }
  // Shared gauge NAME across every group (outbox/gaps/quarantine), each
  // contributing its own callback with its own `group` label -- see
  // getObservableGauge's doc comment (one instrument, multiple callbacks).
  getObservableGauge(
    "relaylab_gauge_last_refresh_success_timestamp_seconds",
    "Unix timestamp (seconds) of the last successful durable-gauge refresh for this group",
    "s",
  ).addCallback((result: ObservableResult) => {
    if (cache.lastSuccessMs !== undefined) result.observe(cache.lastSuccessMs / 1000, { group });
  });

  async function runOneRefresh(): Promise<void> {
    concurrentCount++;
    maxConcurrent = Math.max(maxConcurrent, concurrentCount);
    try {
      const values = await query(pool, queryTimeoutMs);
      // Successful-refresh timestamp is updated ONLY here, after the
      // query has actually resolved -- never optimistically before.
      cache.values = values;
      cache.lastSuccessMs = Date.now();
    } catch (err) {
      recordGaugeRefreshFailure(group);
      // Sanitized, allowlisted classification -- the SAME finite
      // classifier telemetry shutdown logging uses (constructor name,
      // plus a known-safe `.code` only -- never a raw error message,
      // which for a MySQL error can embed query/connection detail).
      console.error(`[durable-gauges] ${group} refresh failed: ${classifyShutdownError(err)}`);
      // cache is intentionally left untouched -- stale data, never wiped
      // or reset to 0 ("honest stale state after failures").
    } finally {
      concurrentCount--;
    }
  }

  function scheduleNext(): void {
    if (state === "stopped") return;
    state = "idle";
    timer = setTimeout(tick, intervalMs);
    timer.unref(); // never keeps the process alive on its own -- see src/process/natural-exit.ts
  }

  function tick(): void {
    if (state === "stopped") return;
    state = "refreshing";
    inFlight = runOneRefresh().finally(() => {
      inFlight = undefined;
      scheduleNext();
    });
  }

  tick(); // first refresh runs immediately, not after waiting a full interval

  return {
    async stop(): Promise<void> {
      if (state === "stopped") return;
      state = "stopped";
      if (timer) clearTimeout(timer);
      timer = undefined;
      // "A stop operation that... handles the current refresh": if one
      // is in flight when stop() is called, this AWAITS it (its own
      // `finally` sees `state === "stopped"` and does not reschedule)
      // rather than abandoning it mid-flight -- the caller (e.g. a
      // `*-main.ts` shutdown path about to call `pool.end()`) can safely
      // assume no further query against `pool` will be issued by this
      // refresher once `stop()` resolves.
      if (inFlight) await inFlight;
    },
    getState: () => state,
    getMaxConcurrent: () => maxConcurrent,
  };
}

interface OutboxCountRow extends RowDataPacket {
  unpublished_count: number;
  oldest_age_seconds: number | null;
}

/**
 * Owned by the current publisher LEADER only (see docs/METRICS.md) --
 * callers must start this ONLY while `wasLeader` is true and stop it the
 * moment leadership is lost, so a standby publisher instance never
 * exposes a duplicate/stale copy of this durable, single-owner state.
 */
export function startOutboxGaugeRefresher(pool: mysql.Pool, intervalMs = 5000, queryTimeoutMs = 3000): Refresher {
  return makeRefresher(
    "outbox",
    pool,
    intervalMs,
    queryTimeoutMs,
    async (p, timeoutMs) => {
      // Uses the existing idx_outbox_unpublished (published_at, id) index
      // -- a bounded index range scan, not a full-table scan, regardless
      // of how large outbox_events grows overall.
      const [rows] = await p.query<OutboxCountRow[]>({
        sql: `SELECT COUNT(*) AS unpublished_count,
                     TIMESTAMPDIFF(MICROSECOND, MIN(created_at), NOW(3)) / 1000000 AS oldest_age_seconds
              FROM outbox_events WHERE published_at IS NULL`,
        timeout: timeoutMs,
      });
      const row = rows[0];
      return {
        relaylab_outbox_unpublished_count: row?.unpublished_count ?? 0,
        relaylab_outbox_oldest_unpublished_age_seconds: row?.oldest_age_seconds ?? 0,
      };
    },
    [
      { name: "relaylab_outbox_unpublished_count", description: "Durable count of unpublished outbox_events rows" },
      { name: "relaylab_outbox_oldest_unpublished_age_seconds", description: "Age of the oldest unpublished outbox_events row", unit: "s" },
    ],
  );
}

/**
 * Owned by the consumer process -- see docs/METRICS.md "Gauge
 * ownership." Unlike the outbox refresher above, there is NO real
 * election mechanism preventing two consumer instances from both
 * running this for the SAME `consumerName` (horizontal consumer scaling
 * is an explicit non-goal -- see CLAUDE.md) -- section 6 of the
 * monitoring-reliability patch asks that an unsupported multi-instance
 * topology be made VISIBLE rather than silently producing duplicate
 * series. `instanceId` (a process-unique identity, e.g.
 * `consumer-${pid}-${random}`, mirroring `publisher-main.ts`'s
 * `OWNER_ID` pattern) is heartbeated into `consumer_gauge_exporters` on
 * every refresh cycle; `relaylab_consumer_gauge_exporter_count` exposes
 * how many DISTINCT instances have heartbeated recently for this
 * `consumerName` -- a value > 1 is the unsupported case, directly
 * observable via that gauge and `RelaylabDuplicateConsumerGaugeExporters`
 * (infra/prometheus-alerts.yml), not hidden behind two processes each
 * quietly exposing what looks like a normal, single-owner series.
 */
export function startConsumerGaugeRefresher(pool: mysql.Pool, consumerName: string, instanceId: string, intervalMs = 5000, queryTimeoutMs = 3000): Refresher {
  // A heartbeat is considered "recent" within 3 refresh intervals --
  // generous enough that normal refresh jitter/a single slow cycle
  // never produces a false "duplicate exporter" positive, while a
  // genuinely stopped second instance's heartbeat still ages out and
  // stops being counted within a bounded window (not forever).
  const heartbeatStaleAfterMs = intervalMs * 3;
  return makeRefresher(
    "gaps",
    pool,
    intervalMs,
    queryTimeoutMs,
    async (p, timeoutMs) => {
      await p.query({
        sql: `INSERT INTO consumer_gauge_exporters (consumer_name, instance_id, last_heartbeat_at)
              VALUES (?, ?, NOW(3))
              ON DUPLICATE KEY UPDATE last_heartbeat_at = NOW(3)`,
        timeout: timeoutMs,
        values: [consumerName, instanceId],
      } as never);
      const [pendingRows] = await p.query<(RowDataPacket & { c: number })[]>({
        sql: `SELECT COUNT(*) AS c FROM task_consumer_pending`,
        timeout: timeoutMs,
      });
      const [gapRows] = await p.query<(RowDataPacket & { c: number })[]>({
        sql: `SELECT COUNT(*) AS c FROM task_consumer_state WHERE gap_detected = 1`,
        timeout: timeoutMs,
      });
      const [quarantineRows] = await p.query<(RowDataPacket & { c: number })[]>({
        sql: `SELECT COUNT(*) AS c FROM consumer_quarantine`,
        timeout: timeoutMs,
      });
      const [exporterRows] = await p.query<(RowDataPacket & { c: number })[]>({
        sql: `SELECT COUNT(*) AS c FROM consumer_gauge_exporters WHERE consumer_name = ? AND last_heartbeat_at > NOW(3) - INTERVAL ? MICROSECOND`,
        timeout: timeoutMs,
        values: [consumerName, heartbeatStaleAfterMs * 1000],
      } as never);
      return {
        relaylab_pending_gap_events_count: pendingRows[0]?.c ?? 0,
        relaylab_tasks_with_unresolved_gaps_count: gapRows[0]?.c ?? 0,
        relaylab_quarantined_messages_count: quarantineRows[0]?.c ?? 0,
        relaylab_consumer_gauge_exporter_count: exporterRows[0]?.c ?? 0,
      };
    },
    [
      { name: "relaylab_pending_gap_events_count", description: "Durable count of all task_consumer_pending rows" },
      { name: "relaylab_tasks_with_unresolved_gaps_count", description: "Durable count of tasks with gap_detected = 1" },
      { name: "relaylab_quarantined_messages_count", description: "Durable count of all consumer_quarantine rows" },
      {
        name: "relaylab_consumer_gauge_exporter_count",
        description: "Number of distinct consumer instances currently heartbeating as a gauge exporter for this consumer_name -- >1 means an UNSUPPORTED multi-instance topology",
      },
    ],
  );
}

/** Exported for direct testing of the scheduling/overlap-prevention behavior with an injected, controllable query function. See tests/integration/durable-gauges.test.ts. */
export { makeRefresher as makeRefresherForTesting };
