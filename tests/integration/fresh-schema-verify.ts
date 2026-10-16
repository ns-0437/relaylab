// Phase 3A.2: proves the committed migrations in THIS repository, run
// from scratch against a SEPARATELY NAMED, disposable database, produce
// a working schema -- independent of the existing demo database's own
// history (which carries an unexplained gap: a historical
// `0011_consumer_leader_lock.sql` row with a checksum that does not
// match any file in this repository -- see docs/KUBERNETES.md "Fresh-
// schema reproducibility" for that provenance limitation, stated
// plainly, not invented or guessed at here).
//
// Never touches `relaylab` (the dev database) or `relaylab_test` (the
// CI database) -- creates its OWN database, verifies against it, then
// drops it. Uses the existing migration mechanism (applyMigrations) and
// its existing safety rule (assertSafeTarget: loopback host, database
// name must start with "relaylab") unchanged.
//
// Run: npm run demo:fresh-schema
import mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { Effect } from "effect";
import { loadMysqlConfig, loadNatsConfig, eventsStream, eventsDurable } from "../../src/config.js";
import { applyMigrations } from "../../src/db/migrate.js";
import { createPool } from "../../src/db/pool.js";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { connectNats, ensureEventsStream, ensureDurableConsumer } from "../../src/messaging/nats-client.js";
import { runPublisherOnce } from "../../src/messaging/publisher.js";
import { applyEventWithDedup, defaultConsumerOptions } from "../../src/messaging/consumer.js";
import { decodeEventEnvelope } from "../../src/protocol/protocol.js";

const FRESH_DB = process.env.RELAYLAB_FRESH_DB ?? "relaylab_fresh_verify";
const ROOT_USER = "root";
const ROOT_PASSWORD = process.env.RELAYLAB_MYSQL_ROOT_PASSWORD ?? "relaylab_dev_root";

function log(msg: string) {
  console.log(`[fresh-schema] ${msg}`);
}

async function main() {
  if (FRESH_DB === "relaylab" || FRESH_DB === "relaylab_test") {
    throw new Error(`refusing to use "${FRESH_DB}" -- it must be a database SEPARATE from the dev/CI databases.`);
  }
  const base = loadMysqlConfig();
  log(`target database: "${FRESH_DB}" on ${base.host}:${base.port} (disposable -- created and dropped by this script only)`);

  const admin = await mysql.createConnection({ host: base.host, port: base.port, user: ROOT_USER, password: ROOT_PASSWORD });
  try {
    await admin.query(`DROP DATABASE IF EXISTS \`${FRESH_DB}\``);
    await admin.query(`CREATE DATABASE \`${FRESH_DB}\``);
    log(`created a fresh, empty database "${FRESH_DB}"`);
  } finally {
    await admin.end();
  }

  const freshConfig = { ...base, user: ROOT_USER, password: ROOT_PASSWORD, database: FRESH_DB };

  try {
    log("=== run 1: applying every committed migration to the fresh database ===");
    const run1 = await applyMigrations(freshConfig);
    log(`run 1: applied=${run1.appliedCount} total=${run1.totalCount}`);
    if (run1.appliedCount !== run1.totalCount) {
      throw new Error(`expected a FRESH database to newly apply ALL ${run1.totalCount} committed migrations, but only ${run1.appliedCount} were applied`);
    }

    log("=== run 2: re-running migrations against the SAME database -- must be a true no-op ===");
    const run2 = await applyMigrations(freshConfig);
    log(`run 2: applied=${run2.appliedCount} total=${run2.totalCount}`);
    if (run2.appliedCount !== 0) {
      throw new Error(`expected the second run to apply ZERO new migrations (idempotency), got ${run2.appliedCount}`);
    }
    log("PASS: migrations are idempotent on re-run");

    const pool = createPool(freshConfig);
    try {
      log("=== checking required consumer-lock state ===");
      const [lockRows] = await pool.query<(RowDataPacket & { lock_name: string; epoch: number })[]>(
        `SELECT lock_name, epoch FROM publisher_leader ORDER BY lock_name`,
      );
      const lockNames = lockRows.map((r) => r.lock_name);
      log(`publisher_leader rows: ${JSON.stringify(lockRows)}`);
      if (!lockNames.includes("consumer-default")) {
        throw new Error(`expected a "consumer-default" row (added by 0012_consumer_leader_lock.sql) -- not found. Found: ${JSON.stringify(lockNames)}`);
      }
      if (!lockNames.includes("default")) {
        throw new Error(`expected the publisher's own "default" row (added by 0002) -- not found.`);
      }
      log(
        `PASS: both lock rows present (${lockNames.join(", ")}) on a database that NEVER had a migration numbered ` +
          `0011 of any kind -- 0012 is self-contained; no current operation depends on the old demo database's ` +
          `unexplained historical 0011 row.`,
      );

      log("=== representative task/receipt via the real TaskStore ===");
      const store = makeTaskStoreLive(pool);
      const taskId = `fresh-schema-verify-${Date.now()}`;
      const claim = await Effect.runPromise(store.claimTask(taskId, "fresh-schema-worker", 30_000));
      const receipt = await Effect.runPromise(
        store.applyTaskCommand({
          taskId,
          ownerId: "fresh-schema-worker",
          epoch: claim.epoch,
          commandId: "cmd-1",
          command: { type: "Checkpoint", digest: "fresh-schema-digest" },
        }),
      );
      log(`PASS: claimed task at epoch ${claim.epoch}; command receipt replay=${receipt.replay}`);

      log("=== representative consumer operation (real NATS, isolated namespace) ===");
      const namespace = `relaylab-fresh-verify-${Date.now()}`;
      process.env.RELAYLAB_NAMESPACE = namespace; // isolates subject/stream/durable names, see src/config.ts
      const { nc, js, jsm } = await connectNats(loadNatsConfig().servers);
      try {
        await ensureEventsStream(jsm);
        await ensureDurableConsumer(jsm);
        await runPublisherOnce(pool, js);
        const fetch = js.fetch(eventsStream(), eventsDurable(), { batch: 1, expires: 3000 });
        const msgs = [];
        for await (const m of fetch) msgs.push(m);
        if (msgs.length !== 1) throw new Error(`expected exactly 1 published message for the representative task, got ${msgs.length}`);
        const decoded = decodeEventEnvelope(JSON.parse(new TextDecoder().decode(msgs[0]!.data)));
        if (decoded._tag !== "Right") throw new Error("failed to decode the representative event");
        const consumerName = defaultConsumerOptions().consumerName;
        const result = await applyEventWithDedup(pool, consumerName, decoded.right);
        log(`PASS: the consumer applied the representative event, outcome=${result}`);
        const [stateRows] = await pool.query<(RowDataPacket & { event_count: number; last_applied_seq: number })[]>(
          `SELECT event_count, last_applied_seq FROM task_consumer_state WHERE task_id = ?`,
          [taskId],
        );
        log(`task_consumer_state: ${JSON.stringify(stateRows[0])}`);
        if (!stateRows[0] || stateRows[0].event_count !== 1) {
          throw new Error(`expected event_count=1 on the fresh schema, got ${JSON.stringify(stateRows[0])}`);
        }
      } finally {
        await nc.drain();
      }
    } finally {
      await pool.end();
    }

    log("ALL CHECKS PASSED on the fresh schema.");
  } finally {
    const admin2 = await mysql.createConnection({ host: base.host, port: base.port, user: ROOT_USER, password: ROOT_PASSWORD });
    try {
      await admin2.query(`DROP DATABASE IF EXISTS \`${FRESH_DB}\``);
      log(`dropped the disposable database "${FRESH_DB}" -- "relaylab" and "relaylab_test" were never touched by this script.`);
    } finally {
      await admin2.end();
    }
  }
}

main().catch((err) => {
  console.error("[fresh-schema] FAILED:", err);
  process.exitCode = 1;
});
