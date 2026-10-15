import mysql from "mysql2/promise";
import { connect, type NatsConnection, type JetStreamClient, type JetStreamManager } from "nats";
import { loadTestMysqlConfig, loadNatsConfig, eventsStream, eventsDurable } from "../../src/config.js";
import { ensureDurableConsumer, ensureEventsStream } from "../../src/messaging/nats-client.js";
import { applyMigrations } from "../../src/db/migrate.js";

const INTEGRATION_HINT =
  "Integration tests require real MySQL + NATS. Start them with:\n" +
  "  docker compose -f infra/compose.yaml up -d\n" +
  "(infra/mysql-init/01-test-database.sql creates the disposable relaylab_test\n" +
  "database automatically on first container start). Set RELAYLAB_MYSQL_* /\n" +
  "RELAYLAB_TEST_MYSQL_DATABASE / RELAYLAB_NATS_URL env vars if not using the\n" +
  "compose defaults.";

// Every integration test in this suite sets this BEFORE importing/calling
// anything from src/config.js's eventsStream()/eventsSubject()/eventsDurable(),
// so tests never share a NATS stream/subject/durable-consumer name with a
// running dev publisher/consumer/demo (see src/config.ts "namespace").
// Setting it here, at module load of the FIRST helpers.ts import in a test
// file, is sufficient because those functions re-read process.env on every
// call rather than caching a value at module-load time.
process.env.RELAYLAB_NAMESPACE = "relaylab-test";

/**
 * A SEPARATE, disposable MySQL database (relaylab_test, not the "relaylab"
 * dev database) so that truncateAll() below can never touch anything a
 * running dev publisher/consumer/demo is using. See
 * infra/mysql-init/01-test-database.sql and src/config.ts loadTestMysqlConfig.
 */
export async function requireMysqlPool(): Promise<mysql.Pool> {
  const config = loadTestMysqlConfig();
  const pool = mysql.createPool({ ...config, waitForConnections: true, connectionLimit: 10 });
  try {
    const conn = await pool.getConnection();
    conn.release();
  } catch (err) {
    await pool.end().catch(() => {});
    throw new Error(
      `Cannot reach the relaylab_test MySQL database at ${config.host}:${config.port}/${config.database} ` +
        `(${err instanceof Error ? err.message : err}).\n${INTEGRATION_HINT}`,
    );
  }
  return pool;
}

/** Applies infra/migrations/*.sql against the disposable test database, via the SAME ledger `npm run migrate` uses. */
export async function applySchema(_pool: mysql.Pool): Promise<void> {
  await applyMigrations(loadTestMysqlConfig());
}

export async function truncateAll(pool: mysql.Pool): Promise<void> {
  const conn = await pool.getConnection();
  try {
    await conn.query("SET FOREIGN_KEY_CHECKS=0");
    for (const t of [
      "consumer_quarantine",
      "consumer_gauge_exporters",
      "task_consumer_pending",
      "consumer_event_conflicts",
      "task_consumer_state",
      "consumer_processed",
      "outbox_events",
      "task_checkpoints",
      "task_handoffs",
      "command_receipts",
      "tasks",
      "publisher_leader",
    ]) {
      await conn.query(`TRUNCATE TABLE ${t}`);
    }
    await conn.query(`INSERT INTO publisher_leader (lock_name, epoch) VALUES ('default', 0)`);
    await conn.query("SET FOREIGN_KEY_CHECKS=1");
  } finally {
    conn.release();
  }
}

export interface NatsTestHandles {
  nc: NatsConnection;
  js: JetStreamClient;
  jsm: JetStreamManager;
}

export async function requireNats(): Promise<NatsTestHandles> {
  const { servers } = loadNatsConfig();
  let nc: NatsConnection;
  try {
    nc = await connect({ servers, timeout: 3000 });
  } catch (err) {
    throw new Error(
      `Cannot reach NATS at ${servers} (${err instanceof Error ? err.message : err}).\n${INTEGRATION_HINT}`,
    );
  }
  const jsm = await nc.jetstreamManager();
  const js = nc.jetstream();
  return { nc, js, jsm };
}

/**
 * Resets the TEST-namespaced stream (relaylab-test.events /
 * RELAYLAB_TEST_EVENTS), never the dev one, between tests, by purging its
 * messages. Deliberately does NOT delete/recreate the stream or the
 * durable consumer on every test (both were tried and both produced a
 * reproducible spurious "503" NatsError on a LATER js.fetch() call within
 * the SAME test run -- see docs/PROGRESS.md "Phase 1.1 audit" for the
 * investigation. Leaving the stream and consumer alive for the whole test
 * file and only purging messages between tests avoids that race entirely,
 * and is sufficient: a purge advances the stream's first_seq past
 * whatever existed before, so a fresh pull finds nothing left over from
 * the previous test.
 */
export async function resetStream(jsm: JetStreamManager): Promise<void> {
  await ensureEventsStream(jsm);
  await ensureDurableConsumer(jsm);
  await jsm.streams.purge(eventsStream());
}

export function uniqueId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export { eventsStream, eventsDurable };
