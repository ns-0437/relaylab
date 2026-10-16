// Phase 3A.2 corrective patch: this file previously ran `DROP DATABASE
// IF EXISTS` on an environment-selected name BEFORE ever calling
// applyMigrations' own safety check -- meaning an arbitrary
// RELAYLAB_FRESH_DB value (anything starting with "relaylab", which is
// all applyMigrations itself required) could destroy an EXISTING
// database with that name before this script ever got around to
// validating anything. That is fixed here by removing the override
// entirely and restructuring the whole lifecycle around a single rule:
// this script NEVER issues DROP DATABASE on a name it did not itself,
// successfully, just create in THIS run.
//
// Proves the committed migrations in THIS repository, run from scratch
// against a database unique to this run, produce a working schema --
// independent of the existing demo database's own history (which
// carries an unexplained gap: a historical `0011_consumer_leader_lock.sql`
// row with a checksum that does not match any file in this repository --
// see docs/KUBERNETES.md "Fresh-schema reproducibility" for that
// provenance limitation, stated plainly, not invented or guessed at here).
//
// Run: npm run demo:fresh-schema
import mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { randomBytes } from "node:crypto";
import { Effect } from "effect";
import { loadMysqlConfig, loadNatsConfig, eventsStream, eventsDurable } from "../../src/config.js";
import { applyMigrations } from "../../src/db/migrate.js";
import { createPool } from "../../src/db/pool.js";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { connectNats, ensureEventsStream, ensureDurableConsumer } from "../../src/messaging/nats-client.js";
import { runPublisherOnce } from "../../src/messaging/publisher.js";
import { applyEventWithDedup, defaultConsumerOptions } from "../../src/messaging/consumer.js";
import { decodeEventEnvelope } from "../../src/protocol/protocol.js";

const ROOT_USER = "root";
const ROOT_PASSWORD = process.env.RELAYLAB_MYSQL_ROOT_PASSWORD ?? "relaylab_dev_root";

// A STRICT prefix + charset, enforced on every identifier this module
// will ever pass to CREATE/DROP DATABASE. There is deliberately no
// environment-variable override of the generated name in normal
// operation (see header comment) -- the only way a test can exercise
// the collision-refusal path is by importing `createFreshDatabase`
// directly and passing a name that STILL has to pass this same
// validation, never by influencing this script's own CLI/env surface.
const DB_NAME_PATTERN = /^relaylab_freshtest_[0-9]+_[a-f0-9]{12,}$/;

export function generateDatabaseName(): string {
  return `relaylab_freshtest_${Date.now()}_${randomBytes(6).toString("hex")}`;
}

export function assertValidIdentifier(name: string): void {
  if (!DB_NAME_PATTERN.test(name)) {
    throw new Error(
      `refusing to use database identifier "${name}" -- it does not match the required pattern ${DB_NAME_PATTERN.source} ` +
        `(a strict, generated-only test prefix). This check runs BEFORE any administrative DDL.`,
    );
  }
}

export function assertLoopbackHost(host: string): void {
  if (!["127.0.0.1", "localhost", "::1"].includes(host)) {
    throw new Error(`refusing to run fresh-schema verification against non-loopback host "${host}". This check runs BEFORE any administrative DDL.`);
  }
}

// MySQL has no parameterized identifiers. Backtick-quoting with any
// embedded backtick doubled is the correct escape -- applied even
// though assertValidIdentifier's own regex already makes an embedded
// backtick impossible for any name this module generates; this is
// defense in depth, not the only thing standing between untrusted input
// and a DDL statement.
export function escapeIdentifier(name: string): string {
  return "`" + name.replace(/`/g, "``") + "`";
}

function log(msg: string) {
  console.log(`[fresh-schema] ${msg}`);
}

export interface CreateDatabaseResult {
  created: boolean;
  dbName: string;
}

/**
 * Creates a NEW, empty database and nothing else. Validates the host
 * and the identifier BEFORE issuing any DDL. Never precedes CREATE with
 * a DROP -- if `dbName` (defaulted to a freshly generated, effectively
 * unique name, but overridable by tests to exercise the collision path
 * deliberately) already exists, plain `CREATE DATABASE` (no `IF NOT
 * EXISTS`) fails on its own, and that failure is returned as
 * `created: false` with the existing database left COMPLETELY
 * untouched -- this function never attempts to clear it.
 */
export async function createFreshDatabase(host: string, port: number, dbNameOverride?: string): Promise<CreateDatabaseResult> {
  assertLoopbackHost(host);
  const dbName = dbNameOverride ?? generateDatabaseName();
  assertValidIdentifier(dbName);
  const escaped = escapeIdentifier(dbName);

  const admin = await mysql.createConnection({ host, port, user: ROOT_USER, password: ROOT_PASSWORD });
  try {
    await admin.query(`CREATE DATABASE ${escaped}`);
    return { created: true, dbName };
  } catch (err) {
    // A collision (ER_DB_CREATE_EXISTS) or any other CREATE failure:
    // reported, but the existing database (if any) was never touched --
    // no DROP was ever issued for this name.
    throw new Error(`could not create database "${dbName}" (left completely untouched if it already existed): ${err instanceof Error ? err.message : String(err)}`);
  } finally {
    await admin.end();
  }
}

/**
 * Drops a database ONLY when `weCreatedIt` is true -- i.e. ONLY when
 * THIS invocation's own `createFreshDatabase` call reported
 * `created: true` for this exact name. A failed create must never
 * reach this function with `weCreatedIt: true`; callers are expected
 * to pass the literal boolean they got back from `createFreshDatabase`,
 * never to infer it. Re-validates the identifier before DROP too.
 */
export async function dropDatabaseIfOwned(host: string, port: number, dbName: string, weCreatedIt: boolean): Promise<void> {
  if (!weCreatedIt) {
    log(`skipping drop of "${dbName}" -- this invocation did not create it, so nothing of ours exists to remove.`);
    return;
  }
  assertValidIdentifier(dbName);
  const escaped = escapeIdentifier(dbName);
  const admin = await mysql.createConnection({ host, port, user: ROOT_USER, password: ROOT_PASSWORD });
  try {
    await admin.query(`DROP DATABASE ${escaped}`);
    log(`dropped the database "${dbName}" that THIS invocation created -- no other database was touched.`);
  } finally {
    await admin.end();
  }
}

export interface VerificationOutcome {
  dbName: string;
  natsStreamName?: string;
  natsNamespace?: string;
}

/**
 * The actual verification logic (migrations x2, lock-row check,
 * representative task/receipt, representative NATS publish-consume
 * cycle), run against an ALREADY-CREATED database. Deliberately
 * contains NO database-creation or cleanup of its own -- callers own
 * that lifecycle (see `main` and the test file), so a failure here can
 * never accidentally skip or duplicate a cleanup step.
 */
export async function runVerification(host: string, port: number, dbName: string): Promise<VerificationOutcome> {
  const base = loadMysqlConfig();
  const freshConfig = { ...base, host, port, user: ROOT_USER, password: ROOT_PASSWORD, database: dbName };

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
  let natsStreamName: string | undefined;
  let natsNamespace: string | undefined;
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
    natsNamespace = `relaylab-freshtest-${Date.now()}-${randomBytes(4).toString("hex")}`;
    process.env.RELAYLAB_NAMESPACE = natsNamespace; // isolates subject/stream/durable names, see src/config.ts
    natsStreamName = eventsStream();
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

  return { dbName, natsStreamName, natsNamespace };
}

/**
 * Deletes ONLY the NATS stream this run itself created (by exact name),
 * never any other stream. Tolerates the stream already being gone.
 */
export async function cleanupNatsStream(natsStreamName: string, natsNamespace: string): Promise<void> {
  const { nc, jsm } = await connectNats(loadNatsConfig().servers);
  try {
    try {
      await jsm.streams.delete(natsStreamName);
      log(`deleted this run's own NATS stream "${natsStreamName}" (namespace ${natsNamespace}) -- no other stream was touched.`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (!/not found|no message found|404/i.test(message)) throw err;
      log(`this run's NATS stream "${natsStreamName}" was already gone -- nothing to clean up.`);
    }
  } finally {
    await nc.drain();
  }
}

async function main() {
  const base = loadMysqlConfig();
  log(`host: ${base.host}:${base.port} (validated as loopback before any administrative DDL)`);

  const { created, dbName } = await createFreshDatabase(base.host, base.port);
  log(`created a fresh, empty, uniquely-named database "${dbName}" (prefix-validated, never preceded by a DROP)`);

  // Collected across the whole run so a cleanup-step failure is NEVER
  // allowed to mask (or be masked by) the original verification result
  // -- both are reported explicitly, see the end of this function.
  let mainError: unknown;
  let outcome: VerificationOutcome | undefined;
  try {
    outcome = await runVerification(base.host, base.port, dbName);
    log("ALL CHECKS PASSED on the fresh schema.");
  } catch (err) {
    mainError = err;
  }

  const cleanupErrors: string[] = [];

  if (outcome?.natsStreamName && outcome.natsNamespace) {
    try {
      await cleanupNatsStream(outcome.natsStreamName, outcome.natsNamespace);
    } catch (err) {
      cleanupErrors.push(`NATS stream cleanup failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  try {
    // `created` is the literal boolean createFreshDatabase returned --
    // never re-derived or assumed. A failed create (created: false)
    // would never reach here with `created: true`.
    await dropDatabaseIfOwned(base.host, base.port, dbName, created);
  } catch (err) {
    cleanupErrors.push(`database cleanup failed for "${dbName}": ${err instanceof Error ? err.message : String(err)}`);
  }

  if (cleanupErrors.length > 0) {
    console.error(`[fresh-schema] CLEANUP ISSUE(S) (reported, never silently swallowed): ${cleanupErrors.join(" | ")}`);
  }

  if (mainError) {
    console.error("[fresh-schema] FAILED (the original verification error -- not masked by any cleanup result above):", mainError);
    process.exitCode = 1;
    return;
  }
  if (cleanupErrors.length > 0) {
    // Verification itself passed, but at least one cleanup step did not
    // -- still a real failure to surface, just a distinct one from a
    // verification failure.
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("[fresh-schema] FAILED (unexpected, outside the normal verification/cleanup flow):", err);
  process.exitCode = 1;
});
