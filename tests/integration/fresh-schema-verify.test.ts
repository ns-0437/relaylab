// Phase 3A.2 corrective patch: focused tests for the non-destructive
// database lifecycle in fresh-schema-verify.ts. Every scenario here
// uses its OWN isolated fixture database (never relaylab/relaylab_test,
// never a database this test did not itself create) -- see each test's
// own setup/teardown.
import { test, after } from "node:test";
import assert from "node:assert/strict";
import mysql from "mysql2/promise";
import { randomBytes } from "node:crypto";
import { loadMysqlConfig } from "../../src/config.js";
import {
  createFreshDatabase,
  dropDatabaseIfOwned,
  assertValidIdentifier,
  assertLoopbackHost,
  runVerification,
  cleanupNatsStream,
} from "./fresh-schema-verify.js";

const ROOT_USER = "root";
const ROOT_PASSWORD = process.env.RELAYLAB_MYSQL_ROOT_PASSWORD ?? "relaylab_dev_root";
const base = loadMysqlConfig();

async function adminConn() {
  return mysql.createConnection({ host: base.host, port: base.port, user: ROOT_USER, password: ROOT_PASSWORD });
}

function sentinelName(): string {
  // A valid name under the SAME strict prefix the module itself
  // requires -- this is not "the arbitrary override" (there is none in
  // the module's own CLI/env surface); it is a test directly exercising
  // createFreshDatabase's own dbNameOverride parameter, which still has
  // to pass assertValidIdentifier like any other name.
  return `relaylab_freshtest_${Date.now()}_${randomBytes(6).toString("hex")}`;
}

// Tracks every database this test file itself creates directly (NOT
// via createFreshDatabase), so a final safety-net cleanup can remove
// them even if an assertion throws mid-test -- never relies on a
// single test's own try/finally alone.
const sentinelsToClean: string[] = [];
after(async () => {
  if (sentinelsToClean.length === 0) return;
  const admin = await adminConn();
  try {
    for (const name of sentinelsToClean) {
      await admin.query(`DROP DATABASE IF EXISTS \`${name}\``).catch(() => {});
    }
  } finally {
    await admin.end();
  }
});

test("invalid identifier is rejected before any DDL", () => {
  assert.throws(() => assertValidIdentifier("relaylab; DROP TABLE x"), /refusing to use database identifier/);
  assert.throws(() => assertValidIdentifier("not_even_the_right_prefix_12345"), /refusing to use database identifier/);
  assert.throws(() => assertValidIdentifier("relaylab_freshtest_"), /refusing to use database identifier/);
  // A syntactically valid-looking one must pass.
  assert.doesNotThrow(() => assertValidIdentifier("relaylab_freshtest_1700000000000_abcdef123456"));
});

test("unsafe (non-loopback) host is rejected before any DDL", () => {
  assert.throws(() => assertLoopbackHost("some-remote-host.example.com"), /refusing to run fresh-schema verification/);
  assert.throws(() => assertLoopbackHost("10.0.0.5"), /refusing to run fresh-schema verification/);
  assert.doesNotThrow(() => assertLoopbackHost("127.0.0.1"));
  assert.doesNotThrow(() => assertLoopbackHost("localhost"));
});

test(
  "an EXISTING database with a sentinel row survives a name collision -- createFreshDatabase refuses, " +
    "the existing database and its data are completely untouched",
  async () => {
    const name = sentinelName();
    sentinelsToClean.push(name);
    const admin = await adminConn();
    try {
      await admin.query(`CREATE DATABASE \`${name}\``);
      await admin.query(`USE \`${name}\``);
      await admin.query(`CREATE TABLE sentinel (id INT PRIMARY KEY, marker VARCHAR(64))`);
      await admin.query(`INSERT INTO sentinel (id, marker) VALUES (1, 'do-not-touch-me')`);
    } finally {
      await admin.end();
    }

    // The collision: ask createFreshDatabase to "create" the SAME name
    // that already exists and already holds real data.
    await assert.rejects(
      () => createFreshDatabase(base.host, base.port, name),
      /could not create database/,
      "CREATE DATABASE must fail outright on a name that already exists -- no IF NOT EXISTS, no preceding DROP",
    );

    // The sentinel row must still be there, byte for byte.
    const check = await adminConn();
    try {
      const [rows] = await check.query<mysql.RowDataPacket[]>(`SELECT id, marker FROM \`${name}\`.sentinel`);
      assert.deepEqual(rows, [{ id: 1, marker: "do-not-touch-me" }], "the existing database's data must be completely unaffected by the refused collision");
    } finally {
      await check.end();
    }
  },
);

test(
  "a failed create must never authorize cleanup of an existing database -- dropDatabaseIfOwned only acts " +
    "on the literal `created` boolean createFreshDatabase returned, never a re-derived guess",
  async () => {
    const name = sentinelName();
    sentinelsToClean.push(name);
    const admin = await adminConn();
    try {
      await admin.query(`CREATE DATABASE \`${name}\``);
    } finally {
      await admin.end();
    }

    let created = false;
    try {
      const result = await createFreshDatabase(base.host, base.port, name);
      created = result.created;
    } catch {
      created = false; // exactly what main() does: a throw never sets created=true
    }
    assert.equal(created, false, "a collision must report created=false");

    // Calling dropDatabaseIfOwned with the correct, honest `created=false`
    // must be a complete no-op.
    await dropDatabaseIfOwned(base.host, base.port, name, created);

    const check = await adminConn();
    try {
      const [rows] = await check.query<mysql.RowDataPacket[]>(`SHOW DATABASES LIKE ?`, [name]);
      assert.equal(rows.length, 1, "the pre-existing database must still exist after a refused-collision cleanup pass");
    } finally {
      await check.end();
    }
  },
);

test(
  "a failure AFTER successful creation still cleans up only THIS run's own database, " +
    "never a separate, unrelated database that happens to exist at the same time",
  async () => {
    // An unrelated "somebody else's" database, created independently,
    // present throughout this test to prove it is never touched.
    const unrelatedName = sentinelName();
    sentinelsToClean.push(unrelatedName);
    const admin = await adminConn();
    try {
      await admin.query(`CREATE DATABASE \`${unrelatedName}\``);
    } finally {
      await admin.end();
    }

    const { created, dbName } = await createFreshDatabase(base.host, base.port);
    assert.equal(created, true);

    // Simulate "failure after successful creation" -- the exact
    // scenario main() handles: runVerification would throw for some
    // unrelated reason, but cleanup must still run for dbName, scoped
    // to exactly that name.
    const simulatedFailure = new Error("simulated verification failure after creation");
    let mainError: unknown;
    try {
      throw simulatedFailure;
    } catch (err) {
      mainError = err;
    }
    assert.equal(mainError, simulatedFailure);

    await dropDatabaseIfOwned(base.host, base.port, dbName, created);

    const check = await adminConn();
    try {
      const [oursRows] = await check.query<mysql.RowDataPacket[]>(`SHOW DATABASES LIKE ?`, [dbName]);
      assert.equal(oursRows.length, 0, "this run's own database must be gone after cleanup");
      const [unrelatedRows] = await check.query<mysql.RowDataPacket[]>(`SHOW DATABASES LIKE ?`, [unrelatedName]);
      assert.equal(unrelatedRows.length, 1, "the UNRELATED database must survive completely untouched");
    } finally {
      await check.end();
    }
  },
);

test(
  "successful migration/re-entry verification still works end to end on a freshly created, uniquely named database",
  async () => {
    const { created, dbName } = await createFreshDatabase(base.host, base.port);
    assert.equal(created, true);
    try {
      const outcome = await runVerification(base.host, base.port, dbName);
      assert.equal(outcome.dbName, dbName);
      assert.ok(outcome.natsStreamName, "runVerification must report which NATS stream it created, for cleanup");
      if (outcome.natsStreamName && outcome.natsNamespace) {
        await cleanupNatsStream(outcome.natsStreamName, outcome.natsNamespace);
      }
    } finally {
      await dropDatabaseIfOwned(base.host, base.port, dbName, created);
    }
  },
);
