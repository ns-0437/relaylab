// Minimal ordered migration ledger. NOT a generic migration framework --
// just enough to (a) apply infra/migrations/*.sql in order exactly once,
// (b) detect if a previously-applied file's content has silently changed
// underneath us (checksum mismatch => fail loudly, do not guess), and
// (c) refuse to run against a database that doesn't look like a RelayLab
// dev database, so a misconfigured env var can't point this at someone
// else's schema.
//
// Down-migrations are intentionally not provided. Phase 1's recovery
// approach for an unwanted migration is: restore infra/compose.yaml's
// mysql volume from a snapshot, or drop and recreate the dev database --
// there is no production data to preserve yet. This is a deliberate scope
// decision, not an oversight; revisit before any non-disposable database
// depends on this schema.
import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import mysql from "mysql2/promise";
import { loadMysqlConfig } from "../config.js";

const LEDGER_TABLE = `
  CREATE TABLE IF NOT EXISTS schema_migrations (
    id          VARCHAR(191) NOT NULL PRIMARY KEY,
    checksum    CHAR(64)     NOT NULL,
    applied_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
  ) ENGINE=InnoDB
`;

function checksum(sql: string): string {
  return createHash("sha256").update(sql).digest("hex");
}

interface MigrationFile {
  id: string;
  path: string;
  sql: string;
  checksum: string;
}

function loadMigrationFiles(): MigrationFile[] {
  const dir = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "infra", "migrations");
  const files = readdirSync(dir)
    .filter((f) => f.endsWith(".sql"))
    .sort(); // filenames are zero-padded numeric prefixes (0001_, 0002_, ...)
  return files.map((f) => {
    const full = path.join(dir, f);
    const sql = readFileSync(full, "utf8");
    return { id: f, path: full, sql, checksum: checksum(sql) };
  });
}

/**
 * Refuses to proceed unless the target looks like a disposable RelayLab
 * dev/test database: database name must start with "relaylab" AND host
 * must be loopback, UNLESS RELAYLAB_ALLOW_UNSAFE_MIGRATE=1 is explicitly
 * set. This is a guardrail against a copy-pasted env var pointing migrate
 * at an unintended database, not a real multi-environment safety system.
 */
function assertSafeTarget(config: ReturnType<typeof loadMysqlConfig>): void {
  if (process.env.RELAYLAB_ALLOW_UNSAFE_MIGRATE === "1") return;
  const isLoopbackHost = ["127.0.0.1", "localhost", "::1"].includes(config.host);
  const looksLikeRelaylabDb = config.database.toLowerCase().startsWith("relaylab");
  if (!isLoopbackHost || !looksLikeRelaylabDb) {
    throw new Error(
      `Refusing to run migrations against ${config.host}:${config.port}/${config.database} -- ` +
        `it does not look like a local RelayLab dev database (host must be loopback, database name ` +
        `must start with "relaylab"). If this is intentional, set RELAYLAB_ALLOW_UNSAFE_MIGRATE=1.`,
    );
  }
}

export interface MigrateResult {
  appliedCount: number;
  totalCount: number;
}

/**
 * Applies infra/migrations/*.sql in order, exactly once each, tracked in
 * schema_migrations. Shared by the CLI entrypoint below and by
 * tests/integration/helpers.ts, so tests exercise the exact same migration
 * path as `npm run migrate` rather than a parallel ad hoc schema loader.
 */
export async function applyMigrations(config: ReturnType<typeof loadMysqlConfig>): Promise<MigrateResult> {
  assertSafeTarget(config);

  const connection = await mysql.createConnection({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    multipleStatements: true,
  });

  try {
    console.log(`Migrating ${config.host}:${config.port}/${config.database}`);
    await connection.query(LEDGER_TABLE);

    const [appliedRows] = await connection.query<(mysql.RowDataPacket & { id: string; checksum: string })[]>(
      `SELECT id, checksum FROM schema_migrations`,
    );
    const applied = new Map(appliedRows.map((r) => [r.id, r.checksum]));

    const files = loadMigrationFiles();
    if (files.length === 0) throw new Error("no migration files found in infra/migrations/");

    let appliedCount = 0;
    for (const file of files) {
      const priorChecksum = applied.get(file.id);
      if (priorChecksum !== undefined) {
        if (priorChecksum !== file.checksum) {
          throw new Error(
            `Migration ${file.id} was already applied with checksum ${priorChecksum} but the file on ` +
              `disk now has checksum ${file.checksum}. A previously-applied migration must never be ` +
              `edited -- add a new migration file instead. Refusing to continue (no further migrations ` +
              `were applied this run).`,
          );
        }
        console.log(`  [skip]  ${file.id} (already applied, checksum matches)`);
        continue;
      }

      console.log(`  [apply] ${file.id}`);
      try {
        // NOTE: MySQL DDL statements each implicitly commit as they run,
        // so this is NOT atomic the way a data-only transaction would be
        // -- a file with multiple DDL statements that fails partway
        // through can leave some of its statements applied. That partial
        // state is exactly the FAILURE case being guarded against below:
        // we do NOT record the migration as applied if any statement in
        // it throws, so the next run will retry the whole file and
        // surface the same error rather than silently skipping it, and
        // the error names the exact file so the partial state can be
        // inspected by hand. A fully transactional migration runner would
        // need to avoid DDL or use a database that supports transactional
        // DDL; out of scope for Phase 1.
        await connection.query(file.sql);
      } catch (err) {
        throw new Error(
          `Migration ${file.id} failed partway through: ${err instanceof Error ? err.message : err}. ` +
            `It may be PARTIALLY APPLIED -- inspect the database by hand before retrying. This migration ` +
            `was NOT recorded as applied, so the next run will attempt it again.`,
        );
      }
      await connection.query(`INSERT INTO schema_migrations (id, checksum) VALUES (?, ?)`, [file.id, file.checksum]);
      appliedCount++;
    }

    console.log(
      appliedCount > 0
        ? `Migration complete: ${appliedCount} new migration(s) applied, ${files.length - appliedCount} already up to date.`
        : `Migration complete: nothing to do, all ${files.length} migration(s) already applied.`,
    );
    return { appliedCount, totalCount: files.length };
  } finally {
    await connection.end();
  }
}

