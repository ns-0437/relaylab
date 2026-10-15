import mysql from "mysql2/promise";
import type { MysqlConfig } from "../config.js";

export function createPool(config: MysqlConfig): mysql.Pool {
  return mysql.createPool({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    waitForConnections: true,
    connectionLimit: 10,
    namedPlaceholders: false,
    decimalNumbers: true,
    dateStrings: false,
  });
}

/**
 * Runs `work` inside a transaction on a single connection from the pool.
 * Always releases the connection. Rolls back on any thrown error.
 */
export async function withTransaction<T>(
  pool: mysql.Pool,
  work: (conn: mysql.PoolConnection) => Promise<T>,
): Promise<T> {
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    try {
      const result = await work(conn);
      await conn.commit();
      return result;
    } catch (err) {
      await conn.rollback();
      throw err;
    }
  } finally {
    conn.release();
  }
}

/** MySQL error codes worth a bounded retry (deadlock / lock wait timeout). */
export function isRetryableDbError(err: unknown): boolean {
  const code = (err as { code?: string } | undefined)?.code;
  return code === "ER_LOCK_DEADLOCK" || code === "ER_LOCK_WAIT_TIMEOUT";
}

export function isDuplicateKeyError(err: unknown): boolean {
  return (err as { code?: string } | undefined)?.code === "ER_DUP_ENTRY";
}
