// Demo UI milestone, task 1: proves startRun's reservation uses ONE
// stable lifecycle promise -- awaitActiveRun (graceful shutdown's own
// wait) genuinely cannot resolve before that reservation is fully
// settled, even across the pending-insertion window, and a run
// cancelled WHILE its own insertion is still pending never proceeds
// into normal execution. Uses a JS-level gate on the pool's own
// insert call (never LOCK TABLES -- a previous attempt at a
// DB-level concurrency barrier in this project hung the test runner;
// see api.test.ts's history) so this stays fast and deterministic.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import { createExecutor } from "../../src/api/executor.js";
import { applySchema, requireMysqlPool, truncateAll } from "./helpers.js";

let pool: mysql.Pool;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
});

beforeEach(async () => {
  await truncateAll(pool);
});

after(async () => {
  await pool.end();
});

/** Wraps a real pool so the SPECIFIC `INSERT INTO demo_runs` call awaits `gate` before actually running -- every other query passes straight through, unmodified, to the real pool. */
function gateDemoRunsInsert(realPool: mysql.Pool, gate: Promise<void>): mysql.Pool {
  const realQuery = realPool.query.bind(realPool);
  return {
    ...realPool,
    query: (async (sql: unknown, params?: unknown) => {
      if (typeof sql === "string" && sql.includes("INSERT INTO demo_runs")) {
        await gate;
      }
      return (realQuery as (sql: unknown, params?: unknown) => Promise<unknown>)(sql, params);
    }) as typeof realPool.query,
  } as mysql.Pool;
}

/** Races `p` against a short timer; returns "resolved" or "still-pending" without ever leaving a dangling timer (the loser is just garbage, nothing to clean up). */
async function settledWithin<T>(p: Promise<T>, ms: number): Promise<"resolved" | "still-pending"> {
  const sentinel = Symbol("timeout");
  const result = await Promise.race([p.then(() => "resolved" as const), new Promise<typeof sentinel>((resolve) => setTimeout(() => resolve(sentinel), ms))]);
  return result === sentinel ? "still-pending" : "resolved";
}

test("awaitActiveRun cannot resolve while this run's own insertion is still pending, and a cancel requested during that window skips normal execution entirely", async () => {
  let releaseInsert: () => void;
  const gate = new Promise<void>((resolve) => (releaseInsert = resolve));
  const gatedPool = gateDemoRunsInsert(pool, gate);
  const executor = createExecutor();

  const startPromise = executor.startRun(gatedPool, "deterministic", false);

  // The reservation is set SYNCHRONOUSLY (before startRun's first
  // await), so activeRunId() is already populated even though
  // insertRun itself is still gated/pending.
  const runId = executor.activeRunId();
  assert.ok(runId, "the reservation must be visible immediately, before the gated insertion has resolved");

  // Cancel WHILE insertion is still pending -- a legitimate race this
  // task requires handling: the reservation (and therefore cancelRun's
  // target) exists before insertRun has resolved.
  executor.cancelRun(runId!);

  // awaitActiveRun must NOT resolve yet: the ONE lifecycle promise it
  // reads is still covering the (still-gated) insertion phase. Proven
  // by racing it against a short timer, not by assuming via a sleep
  // that unrelated work is "probably still busy".
  const awaitPromise = executor.awaitActiveRun();
  assert.equal(await settledWithin(awaitPromise, 300), "still-pending", "awaitActiveRun must still be waiting while the reservation's insertion has not yet settled");

  // Release the gate -- insertion now actually runs.
  releaseInsert!();

  const { runId: returnedRunId } = await startPromise;
  assert.equal(returnedRunId, runId);

  // NOW both must resolve promptly: the SAME lifecycle promise settles
  // once startRun observes the pending cancellation right after
  // insertion succeeds, skipping normal execution entirely.
  assert.equal(await settledWithin(awaitPromise, 5_000), "resolved", "awaitActiveRun must resolve once the reservation is genuinely fully settled");
  assert.equal(executor.activeRunId(), null, "the active slot must be cleared once settled");

  const [rows] = await pool.query(`SELECT status, detail_json FROM demo_runs WHERE run_id = ?`, [runId]);
  const row = (rows as { status: string; detail_json: string | object }[])[0];
  assert.ok(row, "the run row must exist (insertion did complete, once released)");
  assert.equal(row.status, "cancelled", "a cancel observed right after insertion must be recorded directly as cancelled -- never 'running', which would imply normal execution started");
  const detail = typeof row.detail_json === "string" ? JSON.parse(row.detail_json) : row.detail_json;
  assert.equal(detail.stage, "generation", "cancelled-during-insertion never reached any later stage, since normal execution never started");
});
