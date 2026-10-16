import { test } from "node:test";
import assert from "node:assert/strict";
import type mysql from "mysql2/promise";
import { startRun, activeRunId, cancelRun, awaitActiveRun } from "../../src/api/executor.js";

test("pending insertion reserves admission; shutdown awaits the same lifecycle through cancellation persistence", async () => {
  let resolveInsert!: (value: unknown) => void;
  let resolveTerminal!: (value: unknown) => void;
  let terminalStarted!: () => void;
  const terminalReached = new Promise<void>((resolve) => { terminalStarted = resolve; });
  const calls: string[] = [];
  const pool = { query: (sql: string) => {
    calls.push(sql);
    if (sql.startsWith("INSERT")) return new Promise((resolve) => { resolveInsert = resolve; });
    if (sql.startsWith("SELECT")) return Promise.resolve([[{ detail_json: { stage: "generation" } }]]);
    terminalStarted();
    return new Promise((resolve) => { resolveTerminal = resolve; });
  } } as unknown as mysql.Pool;
  const first = startRun(pool, "deterministic", false);
  await assert.rejects(startRun(pool, "deterministic", false), /already active/);
  assert.equal(calls.length, 1);
  cancelRun(activeRunId()!);
  let finished = false;
  const waiting = awaitActiveRun().then(() => { finished = true; });
  resolveInsert([{}]);
  await first;
  await terminalReached;
  assert.equal(finished, false, "insertion resolution must not release shutdown before terminal persistence");
  resolveTerminal([{}]);
  await waiting;
  assert.equal(activeRunId(), null);
});

test("failed insertion frees only its reservation and permits the next start", async () => {
  let inserts = 0;
  const pool = { query: async () => { inserts++; throw Object.assign(new Error("injected"), { code: "ER_TEST" }); } } as unknown as mysql.Pool;
  await assert.rejects(startRun(pool, "deterministic", false), /database error/);
  await assert.rejects(startRun(pool, "deterministic", false), /database error/);
  assert.equal(inserts, 2);
  assert.equal(activeRunId(), null);
});
