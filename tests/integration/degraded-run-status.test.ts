// Demo UI milestone, task 3: proves a terminal persistence failure
// (the FINAL status write also fails, after an earlier write already
// failed) surfaces as a sanitized degraded note via GET /api/runs/:id,
// overlaid on the last durably-recorded (stale) row -- never silently
// shown as healthy. A SEPARATE file (own process) so this instance's
// own admission is never pre-blocked by another test file's shutdown().
process.env.RELAYLAB_MYSQL_DATABASE = process.env.RELAYLAB_TEST_MYSQL_DATABASE ?? "relaylab_test";
process.env.RELAYLAB_API_PORT = "0";

import { test, after } from "node:test";
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { createApiServer, type ApiServerHandle } from "../../src/api/server.js";
import { degradedRunNote } from "../../src/api/executor.js";
import { createPool } from "../../src/db/pool.js";
import { loadMysqlConfig } from "../../src/config.js";

let handle: ApiServerHandle;
let baseUrl: string;

function postTo(path: string, body: unknown) {
  return fetch(`${baseUrl}${path}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }).then(async (res) => ({ status: res.status, body: await res.json() }));
}
function getFrom(path: string) {
  return fetch(`${baseUrl}${path}`).then(async (res) => ({ status: res.status, body: await res.json() }));
}

after(async () => {
  await handle?.shutdown();
});

test("a terminal persistence failure surfaces as a sanitized degraded note via GET /api/runs/:id, overlaid on the last durably-recorded (stale) row, and never silently shown as healthy", async () => {
  handle = await createApiServer();
  await new Promise<void>((resolve) => handle.server.listen(0, "127.0.0.1", resolve));
  const addr = handle.server.address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${addr.port}`;

  const started = await postTo("/api/runs", { mode: "deterministic" });
  assert.equal(started.status, 201, `expected to start cleanly, got ${started.status}: ${JSON.stringify(started.body)}`);
  const runId = (started.body as { runId: string }).runId;

  // Bounded poll for the first durable confirmation (status "running")
  // before injecting the fault, so there is a known-good stale row to
  // overlay the degraded note onto.
  const adminPool = createPool(loadMysqlConfig());
  let sawRunning = false;
  for (let i = 0; i < 40; i++) {
    const [rows] = await adminPool.query(`SELECT status FROM demo_runs WHERE run_id = ?`, [runId]);
    const status = (rows as { status: string }[])[0]?.status;
    if (status === "running") {
      sawRunning = true;
      break;
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  assert.ok(sawRunning, "the run must reach a durably-recorded 'running' status before the fault is injected");

  // Rename demo_runs away WHILE the run is still in flight -- the next
  // write it attempts (a stage transition, or the terminal write) will
  // throw, propagating to executeRun's outer catch, whose OWN attempt
  // to persist the resulting terminal status then ALSO fails (table
  // still renamed) -- this is the genuine double-failure path that
  // produces a degraded note, never a mocked one.
  await adminPool.query(`RENAME TABLE demo_runs TO demo_runs_disabled_for_degraded_test`);
  try {
    await postTo(`/api/runs/${runId}/cancel`, {});
    let note: string | undefined;
    for (let i = 0; i < 300; i++) {
      note = degradedRunNote(runId);
      if (note) break;
      await new Promise((r) => setTimeout(r, 200));
    }
    assert.ok(note, "a degraded note must be recorded once even the terminal status write fails");
    assert.doesNotMatch(note!, /demo_runs_disabled_for_degraded_test/, "the degraded note must be sanitized, never leaking the raw table/error detail");
  } finally {
    await adminPool.query(`RENAME TABLE demo_runs_disabled_for_degraded_test TO demo_runs`);
    await adminPool.end();
  }

  // Now that storage has recovered, GET must succeed and overlay the
  // degraded note alongside the (stale, pre-fault) durable row --
  // never silently presenting the run as healthy/current.
  let detail: { status: number; body: unknown } | undefined;
  for (let i = 0; i < 20; i++) {
    detail = await getFrom(`/api/runs/${runId}`);
    if (detail.status === 200 && (detail.body as { degraded?: string }).degraded) break;
    await new Promise((r) => setTimeout(r, 200));
  }
  assert.equal(detail!.status, 200);
  const body = detail!.body as { run: { status: string }; degraded?: string };
  assert.ok(body.degraded, "GET /api/runs/:id must surface the degraded note once present");
  assert.notEqual(body.run.status, "completed", "the overlaid row must be the last genuinely-durable (stale) status, never fabricated as a healthy completion");
  const list = await getFrom("/api/runs");
  const row = (list.body as { runs: { runId: string; status: string; detail: { error?: string } }[] }).runs.find(r => r.runId === runId);
  assert.equal(row?.status, "interrupted");
  assert.match(row?.detail.error ?? "", /could not be durably recorded/);
});
