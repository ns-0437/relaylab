// Demo UI milestone: focused tests for the local HTTP API. Uses the
// SAME disposable relaylab_test database every other integration test
// targets -- never the dev database the manually-run demo UI uses, so
// this suite never truncates/disturbs a demo in progress. Starts its
// own API server instance on an ephemeral port; never the port a human
// might be using for the demo (RELAYLAB_API_PORT 0 lets the OS assign
// one). No real Anthropic calls anywhere in this file.
process.env.RELAYLAB_MYSQL_DATABASE = process.env.RELAYLAB_TEST_MYSQL_DATABASE ?? "relaylab_test";
process.env.RELAYLAB_API_PORT = "0";

import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { createApiServer, type ApiServerHandle } from "../../src/api/server.js";

let handle: ApiServerHandle;
let baseUrl: string;

before(async () => {
  handle = await createApiServer();
  await new Promise<void>((resolve) => handle.server.listen(0, "127.0.0.1", resolve));
  const addr = handle.server.address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${addr.port}`;
});

after(async () => {
  await handle.shutdown();
});

async function post(path: string, body: unknown, headers: Record<string, string> = {}) {
  const res = await fetch(`${baseUrl}${path}`, { method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify(body) });
  return { status: res.status, body: await res.json() };
}
async function get(path: string) {
  const res = await fetch(`${baseUrl}${path}`);
  return { status: res.status, body: await res.json() };
}

/** Bounded poll (not indefinite) for the executor's in-memory "one active run" slot to clear after a cancel. */
async function waitForNoActiveRun(): Promise<void> {
  for (let i = 0; i < 20; i++) {
    const { body } = await get("/api/runs");
    if ((body as { activeRunId: string | null }).activeRunId === null) return;
    await new Promise((r) => setTimeout(r, 300));
  }
  throw new Error("timed out waiting for the active run slot to clear");
}

test("POST /api/runs rejects an invalid mode", async () => {
  const { status, body } = await post("/api/runs", { mode: "bogus" });
  assert.equal(status, 400);
  assert.equal((body as { error: { code: string } }).error.code, "invalid_mode");
});

test("POST /api/runs rejects a non-JSON content type for a mutation", async () => {
  const res = await fetch(`${baseUrl}/api/runs`, { method: "POST", headers: { "content-type": "text/plain" }, body: "mode=deterministic" });
  assert.equal(res.status, 415);
});

test("POST /api/runs rejects a request from an untrusted browser Origin", async () => {
  const { status, body } = await post("/api/runs", { mode: "deterministic" }, { origin: "http://evil.example" });
  assert.equal(status, 403);
  assert.equal((body as { error: { code: string } }).error.code, "untrusted_origin");
});

test("POST /api/runs for ai-live is rejected when the server has not enabled live mode", async () => {
  const { status, body } = await post("/api/runs", { mode: "ai-live", confirmPaidCall: true });
  assert.equal(status, 403);
  assert.equal((body as { error: { code: string } }).error.code, "live_mode_disabled");
  assert.match((body as { error: { message: string } }).error.message, /RELAYLAB_LIVE_MODE/);
});

test("POST /api/runs for ai-live, with the server gate ACTUALLY ENABLED (isolated to this one test) but confirmPaidCall omitted, is refused -- zero provider calls, zero run rows created", async () => {
  // The server-side RELAYLAB_LIVE_MODE gate is read fresh on every call
  // (see executor.ts's startRun), so toggling it for just this one
  // request -- against the SAME already-running server instance, never
  // a second one (the singleton lock only allows one) -- genuinely
  // exercises the "gate enabled" branch without ever reaching the
  // provider-client construction that only happens once confirmPaidCall
  // has ALSO passed. No real credentials are configured in this test
  // environment either way, so even a code-path regression here could
  // not reach a real network call.
  const before = await get("/api/runs");
  const countBefore = (before.body as { runs: unknown[] }).runs.length;

  const saved = process.env.RELAYLAB_LIVE_MODE;
  process.env.RELAYLAB_LIVE_MODE = "1";
  let status: number, body: unknown;
  try {
    ({ status, body } = await post("/api/runs", { mode: "ai-live" })); // confirmPaidCall omitted entirely
  } finally {
    if (saved === undefined) delete process.env.RELAYLAB_LIVE_MODE;
    else process.env.RELAYLAB_LIVE_MODE = saved;
  }

  assert.equal(status, 403);
  assert.equal((body as { error: { code: string } }).error.code, "live_mode_disabled");
  assert.match((body as { error: { message: string } }).error.message, /confirmPaidCall/);

  const after = await get("/api/runs");
  const countAfter = (after.body as { runs: unknown[] }).runs.length;
  assert.equal(countAfter, countBefore, "a refused live-mode start must never insert a run row -- confirms execution (and any provider client construction) was never reached");
});

test("overlapping starts: a second POST /api/runs while one is active is rejected with 409, and cancelling it lets a new one start", async () => {
  const first = await post("/api/runs", { mode: "ai-simulated" });
  assert.equal(first.status, 201);
  const runId = (first.body as { runId: string }).runId;

  const second = await post("/api/runs", { mode: "ai-simulated" });
  assert.equal(second.status, 409);
  assert.equal((second.body as { error: { code: string } }).error.code, "run_conflict");

  const cancel = await post(`/api/runs/${runId}/cancel`, {});
  assert.equal(cancel.status, 202);
  await waitForNoActiveRun();

  const third = await post("/api/runs", { mode: "ai-simulated" });
  assert.equal(third.status, 201, "a new run must be startable once the previous one is no longer active");
  await post(`/api/runs/${(third.body as { runId: string }).runId}/cancel`, {});
  await waitForNoActiveRun();
});

test("GET /api/runs/:id returns 404 for an unknown run id, and 200 with run detail for a real one", async () => {
  const missing = await get("/api/runs/does-not-exist");
  assert.equal(missing.status, 404);

  const started = await post("/api/runs", { mode: "ai-simulated" });
  assert.equal(started.status, 201, `expected to start cleanly, got ${started.status}: ${JSON.stringify(started.body)}`);
  const runId = (started.body as { runId: string }).runId;
  const detail = await get(`/api/runs/${runId}`);
  assert.equal(detail.status, 200);
  assert.equal((detail.body as { run: { runId: string } }).run.runId, runId);
  await post(`/api/runs/${runId}/cancel`, {});
  await waitForNoActiveRun();
});

test("POST /api/runs/:id/cancel applies the same JSON-mutation validation as POST /api/runs (rejects a non-JSON body)", async () => {
  const started = await post("/api/runs", { mode: "ai-simulated" });
  assert.equal(started.status, 201);
  const runId = (started.body as { runId: string }).runId;

  const res = await fetch(`${baseUrl}/api/runs/${runId}/cancel`, { method: "POST", headers: { "content-type": "text/plain" }, body: "not json" });
  assert.equal(res.status, 415, "cancel must validate Content-Type exactly like POST /api/runs does, not accept an arbitrary body");

  // Clean up with a VALID cancel so this test doesn't leave an active run behind.
  await post(`/api/runs/${runId}/cancel`, {});
  await waitForNoActiveRun();
});

test("singleton lock: a second server instance against the SAME database cannot start while the first genuinely holds an active run", async () => {
  const started = await post("/api/runs", { mode: "ai-simulated" });
  assert.equal(started.status, 201);
  const runId = (started.body as { runId: string }).runId;

  await assert.rejects(
    () => createApiServer(),
    /already holds the singleton lock/,
    "a second instance must refuse to start at all while the first instance's lock is still valid -- it must never reconcile or otherwise touch the first instance's active run",
  );

  // Confirm the first instance's run is COMPLETELY UNTOUCHED by the refused second instance's attempt.
  const stillThere = await get(`/api/runs/${runId}`);
  assert.equal(stillThere.status, 200);
  assert.notEqual((stillThere.body as { run: { status: string } }).run.status, "interrupted", "the second instance's refused startup must never have reconciled (interrupted) the first instance's genuinely active run");

  await post(`/api/runs/${runId}/cancel`, {});
  await waitForNoActiveRun();
});

test("failed run-lifecycle insertion releases its reservation -- a subsequent start still succeeds", async () => {
  // Controlled fault injection: the demo_runs table is temporarily
  // renamed out from under the running server, so the VERY FIRST
  // durable write startRun performs (insertRun) fails -- never a real
  // production code path, and restored in a `finally` regardless of
  // test outcome.
  const { createPool } = await import("../../src/db/pool.js");
  const { loadMysqlConfig } = await import("../../src/config.js");
  const adminPool = createPool(loadMysqlConfig());
  try {
    await adminPool.query(`RENAME TABLE demo_runs TO demo_runs_disabled_for_test`);
    try {
      const { status, body } = await post("/api/runs", { mode: "deterministic" });
      assert.equal(status, 500);
      const errorBody = body as { error: { code: string; message: string } };
      assert.equal(errorBody.error.code, "storage_error", "a failed lifecycle write must be reported as a sanitized, actionable storage_error code, not an arbitrary message");
      assert.doesNotMatch(errorBody.error.message, /demo_runs_disabled_for_test/, "the sanitized message must not leak the raw table/error detail");
    } finally {
      await adminPool.query(`RENAME TABLE demo_runs_disabled_for_test TO demo_runs`);
    }
  } finally {
    await adminPool.end();
  }

  // The reservation must have been released by the failed attempt --
  // confirmed by a genuinely NEW start succeeding immediately after.
  const recovered = await post("/api/runs", { mode: "ai-simulated" });
  assert.equal(recovered.status, 201, "a new run must start successfully once the table is restored -- the failed insertion must never have left the active slot permanently occupied");
  await post(`/api/runs/${(recovered.body as { runId: string }).runId}/cancel`, {});
  await waitForNoActiveRun();
});
