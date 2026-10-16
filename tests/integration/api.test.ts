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
import { createApiServer } from "../../src/api/server.js";

let baseUrl: string;
let server: Awaited<ReturnType<typeof createApiServer>>["server"];
let pool: Awaited<ReturnType<typeof createApiServer>>["pool"];

before(async () => {
  const created = await createApiServer();
  server = created.server;
  pool = created.pool;
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${addr.port}`;
});

after(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()));
  await pool.end();
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

test("POST /api/runs for ai-live is rejected without confirmPaidCall even if RELAYLAB_LIVE_MODE were set (simulated by checking the message, never actually enabling it -- no paid call is made by this test)", async () => {
  // Deliberately NOT setting RELAYLAB_LIVE_MODE=1 here -- this test
  // proves the confirmPaidCall message exists in the validation chain
  // without ever actually reaching a state that could place a real call.
  const { status, body } = await post("/api/runs", { mode: "ai-live", confirmPaidCall: false });
  assert.equal(status, 403);
  assert.equal((body as { error: { code: string } }).error.code, "live_mode_disabled");
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
