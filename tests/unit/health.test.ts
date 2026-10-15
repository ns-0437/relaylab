// Phase 3A.1: focused tests for src/telemetry/health.ts -- startup,
// healthy state, dependency failure, recovery, and shutdown. No
// infrastructure required (no MySQL/NATS/Docker): `startDependencyPing`
// takes a caller-supplied `ping` function, so it is tested here with
// plain in-memory stand-ins, never a real database.
import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { startHealthServer, startDependencyPing } from "../../src/telemetry/health.js";

function get(port: number, path: string): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: "127.0.0.1", port, path }, (res) => {
      let body = "";
      res.on("data", (c) => (body += c));
      res.on("end", () => resolve({ status: res.statusCode ?? 0, body }));
    });
    req.on("error", reject);
  });
}

test("startup: /healthz/startup is 503 before markStarted(), 200 after; live/ready are independent of it", async () => {
  const port = 19481;
  let ready = true;
  const health = startHealthServer(port, { isReady: () => ready });
  try {
    const before = await get(port, "/healthz/startup");
    assert.equal(before.status, 503);
    health.markStarted();
    const after = await get(port, "/healthz/startup");
    assert.equal(after.status, 200);
    // live/ready never depended on startup in the first place
    assert.equal((await get(port, "/healthz/live")).status, 200);
    assert.equal((await get(port, "/healthz/ready")).status, 200);
  } finally {
    await health.stop();
  }
});

test("healthy state: live is 200 and ready is 200 while isReady() returns true", async () => {
  const port = 19482;
  const health = startHealthServer(port, { isReady: () => true });
  try {
    assert.equal((await get(port, "/healthz/live")).status, 200);
    assert.equal((await get(port, "/healthz/ready")).status, 200);
  } finally {
    await health.stop();
  }
});

test("dependency failure: ready goes 503 when isReady() is false, but live STAYS 200 (never inspects the dependency)", async () => {
  const port = 19483;
  let ready = true;
  const health = startHealthServer(port, { isReady: () => ready });
  try {
    ready = false;
    const r = await get(port, "/healthz/ready");
    assert.equal(r.status, 503);
    assert.match(r.body, /not ready/);
    const live = await get(port, "/healthz/live");
    assert.equal(live.status, 200, "a dependency outage must never affect liveness");
  } finally {
    await health.stop();
  }
});

test("recovery: ready returns to 200 once isReady() flips back to true, with no restart/reconnect of the health server itself", async () => {
  const port = 19484;
  let ready = false;
  const health = startHealthServer(port, { isReady: () => ready });
  try {
    assert.equal((await get(port, "/healthz/ready")).status, 503);
    ready = true;
    assert.equal((await get(port, "/healthz/ready")).status, 200);
  } finally {
    await health.stop();
  }
});

test("shutdown: after health.stop(), the server no longer accepts connections", async () => {
  const port = 19485;
  const health = startHealthServer(port, { isReady: () => true });
  assert.equal((await get(port, "/healthz/live")).status, 200);
  await health.stop();
  await assert.rejects(() => get(port, "/healthz/live"));
});

test("startDependencyPing: isUp() is false until the first successful ping resolves", async () => {
  let shouldSucceed = false;
  const dep = startDependencyPing(async () => {
    if (!shouldSucceed) throw new Error("not up yet");
  }, 20, 200);
  try {
    assert.equal(dep.isUp(), false);
    shouldSucceed = true;
    await new Promise((r) => setTimeout(r, 100));
    assert.equal(dep.isUp(), true);
  } finally {
    dep.stop();
  }
});

test("startDependencyPing: a ping that never resolves is treated as down once timeoutMs elapses (bounded, not hung forever)", async () => {
  const dep = startDependencyPing(() => new Promise(() => {}), 500, 50);
  try {
    await new Promise((r) => setTimeout(r, 150));
    assert.equal(dep.isUp(), false);
  } finally {
    dep.stop();
  }
});

test("startDependencyPing: recovers to up after a failing ping starts succeeding again", async () => {
  let fail = true;
  const dep = startDependencyPing(async () => {
    if (fail) throw new Error("down");
  }, 20, 200);
  try {
    await new Promise((r) => setTimeout(r, 60));
    assert.equal(dep.isUp(), false);
    fail = false;
    await new Promise((r) => setTimeout(r, 60));
    assert.equal(dep.isUp(), true);
  } finally {
    dep.stop();
  }
});

test("startDependencyPing: stop() halts further ticking (isUp() freezes at its last value)", async () => {
  let callCount = 0;
  const dep = startDependencyPing(async () => {
    callCount++;
  }, 15, 200);
  await new Promise((r) => setTimeout(r, 60));
  dep.stop();
  const countAtStop = callCount;
  await new Promise((r) => setTimeout(r, 60));
  assert.equal(callCount, countAtStop, "no further ping attempts after stop()");
});
