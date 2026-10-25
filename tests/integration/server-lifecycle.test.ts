// Demo UI milestone: server lifecycle acceptance -- bounded shutdown,
// startup-failure cleanup, end-to-end lock takeover, and degraded-state
// surfacing plus reconciliation. Deliberately runs SEVERAL server
// lifecycles sequentially in ONE process: executor state is per server
// (createExecutor), so one lifecycle's permanent admission block must
// never leak into the next. Every "the run is mid-flight" assumption is
// a controlled barrier (the executor's test-only beforeStage hook),
// never a sleep.
process.env.RELAYLAB_MYSQL_DATABASE = process.env.RELAYLAB_TEST_MYSQL_DATABASE ?? "relaylab_test";
delete process.env.RELAYLAB_LIVE_MODE;

import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import type mysql from "mysql2/promise";
import { createApiServer, type ApiServerHandle, type ApiServerOptions } from "../../src/api/server.js";
import type { StageHookName } from "../../src/api/executor.js";
import { DEMO_API_LOCK_NAME } from "../../src/api/singleton-lock.js";
import { tryAcquireOrRenewExclusiveLock } from "../../src/messaging/publisher-lock.js";
import { createPool } from "../../src/db/pool.js";
import { loadMysqlConfig } from "../../src/config.js";

let admin: mysql.Pool;

before(() => {
  admin = createPool(loadMysqlConfig());
});
after(async () => {
  await admin.end();
});

/** A one-shot barrier: `entered` resolves when the run reaches the stage; the run then waits until `release()`. */
function stageBarrier(stage: StageHookName) {
  let markEntered!: (runId: string) => void;
  let release!: () => void;
  const entered = new Promise<string>((r) => (markEntered = r));
  const gate = new Promise<void>((r) => (release = r));
  const beforeStage = async (s: StageHookName, runId: string) => {
    if (s !== stage) return;
    markEntered(runId);
    await gate;
  };
  return { entered, release, beforeStage };
}

async function startServer(options: ApiServerOptions = {}) {
  const handle = await createApiServer({ forbidLiveMode: true, ...options });
  await new Promise<void>((resolve) => handle.server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${(handle.server.address() as AddressInfo).port}`;
  const post = (p: string, body: unknown) =>
    fetch(base + p, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }).then(async (r) => ({ status: r.status, body: (await r.json()) as any }));
  const get = (p: string) => fetch(base + p).then(async (r) => ({ status: r.status, body: (await r.json().catch(() => ({}))) as any }));
  return { handle, post, get };
}

async function pollUntil<T>(fn: () => Promise<T>, ok: (v: T) => boolean, what: string, tries = 120, everyMs = 250): Promise<T> {
  let last: T | undefined;
  for (let i = 0; i < tries; i++) {
    last = await fn();
    if (ok(last)) return last;
    await new Promise((r) => setTimeout(r, everyMs));
  }
  throw new Error(`timed out waiting for ${what}; last value: ${JSON.stringify(last)}`);
}

async function settledWithin(p: Promise<unknown>, ms: number): Promise<"settled" | "pending"> {
  let t: ReturnType<typeof setTimeout> | undefined;
  const r = await Promise.race([p.then(() => "settled" as const, () => "settled" as const), new Promise<"pending">((res) => (t = setTimeout(() => res("pending"), ms)))]);
  clearTimeout(t);
  return r;
}

async function runStatus(runId: string): Promise<string | undefined> {
  const [rows] = await admin.query(`SELECT status FROM demo_runs WHERE run_id = ?`, [runId]);
  return (rows as { status: string }[])[0]?.status;
}

test("shutdown honestly reports a timeout while the active run is held mid-flight, and its cleanup promise only settles once cleanup genuinely finishes", async () => {
  const barrier = stageBarrier("generation");
  process.env.RELAYLAB_SHUTDOWN_TIMEOUT_MS = "200";
  let s: Awaited<ReturnType<typeof startServer>>;
  try {
    s = await startServer({ executor: { beforeStage: barrier.beforeStage } });
  } finally {
    delete process.env.RELAYLAB_SHUTDOWN_TIMEOUT_MS;
  }
  const started = await s.post("/api/runs", { mode: "deterministic" });
  assert.equal(started.status, 201);
  const runId = await barrier.entered; // the run is now provably mid-flight, blocked at the barrier

  const result = await s.handle.shutdown();
  assert.equal(result.timedOut, true, "cleanup cannot finish while the run is held -- the deadline must be reported as a timeout");
  assert.equal(await settledWithin(result.cleanup, 300), "pending", "a timed-out shutdown must not have claimed cleanup completion");
  assert.equal((await s.handle.shutdown()).timedOut, true, "shutdown is memoized");

  barrier.release();
  assert.equal(await settledWithin(result.cleanup, 60_000), "settled", "cleanup must genuinely complete once the run can proceed");
  assert.equal(await runStatus(runId), "cancelled", "the run that shutdown cancelled must be recorded as cancelled");
});

test("a second lifecycle in the same process is unaffected by the first one's shutdown (admission is per server, not module-global)", async () => {
  const barrier = stageBarrier("generation");
  const s = await startServer({ executor: { beforeStage: barrier.beforeStage } });
  try {
    const started = await s.post("/api/runs", { mode: "deterministic" });
    assert.equal(started.status, 201, `a fresh server must admit runs even though an earlier server in this process blocked its own admission: ${JSON.stringify(started.body)}`);
    await barrier.entered;
  } finally {
    const shut = s.handle.shutdown();
    barrier.release();
    await (await shut).cleanup;
  }
});

test("a startup failure after the lock is acquired releases the lock and pool -- an immediate restart succeeds", async () => {
  await admin.query(`RENAME TABLE demo_runs TO demo_runs_disabled_for_startup_test`);
  try {
    await assert.rejects(() => createApiServer({ forbidLiveMode: true }), "startup must fail when reconciliation cannot reach demo_runs");
  } finally {
    await admin.query(`RENAME TABLE demo_runs_disabled_for_startup_test TO demo_runs`);
  }
  const recovered = await createApiServer({ forbidLiveMode: true });
  await (await recovered.shutdown()).cleanup;
});

test("end-to-end lock takeover during an active run: the stale server blocks admission and its run ends cancelled", async () => {
  const barrier = stageBarrier("generation");
  const s = await startServer({ lockTimings: { leaseMs: 10_000, renewIntervalMs: 100, watchdogIntervalMs: 50 }, executor: { beforeStage: barrier.beforeStage } });
  try {
    const started = await s.post("/api/runs", { mode: "deterministic" });
    assert.equal(started.status, 201);
    const runId = await barrier.entered;

    // A successor takes the row (simulated stall-and-replace).
    await admin.query(`UPDATE publisher_leader SET lease_expires_at = DATE_SUB(NOW(3), INTERVAL 1 SECOND) WHERE lock_name = ?`, [DEMO_API_LOCK_NAME]);
    assert.equal((await tryAcquireOrRenewExclusiveLock(admin, "successor-in-test", 10_000, DEMO_API_LOCK_NAME)).acquired, true);

    const refused = await pollUntil(() => s.post("/api/runs", { mode: "deterministic" }), (r) => r.status === 503, "the stale server to refuse admission");
    assert.equal(refused.body.error.code, "singleton_lock_lost");
    assert.equal((await s.get("/api/status")).body.lockHeld, false);

    barrier.release();
    await pollUntil(() => s.get("/api/status"), (r) => r.body.activeRunId === null, "the stale server's run to finish");
    assert.equal(await runStatus(runId), "cancelled", "lock loss must cancel the active run, not let it run to completion");
  } finally {
    barrier.release();
    await (await s.handle.shutdown()).cleanup;
    await admin.query(`UPDATE publisher_leader SET owner_id = NULL, lease_expires_at = NULL WHERE lock_name = ?`, [DEMO_API_LOCK_NAME]);
  }
});

test("a terminal-write failure is surfaced as degraded on GET /api/runs (the endpoint the UI polls) and reconciled once storage returns, without a restart", async () => {
  const barrier = stageBarrier("checkpoint");
  const s = await startServer({ executor: { beforeStage: barrier.beforeStage } });
  try {
    const started = await s.post("/api/runs", { mode: "deterministic" });
    assert.equal(started.status, 201);
    const runId = await barrier.entered; // worker A has durably checkpointed; the run's next write is the "checkpointed" transition
    const lastDurable = await runStatus(runId);

    await admin.query(`RENAME TABLE demo_runs TO demo_runs_disabled_for_degraded_test`);
    try {
      barrier.release();
      // Both the transition AND the resulting terminal write fail; the run ends with its outcome held in memory.
      await pollUntil(() => s.get("/api/status"), (r) => r.body.activeRunId === null, "the run to finish while storage is down");
    } finally {
      await admin.query(`RENAME TABLE demo_runs_disabled_for_degraded_test TO demo_runs`);
    }

    const first = await s.get("/api/runs");
    const degradedRow = first.body.runs.find((r: any) => r.runId === runId);
    assert.ok(degradedRow?.degraded, "the polled list must flag the run as degraded instead of showing a stale live status");
    assert.equal(degradedRow.status, lastDurable, "the durable row is shown as-is (stale), with the degraded note -- never a fabricated outcome");
    assert.doesNotMatch(degradedRow.degraded, /demo_runs_disabled/, "the note is sanitized");

    const reconciled = await pollUntil(
      () => s.get("/api/runs"),
      (r) => {
        const row = r.body.runs.find((x: any) => x.runId === runId);
        return row && !row.degraded && row.status === "failed";
      },
      "bounded reconciliation to durably record the held terminal outcome",
      40,
      500,
    );
    const row = reconciled.body.runs.find((x: any) => x.runId === runId);
    assert.match(row.detail.error, /^storage_error:/, "the reconciled outcome is the sanitized storage failure that actually ended the run");
    assert.equal(row.detail.checkpointDigest, undefined, "the never-recorded 'checkpointed' transition is not invented after the fact");
  } finally {
    barrier.release();
    await (await s.handle.shutdown()).cleanup;
  }
});

test("a stale owner can never overwrite a successor's terminal decision for the same run", async () => {
  const barrier = stageBarrier("checkpoint");
  const s = await startServer({ executor: { beforeStage: barrier.beforeStage } });
  try {
    const started = await s.post("/api/runs", { mode: "deterministic" });
    const runId = (started.body as { runId: string }).runId;
    await barrier.entered;
    // A successor's reconcileOnStartup decides this run's outcome while the old owner is still mid-flight.
    await admin.query(`UPDATE demo_runs SET status = 'interrupted' WHERE run_id = ?`, [runId]);
    barrier.release();
    await pollUntil(() => s.get("/api/status"), (r) => r.body.activeRunId === null, "the stale owner's run to stop");
    assert.equal(await runStatus(runId), "interrupted", "the stale owner's later writes must be refused, leaving the successor's decision intact");
  } finally {
    barrier.release();
    await (await s.handle.shutdown()).cleanup;
  }
});
