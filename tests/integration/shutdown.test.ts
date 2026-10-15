// Section 7 of the Phase 1.1 audit (graceful shutdown), plus Part A.1 of
// the Phase 1.2 audit (robust harness cleanup -- see
// docs/decisions/0009-harness-child-cleanup.md). Spawns each process via
// the shared ProcessHarness, requests graceful shutdown over IPC, and
// verifies both the shutdown behavior itself AND that the harness always
// cleans its children up, on success, assertion failure, and timeout.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import path from "node:path";
import type mysql from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { runPublisherOnce } from "../../src/messaging/publisher.js";
import { loadTestMysqlConfig } from "../../src/config.js";
import { ProcessHarness, testChildEnv, repoRootFrom } from "./process-harness.js";
import { applySchema, requireMysqlPool, requireNats, resetStream, truncateAll, uniqueId, type NatsTestHandles } from "./helpers.js";

let pool: mysql.Pool;
let nats: NatsTestHandles;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
  nats = await requireNats();
});

beforeEach(async () => {
  await truncateAll(pool);
  await resetStream(nats.jsm);
});

after(async () => {
  await nats.nc.drain();
  await pool.end();
});

const here = path.dirname(fileURLToPath(import.meta.url));
const publisherEntry = path.join(here, "..", "..", "src", "messaging", "publisher-main.ts");
const consumerEntry = path.join(here, "..", "..", "src", "messaging", "consumer-main.ts");
const repoRoot = repoRootFrom(here);
const testConfig = loadTestMysqlConfig();

function waitForExit(harness: ProcessHarness, child: ReturnType<ProcessHarness["spawn"]>, timeoutMs: number) {
  return new Promise<{ code: number | null; signal: string | null }>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`process did not exit within ${timeoutMs}ms of shutdown signal`)), timeoutMs);
    child.exited.then((r) => {
      clearTimeout(timer);
      resolve(r);
    });
  });
}

test("publisher: a graceful shutdown request (IPC on this Windows dev machine; SIGTERM/SIGINT in production) stops polling and the process exits cleanly, releasing its DB/NATS connections", async () => {
  const harness = new ProcessHarness();
  try {
    const store = makeTaskStoreLive(pool);
    const taskId = uniqueId("task");
    const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 60_000));
    for (let i = 0; i < 3; i++) {
      await Effect.runPromise(
        store.applyTaskCommand({
          taskId,
          ownerId: "worker-a",
          epoch: claim.epoch,
          commandId: `cmd-${i}`,
          command: { type: "Checkpoint", digest: `d${i}` },
        }),
      );
    }

    const child = harness.spawn({ label: "pub", entry: publisherEntry, cwd: repoRoot, env: testChildEnv(testConfig.database) });
    // Give it a moment to acquire leadership and start publishing.
    await new Promise((r) => setTimeout(r, 600));

    child.process.send("shutdown");
    const exit = await waitForExit(harness, child, 10000);
    // Phase 1.2.1 Part A.1: there is no process.exit() call on this path
    // any more -- a NATURAL exit (nothing left keeping the event loop
    // alive after NATS drain / MySQL pool end / telemetry shutdown all
    // returned) looks IDENTICAL here (signal null, code 0) to a forced
    // exit, which is exactly why this test alone cannot distinguish them
    // -- see the dedicated natural-exit tests below, which additionally
    // assert the "graceful shutdown steps complete" log line appeared
    // BEFORE exit was observed, proving the steps actually ran to
    // completion rather than the process being cut off mid-sequence.
    assert.equal(exit.signal, null, "the process should exit on its own after graceful shutdown, not be killed by a signal");
    assert.equal(exit.code, 0);
    assert.ok(child.lines.some((l) => l.includes("shutdown requested")), "must log that shutdown was requested");
    // NOTE on what this does NOT assert, and why: checking that no
    // "acquired leadership" / "published" line appears textually AFTER the
    // "shutdown requested" line is tempting but FLAKY -- the iteration that
    // was already in flight when the shutdown request arrived keeps running
    // to completion (by design: `stop()` awaits `currentWork` rather than
    // aborting it), and ITS OWN log lines can legitimately interleave with
    // or print after "shutdown requested" purely due to console output
    // timing between two concurrent async flows, with no second iteration
    // ever having started.
  } finally {
    await harness.stopAll();
  }
});

test("consumer: a graceful shutdown request (IPC on this Windows dev machine; SIGTERM/SIGINT in production) stops polling and the process exits cleanly, releasing its DB/NATS connections", async () => {
  const harness = new ProcessHarness();
  try {
    const store = makeTaskStoreLive(pool);
    const taskId = uniqueId("task");
    const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 60_000));
    await Effect.runPromise(
      store.applyTaskCommand({ taskId, ownerId: "worker-a", epoch: claim.epoch, commandId: "cmd-1", command: { type: "Checkpoint", digest: "d1" } }),
    );
    await runPublisherOnce(pool, nats.js);

    const child = harness.spawn({ label: "con", entry: consumerEntry, cwd: repoRoot, env: testChildEnv(testConfig.database) });
    await new Promise((r) => setTimeout(r, 400));

    child.process.send("shutdown");
    const exit = await waitForExit(harness, child, 10000);
    assert.equal(exit.signal, null, "the process should exit on its own after graceful shutdown, not be killed by a signal");
    assert.equal(exit.code, 0);
    assert.ok(child.lines.some((l) => l.includes("shutdown requested")), "must log that shutdown was requested");
  } finally {
    await harness.stopAll();
  }
});

// Phase 1.2.1 Part A.1: natural-exit evidence. Both tests below enable
// real telemetry (unlike every other test in this file) so the FULL
// graceful-shutdown sequence -- stop polling, finish in-flight work,
// drain NATS, end the MySQL pool, flush/shutdown the OTel SDK -- runs
// for real, with NO process.exit() call anywhere in the path. A process
// that still exits cleanly (code 0, no signal) after this, well inside
// the harness's bounded wait, is proof the process released every
// resource it opened ON ITS OWN -- the harness's SIGKILL fallback never
// had to fire.
test("publisher: exits NATURALLY (no process.exit) after a successful graceful shutdown with telemetry enabled and the Collector reachable", async () => {
  const harness = new ProcessHarness();
  try {
    const child = harness.spawn({
      label: "pub-natural-exit-ok",
      entry: publisherEntry,
      cwd: repoRoot,
      env: testChildEnv(testConfig.database, { RELAYLAB_OTEL_DISABLED: "0" }),
    });
    await new Promise((r) => setTimeout(r, 600));

    child.process.send("shutdown");
    const exit = await waitForExit(harness, child, 10000);
    assert.equal(exit.signal, null, "must not have required SIGKILL -- the harness's forced-termination fallback must never have fired");
    assert.equal(exit.code, 0);
    assert.ok(
      child.lines.some((l) => l.includes("graceful shutdown steps complete")),
      "the process must have logged that every shutdown step (NATS drain, pool end, telemetry shutdown) actually completed before exiting",
    );
    assert.ok(
      !child.lines.some((l) => l.includes("handle(s) still active")),
      "the lingering-handle diagnostic must never have fired -- nothing should have been left open",
    );
  } finally {
    await harness.stopAll();
  }
});

test("consumer: exits NATURALLY (no process.exit) after a successful graceful shutdown even when the Collector is UNAVAILABLE (telemetry shutdown hits its own bound, not the process's)", async () => {
  const harness = new ProcessHarness();
  try {
    const child = harness.spawn({
      label: "con-natural-exit-no-collector",
      entry: consumerEntry,
      cwd: repoRoot,
      env: testChildEnv(testConfig.database, {
        RELAYLAB_OTEL_DISABLED: "0",
        // Deliberately unreachable -- port 1 is privileged/unused on
        // this machine, so the exporter's connection attempt fails fast.
        RELAYLAB_OTEL_EXPORTER_OTLP_ENDPOINT: "http://127.0.0.1:1/v1/traces",
      }),
    });
    await new Promise((r) => setTimeout(r, 400));

    child.process.send("shutdown");
    const exit = await waitForExit(harness, child, 10000);
    assert.equal(exit.signal, null, "an unreachable Collector must not prevent a natural exit -- shutdownTelemetry is bounded");
    assert.equal(exit.code, 0);
    assert.ok(child.lines.some((l) => l.includes("graceful shutdown steps complete")));
  } finally {
    await harness.stopAll();
  }
});

test("publisher: repeated NATS/DB failures back off instead of hot-looping (bounded backoff, not unbounded retry)", async () => {
  const harness = new ProcessHarness();
  try {
    // Point the publisher at a MySQL port nothing is listening on, so every
    // iteration fails. Confirm it logs a BOUNDED, small number of failures
    // over a fixed window rather than spinning as fast as possible (which
    // would produce hundreds of "iteration failed" lines in the same window).
    const child = harness.spawn({
      label: "pub-fail",
      entry: publisherEntry,
      cwd: repoRoot,
      env: testChildEnv(testConfig.database, { RELAYLAB_MYSQL_PORT: "39999" }),
    });
    await new Promise((r) => setTimeout(r, 3000));

    const failureLines = child.lines.filter((l) => l.includes("iteration failed"));
    assert.ok(failureLines.length < 20, `expected backoff to bound retries well under a hot loop, saw ${failureLines.length} in 3s`);
    // This process never gets a usable DB connection to even acquire the
    // publisher lock, so there is nothing to shut down gracefully via IPC
    // here -- it is force-stopped by harness.stopAll() below, which is
    // exactly the "graceful attempt times out/is unavailable, so force-kill
    // and await exit" path.
  } finally {
    await harness.stopAll();
  }
});

test("harness cleanup: a SUCCESSFUL scenario still has all its spawned children exited after stopAll(), verified by both liveCount and a live OS process check", async () => {
  const harness = new ProcessHarness();
  const child = harness.spawn({ label: "pub-ok", entry: publisherEntry, cwd: repoRoot, env: testChildEnv(testConfig.database) });
  await new Promise((r) => setTimeout(r, 400));
  const pid = child.process.pid!;
  assert.ok(isPidAlive(pid), "the child must actually be running before we claim to have cleaned it up");

  await harness.stopAll();

  assert.equal(harness.liveCount, 0, "the harness must consider zero children live after stopAll()");
  assert.equal(child.process.exitCode === null && child.process.signalCode === null, false, "the child's own exit fields must be set");
  assert.ok(!isPidAlive(pid), "the OS must no longer have a process at this pid (checked independently of Node's own bookkeeping)");
});

test("harness cleanup: a scenario that THROWS (simulated assertion failure) mid-flight still cleans up its spawned child via finally, and a readiness timeout never leaves a hung child behind", async () => {
  const harness = new ProcessHarness();
  let pid: number | undefined;
  await assert.rejects(async () => {
    try {
      const child = harness.spawn({ label: "pub-throws", entry: publisherEntry, cwd: repoRoot, env: testChildEnv(testConfig.database) });
      pid = child.process.pid;
      await new Promise((r) => setTimeout(r, 300));
      // Simulate an assertion failure / a failpoint-readiness timeout:
      // something goes wrong partway through, well before any explicit
      // shutdown request was sent to the child.
      throw new Error("simulated assertion failure mid-scenario");
    } finally {
      await harness.stopAll();
    }
  }, /simulated assertion failure/);

  assert.equal(harness.liveCount, 0, "cleanup must still have run even though the scenario threw");
  assert.ok(pid !== undefined && !isPidAlive(pid), "the child spawned before the throw must not be left running");
});

function isPidAlive(pid: number): boolean {
  try {
    // signal 0 sends nothing; it only checks whether the process exists
    // and is signalable. Throws ESRCH-equivalent if it does not.
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
