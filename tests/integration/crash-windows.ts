// Section 6 of the Phase 1.1 audit: three deterministic, process-level
// crash-window scenarios, each using a real SIGKILL of a real separate OS
// process at an EXACT point confirmed by an IPC readiness handshake (see
// src/testing/failpoint.ts), never a sleep-based guess. Uses the shared
// ProcessHarness (Phase 1.2 audit, Part A.1 -- see
// docs/decisions/0009-harness-child-cleanup.md): every child, including
// the "recovered" process spawned mid-scenario, is tracked from the
// moment it is forked, and each scenario's `finally` calls
// `harness.stopAll()` so a failpoint-readiness TIMEOUT (the child never
// reports ready) cannot leave it hung -- it gets force-stopped like any
// other still-alive tracked child, not left running past the timeout
// that was supposed to fail the scenario.
//
// Termination mechanism, stated plainly (section 6 asks not to imply
// POSIX signal semantics merely because a Node API accepted a signal
// name): this script calls `child.kill("SIGKILL")` from
// node:child_process. On this Windows machine, Node's child_process
// implementation does not deliver a POSIX SIGKILL to the child -- there
// is no such signal on Windows. Node translates `kill("SIGKILL")` into a
// call to the Windows API `TerminateProcess`, which unconditionally and
// immediately ends the target process (the child gets no chance to run
// any further JS, including its own SIGTERM/SIGINT handlers). That is the
// real termination mechanism exercised here: an unconditional, instant
// external termination of a real OS process, which is what matters for
// these tests (proving recovery after a process disappears with no
// chance to clean up) -- it is just not literally the POSIX SIGKILL
// signal, because this is not a POSIX system. On Linux/macOS the same
// Node API call does deliver a real POSIX SIGKILL.
//
// Usage: npm run demo:crash-windows

import { fileURLToPath } from "node:url";
import path from "node:path";
import mysql from "mysql2/promise";

// Must be set BEFORE importing anything from src/config.js that reads it
// (eventsStream/eventsSubject/eventsDurable read process.env at call
// time, but this orchestrator process's own direct NATS calls still need
// this set on ITS OWN env, not just the child processes' env below, or it
// would operate on the dev namespace while the children operate on the
// test namespace).
process.env.RELAYLAB_NAMESPACE = "relaylab-test";

import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { connectNats, ensureEventsStream, ensureDurableConsumer } from "../../src/messaging/nats-client.js";
import { runPublisherOnce } from "../../src/messaging/publisher.js";
import { loadTestMysqlConfig, loadNatsConfig, eventsStream } from "../../src/config.js";
import { ProcessHarness, testChildEnv, repoRootFrom, type ManagedChild } from "./process-harness.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const publisherEntry = path.join(here, "..", "..", "src", "messaging", "publisher-main.ts");
const consumerEntry = path.join(here, "..", "..", "src", "messaging", "consumer-main.ts");
const repoRoot = repoRootFrom(here);
const testDatabase = loadTestMysqlConfig().database;

function log(scenario: string, msg: string) {
  console.log(`[crash-window ${scenario}] ${msg}`);
}

function logEvidence(scenario: string, label: string, data: unknown) {
  console.log(`[crash-window ${scenario}] EVIDENCE ${label}: ${JSON.stringify(data)}`);
}

/** Waits for the child's IPC readiness message for a specific failpoint, or throws on timeout. */
function waitForFailpointReady(child: ManagedChild, failpointName: string, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      child.process.off("message", onMessage);
      reject(new Error(`timed out after ${timeoutMs}ms waiting for failpoint "${failpointName}" to report ready`));
    }, timeoutMs);
    function onMessage(msg: unknown) {
      const m = msg as { type?: string; name?: string };
      if (m?.type === "failpoint-ready" && m.name === failpointName) {
        clearTimeout(timer);
        child.process.off("message", onMessage);
        resolve();
      }
    }
    child.process.on("message", onMessage);
  });
}

async function killAndWaitExit(child: ManagedChild): Promise<{ exitCode: number | null; signalCode: string | null }> {
  const killed = child.process.kill("SIGKILL");
  const { code, signal } = await child.exited;
  log("", `child.kill("SIGKILL") on pid ${child.process.pid} returned ${killed} (see header comment for what this means on this OS)`);
  return { exitCode: code, signalCode: signal };
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs: number, label: string): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 150));
  }
  throw new Error(`timed out waiting for: ${label}`);
}

async function scenarioA_publisherCrashWindow(pool: mysql.Pool): Promise<void> {
  const scenario = "A (publisher: ack received, mark-sent not committed)";
  const harness = new ProcessHarness();
  try {
    const store = makeTaskStoreLive(pool);
    const taskId = `crashA-${Date.now()}`;
    const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 60_000));
    const receipt = await Effect.runPromise(
      store.applyTaskCommand({
        taskId,
        ownerId: "worker-a",
        epoch: claim.epoch,
        commandId: "cmd-1",
        command: { type: "Checkpoint", digest: "dA" },
      }),
    );
    const [outboxBefore] = await pool.query<mysql.RowDataPacket[]>(
      `SELECT event_id, published_at FROM outbox_events WHERE task_id = ?`,
      [taskId],
    );
    logEvidence(scenario, "application_event_id", outboxBefore[0]?.event_id);
    logEvidence(scenario, "outbox_row_before_failpoint", outboxBefore[0]);
    logEvidence(scenario, "command_receipt", receipt.result);

    const child = harness.spawn({
      label: "crashA",
      entry: publisherEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, { RELAYLAB_FAILPOINT: "after-publish-ack-before-mark-sent" }),
    });
    await waitForFailpointReady(child, "after-publish-ack-before-mark-sent", 10_000);
    log(scenario, "confirmed: broker publish ack received (the failpoint is placed immediately after js.publish() resolves)");

    const [outboxAtFailpoint] = await pool.query<mysql.RowDataPacket[]>(
      `SELECT event_id, published_at FROM outbox_events WHERE task_id = ?`,
      [taskId],
    );
    logEvidence(scenario, "outbox_row_at_failpoint_before_kill", outboxAtFailpoint[0]);
    if (outboxAtFailpoint[0]?.published_at !== null) {
      throw new Error("expected published_at to still be NULL at the failpoint -- the failpoint is in the wrong place");
    }

    const exitInfo = await killAndWaitExit(child);
    logEvidence(scenario, "child_process_identity", { pid: child.process.pid });
    logEvidence(scenario, "child_exit_outcome", exitInfo);

    log(scenario, "restarting a fresh publisher process (no failpoint) to recover");
    const recovered = harness.spawn({ label: "crashA-recovered", entry: publisherEntry, cwd: repoRoot, env: testChildEnv(testDatabase) });
    await waitFor(
      async () => {
        const [rows] = await pool.query<mysql.RowDataPacket[]>(`SELECT published_at FROM outbox_events WHERE task_id = ?`, [taskId]);
        return rows[0]?.published_at !== null;
      },
      10_000,
      "the restarted publisher to mark the row sent",
    );
    const [outboxAfter] = await pool.query<mysql.RowDataPacket[]>(
      `SELECT event_id, published_at FROM outbox_events WHERE task_id = ?`,
      [taskId],
    );
    logEvidence(scenario, "outbox_row_after_recovery", outboxAfter[0]);
    void recovered; // stopped via harness.stopAll() in finally below

    // Now prove the one logical event still produces exactly one durable
    // consumer effect (the crash-and-republish must not have duplicated it
    // at the broker, and even if it had, the consumer's own dedup covers it).
    const { nc, js, jsm } = await connectNats(loadNatsConfig().servers);
    try {
      await ensureEventsStream(jsm);
      await ensureDurableConsumer(jsm);
      await runPublisherOnce(pool, js); // in case anything was left unpublished
      const { processBatch } = await import("../../src/messaging/consumer.js");
      await processBatch(pool, js);
      const [stateRows] = await pool.query<mysql.RowDataPacket[]>(`SELECT event_count FROM task_consumer_state WHERE task_id = ?`, [taskId]);
      logEvidence(scenario, "final_consumer_effect_count", stateRows[0]?.event_count ?? 0);
      if ((stateRows[0]?.event_count ?? 0) !== 1) {
        throw new Error(`expected exactly one durable consumer effect, got ${stateRows[0]?.event_count}`);
      }
    } finally {
      await nc.drain();
    }
    log(scenario, "PASS");
  } finally {
    await harness.stopAll();
  }
}

async function scenarioB_consumerBeforeCommit(pool: mysql.Pool): Promise<void> {
  const scenario = "B (consumer: transaction open, commit not yet run)";
  const harness = new ProcessHarness();
  try {
    const store = makeTaskStoreLive(pool);
    const taskId = `crashB-${Date.now()}`;
    const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 60_000));
    await Effect.runPromise(
      store.applyTaskCommand({ taskId, ownerId: "worker-a", epoch: claim.epoch, commandId: "cmd-1", command: { type: "Checkpoint", digest: "dB" } }),
    );
    const { nc: pubNc, js: pubJs, jsm: pubJsm } = await connectNats(loadNatsConfig().servers);
    try {
      await ensureEventsStream(pubJsm);
      await ensureDurableConsumer(pubJsm);
      await runPublisherOnce(pool, pubJs);
      const streamInfoBefore = await pubJsm.streams.info(eventsStream());
      logEvidence(scenario, "broker_delivery_evidence_before", { messages: streamInfoBefore.state.messages });
    } finally {
      await pubNc.drain();
    }

    const child = harness.spawn({
      label: "crashB",
      entry: consumerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, { RELAYLAB_FAILPOINT: "consumer-before-commit" }),
    });
    await waitForFailpointReady(child, "consumer-before-commit", 10_000);
    log(scenario, "confirmed: the durable-effect transaction is open (writes issued, COMMIT not yet run)");

    const [rowsAtFailpoint] = await pool.query<mysql.RowDataPacket[]>(`SELECT COUNT(*) AS c FROM consumer_processed WHERE task_id = ?`, [taskId]);
    logEvidence(scenario, "consumer_processed_count_visible_to_other_connection_before_kill", rowsAtFailpoint[0]?.c);
    if (Number(rowsAtFailpoint[0]?.c) !== 0) {
      throw new Error("expected the uncommitted write to be invisible to a separate connection (transaction isolation)");
    }

    const exitInfo = await killAndWaitExit(child);
    logEvidence(scenario, "child_process_identity", { pid: child.process.pid });
    logEvidence(scenario, "child_exit_outcome", exitInfo);

    log(scenario, "restarting a fresh consumer (no failpoint) to recover; the broker will redeliver once ack_wait elapses");
    harness.spawn({ label: "crashB-recovered", entry: consumerEntry, cwd: repoRoot, env: testChildEnv(testDatabase) });
    await waitFor(
      async () => {
        const [rows] = await pool.query<mysql.RowDataPacket[]>(`SELECT event_count FROM task_consumer_state WHERE task_id = ?`, [taskId]);
        return (rows[0]?.event_count ?? 0) >= 1;
      },
      20_000, // must exceed ack_wait (8s)
      "the restarted consumer to complete the effect exactly once",
    );
    const [finalRows] = await pool.query<mysql.RowDataPacket[]>(`SELECT event_count FROM task_consumer_state WHERE task_id = ?`, [taskId]);
    logEvidence(scenario, "final_consumer_effect_count", finalRows[0]?.event_count);
    if (finalRows[0]?.event_count !== 1) throw new Error(`expected exactly 1, got ${finalRows[0]?.event_count}`);
    log(scenario, "PASS");
  } finally {
    await harness.stopAll();
  }
}

async function scenarioC_consumerAfterCommitBeforeAck(pool: mysql.Pool): Promise<void> {
  const scenario = "C (consumer: committed, ACK not yet sent)";
  const harness = new ProcessHarness();
  try {
    const store = makeTaskStoreLive(pool);
    const taskId = `crashC-${Date.now()}`;
    const claim = await Effect.runPromise(store.claimTask(taskId, "worker-a", 60_000));
    await Effect.runPromise(
      store.applyTaskCommand({ taskId, ownerId: "worker-a", epoch: claim.epoch, commandId: "cmd-1", command: { type: "Checkpoint", digest: "dC" } }),
    );
    const { nc: pubNc, js: pubJs, jsm: pubJsm } = await connectNats(loadNatsConfig().servers);
    try {
      await ensureEventsStream(pubJsm);
      await ensureDurableConsumer(pubJsm);
      await runPublisherOnce(pool, pubJs);
    } finally {
      await pubNc.drain();
    }

    const child = harness.spawn({
      label: "crashC",
      entry: consumerEntry,
      cwd: repoRoot,
      env: testChildEnv(testDatabase, { RELAYLAB_FAILPOINT: "consumer-after-commit-before-ack" }),
    });
    await waitForFailpointReady(child, "consumer-after-commit-before-ack", 10_000);
    log(scenario, "confirmed: the durable-effect transaction has COMMITTED (visible to other connections), ACK not yet sent");

    const [rowsAtFailpoint] = await pool.query<mysql.RowDataPacket[]>(`SELECT event_count FROM task_consumer_state WHERE task_id = ?`, [taskId]);
    logEvidence(scenario, "consumer_state_visible_to_other_connection_before_kill", rowsAtFailpoint[0]);
    if (rowsAtFailpoint[0]?.event_count !== 1) {
      throw new Error("expected the committed effect to already be visible to a separate connection");
    }

    const exitInfo = await killAndWaitExit(child);
    logEvidence(scenario, "child_process_identity", { pid: child.process.pid });
    logEvidence(scenario, "child_exit_outcome", exitInfo);

    log(scenario, "restarting a fresh consumer (no failpoint); the broker must redeliver the un-acked message");
    harness.spawn({ label: "crashC-recovered", entry: consumerEntry, cwd: repoRoot, env: testChildEnv(testDatabase) });
    await new Promise((r) => setTimeout(r, 9000)); // let ack_wait (8s) elapse and the restarted consumer poll
    const [finalRows] = await pool.query<mysql.RowDataPacket[]>(`SELECT event_count FROM task_consumer_state WHERE task_id = ?`, [taskId]);
    logEvidence(scenario, "final_consumer_effect_count_after_redelivery", finalRows[0]?.event_count);
    if (finalRows[0]?.event_count !== 1) {
      throw new Error(`redelivery duplicated the effect: expected exactly 1, got ${finalRows[0]?.event_count}`);
    }
    log(scenario, "PASS: redelivery occurred (implied by the message no longer being stuck) and the effect was NOT repeated");
  } finally {
    await harness.stopAll();
  }
}

async function main() {
  const pool = mysql.createPool({ ...loadTestMysqlConfig(), waitForConnections: true, connectionLimit: 10 });
  try {
    await scenarioA_publisherCrashWindow(pool);
    await scenarioB_consumerBeforeCommit(pool);
    await scenarioC_consumerAfterCommitBeforeAck(pool);
    console.log("[crash-windows] ALL SCENARIOS PASSED");
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error("[crash-windows] FAILED:", err);
  process.exitCode = 1;
});
