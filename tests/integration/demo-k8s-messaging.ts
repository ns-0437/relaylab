// Phase 3A Part B: proves the REAL, already-running (in-cluster)
// publisher and consumer pods carry a command end to end -- this script
// deliberately does NOT spawn its own publisher/consumer processes
// (unlike recovery-scenario.ts/crash-windows.ts, which spawn local ones
// for a self-contained demo) and NEVER calls runPublisherOnce itself.
// It only writes a command through the existing TaskStore and polls the
// existing durable-consumer-state table, the same way recovery-scenario.ts
// verifies "no duplicates, no loss" -- the proof that the deployed
// pipeline, not this script, did the actual publishing/consuming.
//
// Three modes, driven by env vars so infra/k8s/scripts/05-pod-replacement-demo.sh
// can call this repeatedly (before, immediately after, and after killing
// the consumer pod) against the SAME task:
//   mode=seed      claims a NEW task, applies command index 0, waits for
//                   task_consumer_state.event_count to reach 1, and prints
//                   TASK_ID=/OWNER_ID=/EPOCH= lines for the caller to capture.
//   mode=continue   given RELAYLAB_K8S_DEMO_TASK_ID/_OWNER_ID/_EPOCH, applies
//                   the command at RELAYLAB_K8S_DEMO_SEQ and waits for
//                   event_count to reach RELAYLAB_K8S_DEMO_EXPECTED_COUNT.
//   mode=check      given RELAYLAB_K8S_DEMO_TASK_ID, read-only: asserts
//                   event_count is EXACTLY RELAYLAB_K8S_DEMO_EXPECTED_COUNT
//                   right now, without applying anything. Used to confirm
//                   durable state survived a pod replacement BEFORE any
//                   new command is applied.
//
// Connects via the SAME env vars publisher-main.ts/consumer-main.ts
// already read (RELAYLAB_MYSQL_*, see src/config.ts) -- pointed at the
// cluster via infra/k8s/scripts/start-port-forwards.sh, not at
// infra/compose.yaml's dev database.
import mysql from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { loadMysqlConfig, eventsDurable } from "../../src/config.js";

function log(msg: string) {
  console.log(`[demo-k8s-messaging] ${msg}`);
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs: number, label: string): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`timed out waiting for: ${label}`);
}

async function main() {
  const mode = process.env.RELAYLAB_K8S_DEMO_MODE ?? "seed";
  const pool = mysql.createPool({ ...loadMysqlConfig(), waitForConnections: true, connectionLimit: 5 });
  const store = makeTaskStoreLive(pool);

  try {
    if (mode === "check") {
      const taskId = process.env.RELAYLAB_K8S_DEMO_TASK_ID ?? "";
      const expectedCount = Number.parseInt(process.env.RELAYLAB_K8S_DEMO_EXPECTED_COUNT ?? "", 10);
      if (!taskId || !Number.isFinite(expectedCount)) {
        throw new Error("mode=check requires RELAYLAB_K8S_DEMO_TASK_ID/_EXPECTED_COUNT");
      }
      const [rows] = await pool.query<mysql.RowDataPacket[]>(
        `SELECT event_count FROM task_consumer_state WHERE consumer_name = ? AND task_id = ?`,
        [eventsDurable(), taskId],
      );
      const actual = rows[0]?.event_count ?? 0;
      if (actual !== expectedCount) {
        throw new Error(`durable state check FAILED: expected event_count=${expectedCount} for task "${taskId}", got ${actual}`);
      }
      log(`PASS: durable state intact -- event_count=${actual} for task "${taskId}" (read-only check, nothing applied)`);
      return;
    }

    let taskId: string;
    let ownerId: string;
    let epoch: number;
    let seq: number;
    let expectedCount: number;

    if (mode === "seed") {
      taskId = `k8s-messaging-demo-${Date.now()}`;
      ownerId = "k8s-messaging-demo-worker";
      const claim = await Effect.runPromise(store.claimTask(taskId, ownerId, 600_000));
      epoch = claim.epoch;
      seq = 0;
      expectedCount = 1;
      log(`claimed NEW task "${taskId}" as "${ownerId}" at epoch ${epoch}`);
    } else if (mode === "continue") {
      taskId = process.env.RELAYLAB_K8S_DEMO_TASK_ID ?? "";
      ownerId = process.env.RELAYLAB_K8S_DEMO_OWNER_ID ?? "";
      epoch = Number.parseInt(process.env.RELAYLAB_K8S_DEMO_EPOCH ?? "", 10);
      seq = Number.parseInt(process.env.RELAYLAB_K8S_DEMO_SEQ ?? "", 10);
      expectedCount = Number.parseInt(process.env.RELAYLAB_K8S_DEMO_EXPECTED_COUNT ?? "", 10);
      if (!taskId || !ownerId || !Number.isFinite(epoch) || !Number.isFinite(seq) || !Number.isFinite(expectedCount)) {
        throw new Error("mode=continue requires RELAYLAB_K8S_DEMO_TASK_ID/_OWNER_ID/_EPOCH/_SEQ/_EXPECTED_COUNT");
      }
      log(`continuing task "${taskId}" as "${ownerId}" at epoch ${epoch}, applying command seq=${seq}`);
    } else {
      throw new Error(`unknown RELAYLAB_K8S_DEMO_MODE "${mode}" (expected "seed" or "continue")`);
    }

    await Effect.runPromise(
      store.applyTaskCommand({
        taskId,
        ownerId,
        epoch,
        commandId: `checkpoint-${seq}`,
        command: { type: "Checkpoint", digest: `k8s-demo-digest-${seq}` },
      }),
    );
    log(`applied command seq=${seq} -- this creates an outbox row; the IN-CLUSTER publisher pod (not this script) will publish it`);

    await waitFor(
      async () => {
        const [rows] = await pool.query<mysql.RowDataPacket[]>(
          `SELECT event_count FROM task_consumer_state WHERE consumer_name = ? AND task_id = ?`,
          [eventsDurable(), taskId],
        );
        return (rows[0]?.event_count ?? 0) >= expectedCount;
      },
      30_000,
      `the IN-CLUSTER consumer pod to report event_count >= ${expectedCount} for task "${taskId}"`,
    );

    const [finalRows] = await pool.query<mysql.RowDataPacket[]>(
      `SELECT event_count, last_applied_seq FROM task_consumer_state WHERE consumer_name = ? AND task_id = ?`,
      [eventsDurable(), taskId],
    );
    const final = finalRows[0]!;
    if (final.event_count !== expectedCount) {
      throw new Error(`expected EXACTLY event_count=${expectedCount} (no duplicates, no loss), got ${final.event_count}`);
    }
    log(`PASS: event_count=${final.event_count} last_applied_seq=${final.last_applied_seq} -- the real in-cluster publisher+consumer pods carried this command end to end`);
    console.log(`TASK_ID=${taskId}`);
    console.log(`OWNER_ID=${ownerId}`);
    console.log(`EPOCH=${epoch}`);
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error("[demo-k8s-messaging] FAILED:", err);
  process.exitCode = 1;
});
