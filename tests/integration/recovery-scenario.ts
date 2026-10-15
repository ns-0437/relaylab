// Recovery scenario demo / evidence script. Runs the consumer as a REAL
// separate OS process, SIGKILLs it mid-stream, and shows that a second
// consumer process picks up where it left off with no duplicated local
// effects. This is the one Phase-1 test that uses actual process
// termination rather than an in-process failpoint (see
// tests/integration/consumer-dedup.test.ts for the deterministic
// failpoint-style tests, which are faster and more precise about which
// exact crash window they exercise). Uses the shared ProcessHarness
// (Phase 1.2 audit, Part A.1 -- see docs/decisions/0009-harness-child-cleanup.md)
// so both consumer processes are cleaned up even if an assertion/wait
// below throws.
//
// Usage: npm run demo:recovery
// Requires: docker compose -f infra/compose.yaml up -d, then npm run migrate

import { fileURLToPath } from "node:url";
import path from "node:path";
import mysql from "mysql2/promise";
import { Effect } from "effect";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { runPublisherOnce } from "../../src/messaging/publisher.js";
import { connectNats, ensureEventsStream, ensureDurableConsumer } from "../../src/messaging/nats-client.js";
import { loadMysqlConfig, loadNatsConfig, eventsDurable } from "../../src/config.js";
import { ProcessHarness, repoRootFrom } from "./process-harness.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const consumerEntry = path.join(here, "..", "..", "src", "messaging", "consumer-main.ts");
const repoRoot = repoRootFrom(here);

function log(msg: string) {
  console.log(`[recovery-scenario] ${msg}`);
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs: number, label: string): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(`timed out waiting for: ${label}`);
}

async function main() {
  const pool = mysql.createPool({ ...loadMysqlConfig(), waitForConnections: true, connectionLimit: 10 });
  const harness = new ProcessHarness();
  try {
    const { nc, js, jsm } = await connectNats(loadNatsConfig().servers);
    try {
      await ensureEventsStream(jsm);
      await ensureDurableConsumer(jsm);

      const store = makeTaskStoreLive(pool);
      const taskId = `recovery-demo-${Date.now()}`;
      const claim = await Effect.runPromise(store.claimTask(taskId, "demo-worker", 60_000));
      log(`claimed ${taskId} at epoch ${claim.epoch}`);

      // Produce several events so there is real in-flight work for the killed
      // consumer to have partially processed.
      for (let i = 0; i < 5; i++) {
        await Effect.runPromise(
          store.applyTaskCommand({
            taskId,
            ownerId: "demo-worker",
            epoch: claim.epoch,
            commandId: `checkpoint-${i}`,
            command: { type: "Checkpoint", digest: `digest-${i}` },
          }),
        );
      }
      await runPublisherOnce(pool, js);
      log("published 5 checkpoint events");

      log("starting consumer process A (artificially slowed so a mid-batch kill is reproducible)");
      const consumerA = harness.spawn({
        label: "consumer-A",
        entry: consumerEntry,
        cwd: repoRoot,
        env: { ...process.env, RELAYLAB_OTEL_DISABLED: "1", RELAYLAB_DEMO_SLOWDOWN_MS: "700", RELAYLAB_DEMO_BATCH_SIZE: "1" },
      });

      // Wait for genuine partial progress (not a blind sleep) before killing,
      // so the kill reliably lands mid-batch rather than before any work or
      // after all of it.
      await waitFor(
        async () => {
          const [rows] = await pool.query<mysql.RowDataPacket[]>(
            `SELECT event_count FROM task_consumer_state WHERE consumer_name = ? AND task_id = ?`,
            [eventsDurable(), taskId],
          );
          return (rows[0]?.event_count ?? 0) >= 2;
        },
        10_000,
        "consumer A to make partial progress before it is killed",
      );
      log(`killing consumer process A mid-batch (pid ${consumerA.process.pid}) -- this is a real SIGKILL of a real OS process`);
      consumerA.process.kill("SIGKILL");
      await consumerA.exited;
      log("consumer process A is dead");

      const [partialRows] = await pool.query<mysql.RowDataPacket[]>(
        `SELECT event_count FROM task_consumer_state WHERE consumer_name = ? AND task_id = ?`,
        [eventsDurable(), taskId],
      );
      const partialCount = partialRows[0]?.event_count ?? 0;
      log(`after kill, durable progress recorded: ${partialCount} / 5 events`);

      log("starting consumer process B to recover");
      harness.spawn({ label: "consumer-B", entry: consumerEntry, cwd: repoRoot, env: { ...process.env, RELAYLAB_OTEL_DISABLED: "1" } });

      await waitFor(
        async () => {
          const [rows] = await pool.query<mysql.RowDataPacket[]>(
            `SELECT event_count FROM task_consumer_state WHERE consumer_name = ? AND task_id = ?`,
            [eventsDurable(), taskId],
          );
          return (rows[0]?.event_count ?? 0) >= 5;
        },
        20_000, // must exceed the durable consumer's ack_wait (8s, see nats-client.ts)
        "consumer B to finish processing all 5 events",
      );

      const [finalRows] = await pool.query<mysql.RowDataPacket[]>(
        `SELECT event_count, last_applied_seq, gap_detected FROM task_consumer_state WHERE consumer_name = ? AND task_id = ?`,
        [eventsDurable(), taskId],
      );
      const final = finalRows[0]!;
      log(`final state: event_count=${final.event_count} last_applied_seq=${final.last_applied_seq} gap_detected=${final.gap_detected}`);

      if (final.event_count !== 5) {
        throw new Error(`expected exactly 5 processed events after recovery (no duplicates, no loss), got ${final.event_count}`);
      }
      log("PASS: recovery after real process kill produced exactly 5 local effects, no duplicates, no loss");
    } finally {
      await nc.drain();
    }
  } finally {
    await harness.stopAll();
    await pool.end();
  }
}

main().catch((err) => {
  console.error("[recovery-scenario] FAILED:", err);
  process.exitCode = 1;
});
