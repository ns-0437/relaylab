// AI repair milestone: `npm run demo:ai-repair` -- exercises the AI
// repair mode end to end against the SAME disposable fixture repository
// demo-git-worker.ts uses. Creates a fresh fixture, claims a real task,
// and runs `runAiRepairTask`, which drives a real worktree/commit, real
// Docker verification, and (on success) a real durable checkpoint --
// identical reliability-pipeline guarantees to the deterministic demo.
//
// Makes a REAL, PAID call to the Anthropic API. Requires an EXPLICIT
// opt-in: pass `--live` on the command line (or set
// RELAYLAB_AI_REPAIR_LIVE=1). Without it, this prints what it would do
// and exits 0 without spending anything or requiring credentials --
// this is what "default tests make no paid calls" means in practice for
// this script specifically.
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createPool } from "../../src/db/pool.js";
import { loadMysqlConfig } from "../../src/config.js";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { Effect } from "effect";
import { createFixtureRepo } from "../../src/worker/fixture-repo.js";
import { runAiRepairTask } from "../../src/worker/run-task-ai.js";
import { createManagedWorkspaceRoot } from "../../src/worker/workspace.js";
import { uniqueId } from "./helpers.js";

const liveOptIn = process.argv.includes("--live") || process.env.RELAYLAB_AI_REPAIR_LIVE === "1";

async function main() {
  if (!liveOptIn) {
    console.log("[demo-ai-repair] NOT making a live provider call -- pass --live or set RELAYLAB_AI_REPAIR_LIVE=1 to opt in.");
    console.log("[demo-ai-repair] This is the required opt-in boundary for a real, paid Anthropic API call -- see README.md 'AI repair mode'.");
    return;
  }

  const demoRoot = await mkdtemp(path.join(tmpdir(), "relaylab-ai-repair-demo-"));
  const fixtureDir = path.join(demoRoot, "fixture-repo");
  const fixture = await createFixtureRepo(fixtureDir);
  console.log(`[demo-ai-repair] fixture repository created at ${fixtureDir}`);
  console.log(`[demo-ai-repair] baseline (buggy) commit: ${fixture.baseCommit}`);

  const managedWorkspaceRoot = await createManagedWorkspaceRoot(demoRoot);
  const pool = createPool(loadMysqlConfig());
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("ai-repair-demo-task");
  const ownerId = `ai-repair-demo-${process.pid}`;

  try {
    const claim = await Effect.runPromise(store.claimTask(taskId, ownerId, 120_000));
    console.log(`[demo-ai-repair] claimed task "${taskId}" as "${ownerId}", epoch ${claim.epoch}`);

    const { outcome, attempts } = await runAiRepairTask({
      taskId,
      ownerId,
      epoch: claim.epoch,
      sourceRepoDir: fixtureDir,
      requestedCommitRef: fixture.baseCommit,
      managedWorkspaceRoot,
      pool,
    });

    for (const a of attempts) {
      console.log(`[demo-ai-repair] attempt ${a.attempt}: model=${a.modelUsed ?? "n/a"} outcome=${a.outcome}${a.reason ? ` reason=${a.reason}` : ""}${a.usage ? ` usage=${JSON.stringify(a.usage)}` : ""}`);
    }

    if (outcome.kind !== "checkpointed") {
      console.error(`[demo-ai-repair] FAILED: worker did not checkpoint (${outcome.kind})`);
      process.exitCode = 1;
      return;
    }
    console.log(`[demo-ai-repair] PASS: AI-generated edit, real Docker verification, real durable checkpoint.`);
    console.log(`[demo-ai-repair] resultCommit=${outcome.manifest.resultCommit}`);
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error("[demo-ai-repair] fatal:", err);
  process.exitCode = 1;
});
