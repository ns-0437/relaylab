// Phase 2A: reproducible demo -- `npm run demo:git-worker`. Creates a
// fresh, disposable fixture repository, claims a real task against the
// dev database, and runs the real worker end to end: worktree creation,
// the one scripted repair, real verification, a real local commit, and
// a durable checkpoint. Prints actual base/result commit and tree IDs,
// not placeholders.
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createFixtureRepo } from "../../src/worker/fixture-repo.js";
import { runOnce } from "../../src/worker-main.js";
import { uniqueId } from "./helpers.js";

async function main() {
  const demoRoot = await mkdtemp(path.join(tmpdir(), "relaylab-git-worker-demo-"));
  const fixtureDir = path.join(demoRoot, "fixture-repo");
  const fixture = await createFixtureRepo(fixtureDir);
  console.log(`[demo] fixture repository created at ${fixtureDir}`);
  console.log(`[demo] baseline (buggy) commit: ${fixture.baseCommit}`);

  const taskId = uniqueId("git-worker-demo-task");
  const outcome = await runOnce({ taskId, sourceRepoDir: fixtureDir, requestedCommitRef: fixture.baseCommit });

  if (outcome.kind !== "checkpointed") {
    console.error(`[demo] FAILED: worker did not checkpoint (${outcome.kind})`);
    process.exitCode = 1;
    return;
  }
  console.log("[demo] PASS: real worktree, real scripted repair, real verification, real commit, real durable checkpoint.");
}

main().catch((err) => {
  console.error("[demo] fatal:", err);
  process.exitCode = 1;
});
