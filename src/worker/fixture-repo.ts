// Phase 2A: creates the DISPOSABLE fixture repository this milestone's
// worker operates on -- a known implementation defect, a test that fails
// because of it, and nothing else. Used by both the test suite and the
// reproducible demo command (see package.json `demo:git-worker`). This
// is test/demo INFRASTRUCTURE, not a production code path -- no
// production code ever calls into this module.
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { git, makeIsolatedGitEnv } from "./git.js";

const ADD_JS_BUGGY = `function add(a, b) {\n  return a - b;\n}\n\nmodule.exports = { add };\n`;

const VERIFY_JS = `const assert = require("node:assert/strict");\nconst { add } = require("./src/add.js");\n\nassert.equal(add(2, 3), 5, "add(2, 3) must equal 5");\nconsole.log("verify.js: PASS");\n`;

export interface FixtureRepo {
  repoDir: string;
  /** The commit SHA containing the KNOWN DEFECT (add.js's subtraction bug) -- the base commit tasks in this milestone's tests/demo start their worktree from. */
  baseCommit: string;
}

/** Creates a brand-new, real Git repository at `destDir` (caller-chosen, must not already exist) with the known defect committed. */
export async function createFixtureRepo(destDir: string): Promise<FixtureRepo> {
  await mkdir(destDir, { recursive: true });
  const isolatedHome = path.join(destDir, ".fixture-home");
  await mkdir(isolatedHome, { recursive: true });
  const env = makeIsolatedGitEnv(isolatedHome);

  await git(["init", "--initial-branch=main"], destDir, env);
  await mkdir(path.join(destDir, "src"), { recursive: true });
  await writeFile(path.join(destDir, "src", "add.js"), ADD_JS_BUGGY, "utf8");
  await writeFile(path.join(destDir, "verify.js"), VERIFY_JS, "utf8");

  await git(["add", "-A"], destDir, env);
  await git(
    ["-c", "user.name=relaylab-fixture", "-c", "user.email=relaylab-fixture@localhost", "-c", "commit.gpgsign=false", "commit", "--no-verify", "-m", "baseline: add() has a known subtraction bug"],
    destDir,
    env,
  );
  const { stdout } = await git(["rev-parse", "HEAD"], destDir, env);
  return { repoDir: destDir, baseCommit: stdout.trim() };
}
