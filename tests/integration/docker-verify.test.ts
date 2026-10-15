// Phase 2C, Part 6: dedicated coverage for the Docker verification
// adapter (src/worker/docker-verify.ts) against a REAL, reachable
// Docker daemon -- no mocking of `docker` itself anywhere in this file.
// If Docker is unreachable or the pinned verification image has not
// been built, `before()` below fails LOUDLY (throws, naming the exact
// missing prerequisite and the command to fix it) rather than silently
// skipping these tests or substituting a stand-in -- see CLAUDE.md "How
// to distinguish simulated behavior from real integrations."
//
// Does NOT require MySQL/NATS (this file never touches TaskStore) --
// can run standalone via `npm run test:docker-verify`, and is also
// picked up by `npm run test:integration`'s glob.
import { test, before, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  VERIFY_IMAGE_TAG,
  DOCKER_VERIFY_DEFAULTS,
  buildVerifyCreateArgs,
  materializeCandidateInput,
  reconcileAbandonedExecution,
  resolveVerifyImageId,
  verifyInDocker,
  verifyInDockerWithImageId,
} from "../../src/worker/docker-verify.js";
import { MAX_CAPTURED_OUTPUT_BYTES, deterministicExecutionName } from "../../src/worker/verification.js";
import { createFixtureRepo } from "../../src/worker/fixture-repo.js";
import { applyScriptedRepair } from "../../src/worker/repair.js";
import { commitAllowedPaths, makeIsolatedGitEnv } from "../../src/worker/git.js";
import { uniqueId } from "./helpers.js";

const execFileAsync = promisify(execFile);

async function dockerInspectOrNull(nameOrId: string, format: string): Promise<string | null> {
  try {
    const { stdout } = await execFileAsync("docker", ["inspect", nameOrId, "--format", format]);
    return stdout.trim();
  } catch {
    return null;
  }
}

/** Windows ships a real `tar.exe` (bsdtar) at this fixed path -- used here purely as a TEST-fixture-authoring convenience (these ad hoc scripts have no exec-bit/mode-fidelity concern of their own; that is covered separately, against the real `materializeCandidateInput` path, in the "exact-input fidelity" tests below). Non-Windows hosts use the system `tar` on PATH. */
function hostTarBinaryForTestFixtures(): string {
  if (process.platform === "win32") {
    return path.join(process.env.SystemRoot ?? "C:\\Windows", "System32", "tar.exe");
  }
  return "tar";
}

/**
 * Phase 2C.1: `verifyInDocker`'s `request.inputDir` must now be a
 * directory containing exactly one file, `candidate.tar` (matching
 * `materializeCandidateInput`'s own output shape) -- NOT a bare
 * `verify.js` file directly. This helper builds that shape directly
 * (staging the script, then archiving it) for the ad hoc,
 * non-git-identity scenarios below (timeout, cancellation, noisy
 * output, write-rejection, network isolation) that don't need a real
 * Git repository at all.
 */
async function writeVerifyScript(dir: string, content: string): Promise<string> {
  const stagingDir = path.join(dir, "staging");
  await mkdir(stagingDir, { recursive: true });
  await writeFile(path.join(stagingDir, "verify.js"), content, "utf8");
  await execFileAsync(hostTarBinaryForTestFixtures(), ["-cf", path.join(dir, "candidate.tar"), "-C", stagingDir, "verify.js"]);
  return dir;
}

let scratchRoot: string;

before(async () => {
  try {
    await execFileAsync("docker", ["version", "--format", "{{.Server.Version}}"]);
  } catch (err) {
    throw new Error(
      `docker-verify.test.ts requires a real, reachable Docker daemon -- none found ` +
        `(${err instanceof Error ? err.message : String(err)}). This suite intentionally fails loudly ` +
        `rather than substituting a mock for Docker -- start Docker Desktop and retry.`,
    );
  }
  try {
    await resolveVerifyImageId(VERIFY_IMAGE_TAG);
  } catch (err) {
    throw new Error(
      `docker-verify.test.ts requires the pinned verification image "${VERIFY_IMAGE_TAG}" to already be ` +
        `built locally (this adapter never implicitly builds/pulls it) -- run "npm run docker:build-verify-image" ` +
        `first. (${err instanceof Error ? err.message : String(err)})`,
    );
  }
});

beforeEach(async () => {
  scratchRoot = await mkdtemp(path.join(tmpdir(), "relaylab-docker-verify-test-"));
});

afterEach(async () => {
  await rm(scratchRoot, { recursive: true, force: true });
});

test("docker verify: the existing deterministic fixture FAILS before the repair, with the real assertion failure captured", async () => {
  const { repoDir, baseCommit } = await createFixtureRepo(path.join(scratchRoot, "fixture-repo"));
  const env = makeIsolatedGitEnv(path.join(scratchRoot, ".home"));
  const materialized = await materializeCandidateInput(repoDir, baseCommit, env, path.join(scratchRoot, ".verify-scratch"));
  try {
    const result = await verifyInDocker({ inputDir: materialized.exportDir, taskId: uniqueId("task-docker-verify-fail"), attemptId: "attempt-1" });
    assert.equal(result.outcome, "verification_failed");
    assert.notEqual(result.exitCode, 0);
    assert.match(result.stdout + result.stderr, /add\(2, 3\) must equal 5/);
    assert.equal(result.adapterName, "docker");
    assert.ok(result.executionId.length > 0);
  } finally {
    await materialized.cleanup();
  }
});

test("docker verify: the existing deterministic fixture PASSES after the scripted repair is applied and committed, with the real image ID recorded", async () => {
  const { repoDir } = await createFixtureRepo(path.join(scratchRoot, "fixture-repo"));
  const env = makeIsolatedGitEnv(path.join(scratchRoot, ".home"));
  await applyScriptedRepair(repoDir);
  const repairedCommit = await commitAllowedPaths(repoDir, ["src/add.js"], "repair: fix add() subtraction bug", env);
  const materialized = await materializeCandidateInput(repoDir, repairedCommit, env, path.join(scratchRoot, ".verify-scratch"));
  try {
    const expectedImageId = await resolveVerifyImageId(VERIFY_IMAGE_TAG);
    const { result, imageId } = await verifyInDockerWithImageId({ inputDir: materialized.exportDir, taskId: uniqueId("task-docker-verify-pass"), attemptId: "attempt-1" });
    assert.equal(result.outcome, "passed");
    assert.equal(result.exitCode, 0);
    assert.match(result.stdout, /verify\.js: PASS/);
    assert.equal(imageId, expectedImageId, "the ACTUAL resolved image ID must be recorded, never an unrecorded mutable tag");
    assert.ok(imageId?.startsWith("sha256:"));
  } finally {
    await materialized.cleanup();
  }
});

test("docker verify: a long-running fixture is stopped and reported as timed_out, not left running for its full duration", async () => {
  const inputDir = await writeVerifyScript(path.join(scratchRoot, "input"), `setTimeout(() => {}, 60000);\nconsole.log("still running");\n`);
  const startedAt = Date.now();
  const result = await verifyInDocker({ inputDir, taskId: uniqueId("task-docker-verify-timeout"), attemptId: "attempt-1", timeoutMs: 1500 });
  const wallClockMs = Date.now() - startedAt;
  assert.equal(result.outcome, "timed_out");
  assert.equal(result.exitCode, null);
  // Bounded by timeout + stop-grace + a generous margin for Docker CLI
  // overhead -- proof the container was actually stopped, not left
  // running for anywhere near its own 60s sleep.
  assert.ok(wallClockMs < 1500 + 5_000 + 10_000, `expected to return well before the fixture's own 60s sleep, took ${wallClockMs}ms`);
  const stillExists = await dockerInspectOrNull(result.executionId, "{{.Id}}");
  assert.equal(stillExists, null, "the container must have been removed after verification completed");
});

test("docker verify: cancellation via AbortSignal stops the container the same way a timeout does", async () => {
  const inputDir = await writeVerifyScript(path.join(scratchRoot, "input"), `setTimeout(() => {}, 60000);\n`);
  const controller = new AbortController();
  const startedAt = Date.now();
  const resultPromise = verifyInDocker({ inputDir, taskId: uniqueId("task-docker-verify-cancel"), attemptId: "attempt-1", timeoutMs: 60_000, signal: controller.signal });
  setTimeout(() => controller.abort(), 1_000);
  const result = await resultPromise;
  const wallClockMs = Date.now() - startedAt;
  assert.equal(result.outcome, "cancelled");
  assert.equal(result.exitCode, null);
  assert.ok(wallClockMs < 1_000 + 5_000 + 10_000, `expected cancellation to stop the container promptly, took ${wallClockMs}ms`);
  const stillExists = await dockerInspectOrNull(result.executionId, "{{.Id}}");
  assert.equal(stillExists, null, "the container must have been removed after cancellation");
});

test("docker verify: output from a noisy fixture is bounded, never captured without limit", async () => {
  const inputDir = await writeVerifyScript(
    path.join(scratchRoot, "input"),
    `for (let i = 0; i < 200000; i++) { console.log("x".repeat(100)); }\nprocess.exit(0);\n`,
  );
  const result = await verifyInDocker({ inputDir, taskId: uniqueId("task-docker-verify-noisy"), attemptId: "attempt-1" });
  assert.equal(result.stdoutTruncated, true);
  assert.ok(Buffer.byteLength(result.stdout, "utf8") <= MAX_CAPTURED_OUTPUT_BYTES, "captured stdout must never exceed the documented cap");
});

test("docker verify: a NAIVE write to the extracted verification input is rejected (EACCES, via chmod a-w after Linux-side extraction) -- permission-based, not an immutability guarantee (see the companion test below)", async () => {
  // Phase 2C.1: the verification input is now extracted onto the
  // container's own tmpfs (necessary for exec-bit fidelity -- see
  // "exact-input fidelity" tests below) and then made read-only via
  // `chmod -R a-w` BEFORE the verified command ever runs, so a
  // straightforward write attempt (the kind a correctly-behaving, or
  // accidentally-buggy, fixed verification command would make) fails
  // -- now with EACCES (permission denied), not EROFS (the
  // pre-Phase-2C.1 directory-bind-mount's errno) -- same observable
  // guarantee for THIS failure mode, different underlying mechanism.
  // This is PERMISSION-based protection, not ownership-based -- see
  // the companion test immediately below for what that distinction
  // actually means and does not mean.
  const script = [
    `const fs = require("node:fs");`,
    `try {`,
    `  fs.writeFileSync("evil.txt", "should not be allowed");`,
    `  console.log("WRITE SUCCEEDED (BUG)");`,
    `  process.exit(1);`,
    `} catch (err) {`,
    `  console.log("WRITE REJECTED: " + err.code);`,
    `  process.exit(1);`,
    `}`,
    ``,
  ].join("\n");
  const inputDir = await writeVerifyScript(path.join(scratchRoot, "input"), script);
  const result = await verifyInDocker({ inputDir, taskId: uniqueId("task-docker-verify-rofs"), attemptId: "attempt-1" });
  assert.equal(result.outcome, "verification_failed");
  assert.match(result.stdout, /WRITE REJECTED: EACCES/);
  assert.doesNotMatch(result.stdout, /WRITE SUCCEEDED/);
});

test("docker verify correctness patch, Part 2 (documented limitation, not a regression): `chmod -R a-w` is PERMISSION-based, not ownership-based -- the verification user OWNS the extracted copy and can chmod it back to writable and tamper with it", async () => {
  // CONFIRMED empirically (see docs/EXECUTION.md "Actual input-
  // protection guarantee" for the exact probe output): the extraction
  // step runs as the SAME non-root `node` user (uid 1000) that then
  // runs the verified command, because `docker create --user node`
  // applies to the ENTIRE container, including the bootstrap's own
  // `tar -xf`/`chmod` steps. `chmod()`'s permission check is "you own
  // the file, or you hold CAP_FOWNER" -- NOT "the file's current mode
  // bits allow it" -- so the OWNER of a file can always chmod it back
  // to writable regardless of its current permission bits. Since
  // extraction and verification run as the SAME owning user here,
  // `chmod -R a-w` is real protection against an ACCIDENTAL/naive
  // write (the companion test above) but provides NO protection
  // against the verification command's own code deliberately
  // reasserting write access to files it owns.
  //
  // A privilege-separated alternative (extract as root, drop to
  // `node` via `su` for the actual verified command, so `node` would
  // NOT own the extracted files and would lack CAP_FOWNER to bypass
  // that) was investigated and rejected: `su` requires
  // `CAP_SETUID`/`CAP_SETGID`, which conflicts with this project's
  // already-established, independently tested `--cap-drop ALL`
  // isolation property (confirmed: `su node -c ...` under
  // `--cap-drop ALL` fails outright with "can't set groups: Operation
  // not permitted"). Re-adding those two capabilities just to enable
  // `su` would trade one tested isolation property for a DIFFERENT,
  // narrower one, for a threat (a deliberately self-tampering
  // verification command) outside this project's documented
  // threat model -- the verification command is FIXED and
  // project-controlled, never task-supplied (see CLAUDE.md "Non-
  // goals" and docs/EXECUTION.md "What this does not claim": this is
  // a local-prototype isolation boundary, not a defense against
  // hostile code).
  //
  // This test exists so that fact is a permanent, regression-tested
  // part of this codebase's documented behavior -- never silently
  // re-claimed as "immutable" by a future reader who only sees the
  // `chmod -R a-w` line and assumes it is sufficient.
  //
  // Phase 3A, Part A: the raw filesystem-permission bypass above is
  // STILL possible (this test still proves that) -- but the ADAPTER
  // now independently DETECTS it afterward (`verifyInputIntegrity`,
  // via `docker cp` + a byte-for-byte comparison against the ORIGINAL
  // candidate.tar) and refuses to report this run as `passed`, even
  // though the script exits 0 claiming success. This is the fixture
  // Part A's own instruction asked for ("a fixture that changes
  // tracked input and exits zero") -- see
  // tests/integration/git-worker.test.ts and
  // tests/integration/git-worker-handoff.test.ts for the end-to-end
  // proof that this also prevents a checkpoint (A) / acknowledgement
  // (B) from ever being recorded.
  const script = [
    `const fs = require("node:fs");`,
    `const before = fs.readFileSync("a.txt", "utf8");`,
    `let tamperedFile = false;`,
    `try {`,
    `  fs.chmodSync("a.txt", 0o644);`,
    `  fs.writeFileSync("a.txt", "TAMPERED CONTENT");`,
    `  tamperedFile = fs.readFileSync("a.txt", "utf8") === "TAMPERED CONTENT";`,
    `} catch {}`,
    `let replacedViaDirectory = false;`,
    `try {`,
    `  fs.chmodSync(".", 0o755);`,
    `  fs.unlinkSync("a.txt");`,
    `  fs.writeFileSync("a.txt", "REPLACED");`,
    `  replacedViaDirectory = fs.readFileSync("a.txt", "utf8") === "REPLACED";`,
    `} catch {}`,
    `console.log("TAMPER_RESULT:" + JSON.stringify({ before, tamperedFile, replacedViaDirectory }));`,
    `process.exit(0);`,
    ``,
  ].join("\n");
  const dir = path.join(scratchRoot, "input");
  await mkdir(path.join(dir, "staging"), { recursive: true });
  await writeFile(path.join(dir, "staging", "verify.js"), script, "utf8");
  await writeFile(path.join(dir, "staging", "a.txt"), "original content", "utf8");
  await execFileAsync(hostTarBinaryForTestFixtures(), ["-cf", path.join(dir, "candidate.tar"), "-C", path.join(dir, "staging"), "verify.js", "a.txt"]);

  const result = await verifyInDocker({ inputDir: dir, taskId: uniqueId("task-docker-verify-tamper"), attemptId: "attempt-1" });
  // The raw process EXITED 0 -- captured via stdout below, proving the
  // tamper itself still succeeds at the filesystem-permission level --
  // but the ADAPTER's own integrity check must have overridden the
  // reported outcome. "Do not accept verification success when the
  // final input differs."
  const match = result.stdout.match(/TAMPER_RESULT:(\{.*\})/);
  assert.ok(match, `expected a TAMPER_RESULT line in stdout, got:\n${result.stdout}`);
  const tamper = JSON.parse(match![1]!) as { before: string; tamperedFile: boolean; replacedViaDirectory: boolean };
  assert.equal(tamper.before, "original content", "test setup check: the file must start with its original content");
  assert.equal(tamper.tamperedFile, true, "documented limitation: the verification user CAN chmod its own extracted file back to writable and alter it -- this is PERMISSION-based protection, not immutability");
  assert.equal(tamper.replacedViaDirectory, true, "documented limitation: the verification user CAN also chmod the containing directory back to writable and replace the file entirely");

  assert.equal(result.outcome, "verification_failed", `Phase 3A Part A: the integrity check must catch this and refuse to report "passed" even though the script exited 0; stderr: ${result.stderr}`);
  assert.match(result.stderr, /integrity check.*refusing to accept/i, "the integrity-check failure reason must be observable in stderr");
  assert.match(result.stderr, /CHANGED BYTES: "a\.txt"/, "the specific changed path must be named");
});

test("docker verify: network access is disabled during verification", async () => {
  const script = [
    `const dns = require("node:dns");`,
    `dns.lookup("example.com", (err) => {`,
    `  if (err) {`,
    `    console.log("DNS LOOKUP FAILED AS EXPECTED: " + err.code);`,
    `    process.exit(1);`,
    `  } else {`,
    `    console.log("DNS LOOKUP SUCCEEDED (BUG)");`,
    `    process.exit(1);`,
    `  }`,
    `});`,
    ``,
  ].join("\n");
  const inputDir = await writeVerifyScript(path.join(scratchRoot, "input"), script);
  const result = await verifyInDocker({ inputDir, taskId: uniqueId("task-docker-verify-net"), attemptId: "attempt-1", timeoutMs: 8_000 });
  assert.equal(result.outcome, "verification_failed");
  assert.match(result.stdout, /DNS LOOKUP FAILED AS EXPECTED/);
  assert.doesNotMatch(result.stdout, /SUCCEEDED \(BUG\)/);
});

test("docker verify: the actual container is created with the documented non-root/read-only/network/capability/resource configuration (docker inspect evidence)", async () => {
  const inputDir = await writeVerifyScript(path.join(scratchRoot, "input"), `console.log("verify.js: PASS");\nprocess.exit(0);\n`);
  const name = deterministicExecutionName(uniqueId("task-docker-verify-config"), "attempt-1", "config-probe");
  const imageId = await resolveVerifyImageId(VERIFY_IMAGE_TAG);
  const createArgs = buildVerifyCreateArgs({ name, inputDir, ...DOCKER_VERIFY_DEFAULTS });
  const { stdout: containerIdRaw } = await execFileAsync("docker", createArgs);
  const containerId = containerIdRaw.trim();
  try {
    const inspect = (format: string) => dockerInspectOrNull(containerId, format);
    assert.equal(await inspect("{{.Image}}"), imageId, "must run the resolved image ID, not a mutable tag reference");
    assert.equal(await inspect("{{.HostConfig.ReadonlyRootfs}}"), "true");
    assert.equal(await inspect("{{.HostConfig.NetworkMode}}"), "none");
    assert.equal(await inspect("{{.Config.User}}"), "node");
    assert.equal(await inspect("{{.HostConfig.CapDrop}}"), "[ALL]");
    assert.match((await inspect("{{.HostConfig.SecurityOpt}}")) ?? "", /no-new-privileges/);
    assert.equal(await inspect("{{.HostConfig.PidsLimit}}"), String(DOCKER_VERIFY_DEFAULTS.pidsLimit));
    assert.notEqual(await inspect("{{.HostConfig.Memory}}"), "0", "memory limit must be set, not unlimited");
    assert.notEqual(await inspect("{{.HostConfig.NanoCpus}}"), "0", "CPU limit must be set, not unlimited");
    const mounts = await inspect("{{json .Mounts}}");
    assert.match(mounts ?? "", /"RW":false/, "the verification input mount must be read-only");
  } finally {
    await execFileAsync("docker", ["rm", "-f", containerId]).catch(() => {});
  }
});

test("docker verify: cleanup is scoped to this execution only -- an unrelated running container is never touched", async () => {
  const unrelatedInputDir = await writeVerifyScript(path.join(scratchRoot, "unrelated-input"), `setTimeout(() => {}, 60000);\n`);
  const unrelatedName = `relaylab-verify-unrelated-${uniqueId("x")}`;
  const unrelatedCreateArgs = buildVerifyCreateArgs({ name: unrelatedName, inputDir: unrelatedInputDir, ...DOCKER_VERIFY_DEFAULTS });
  const { stdout: unrelatedIdRaw } = await execFileAsync("docker", unrelatedCreateArgs);
  const unrelatedId = unrelatedIdRaw.trim();
  await execFileAsync("docker", ["start", unrelatedId]);
  try {
    const inputDir = await writeVerifyScript(path.join(scratchRoot, "input"), `console.log("verify.js: PASS");\nprocess.exit(0);\n`);
    const result = await verifyInDocker({ inputDir, taskId: uniqueId("task-docker-verify-scoped"), attemptId: "attempt-1" });
    assert.equal(result.outcome, "passed");
    const stillRunning = await dockerInspectOrNull(unrelatedId, "{{.State.Running}}");
    assert.equal(stillRunning, "true", "an unrelated container must never be affected by this execution's cleanup");
  } finally {
    await execFileAsync("docker", ["rm", "-f", unrelatedId]).catch(() => {});
  }
});

test("docker verify: restart reconciliation removes exactly the prior abandoned container for the same (taskId, attemptId, role) before creating a fresh one", async () => {
  const taskId = uniqueId("task-docker-verify-reconcile");
  const attemptId = "attempt-1";
  const name = deterministicExecutionName(taskId, attemptId, "run");
  const abandonedInputDir = await writeVerifyScript(path.join(scratchRoot, "abandoned-input"), `setTimeout(() => {}, 60000);\n`);
  const abandonedArgs = buildVerifyCreateArgs({ name, inputDir: abandonedInputDir, ...DOCKER_VERIFY_DEFAULTS });
  const { stdout: abandonedIdRaw } = await execFileAsync("docker", abandonedArgs);
  const abandonedId = abandonedIdRaw.trim();
  await execFileAsync("docker", ["start", abandonedId]);

  const passingInputDir = await writeVerifyScript(path.join(scratchRoot, "input"), `console.log("verify.js: PASS");\nprocess.exit(0);\n`);
  const result = await verifyInDocker({ inputDir: passingInputDir, taskId, attemptId });
  assert.equal(result.outcome, "passed", "a container-name collision with an abandoned execution must not prevent a fresh verification from succeeding");
  assert.notEqual(result.executionId, abandonedId, "the fresh execution must be a genuinely NEW container, not the abandoned one");
  const abandonedStillExists = await dockerInspectOrNull(abandonedId, "{{.Id}}");
  assert.equal(abandonedStillExists, null, "the abandoned container must have been removed by restart reconciliation");
});

test("docker verify: an unresolvable image is reported as infrastructure_failure, never a verification success", async () => {
  const inputDir = await writeVerifyScript(path.join(scratchRoot, "input"), `console.log("verify.js: PASS");\nprocess.exit(0);\n`);
  const result = await verifyInDocker(
    { inputDir, taskId: uniqueId("task-docker-verify-badimage"), attemptId: "attempt-1" },
    { imageTag: "relaylab-verify:this-tag-does-not-exist" },
  );
  assert.equal(result.outcome, "infrastructure_failure");
  assert.equal(result.exitCode, null);
});

test("docker verify: a docker-create-level launch failure is also reported as infrastructure_failure, never a verification success", async () => {
  const inputDir = await writeVerifyScript(path.join(scratchRoot, "input"), `console.log("verify.js: PASS");\nprocess.exit(0);\n`);
  const result = await verifyInDocker(
    { inputDir, taskId: uniqueId("task-docker-verify-badcreate"), attemptId: "attempt-1" },
    { memoryLimit: "not-a-valid-memory-value" },
  );
  assert.equal(result.outcome, "infrastructure_failure");
  assert.equal(result.exitCode, null);
});

test("docker verify: reconcileAbandonedExecution is a no-op (never throws) when no container with that name exists", async () => {
  const name = deterministicExecutionName(uniqueId("task-docker-verify-noop-reconcile"), "attempt-1", "run");
  await reconcileAbandonedExecution(name); // must not throw
  const exists = await dockerInspectOrNull(name, "{{.Id}}");
  assert.equal(exists, null);
});
