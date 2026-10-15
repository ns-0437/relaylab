// Phase 2C correctness patch, Part 1: dedicated, focused coverage for
// `reconcileAbandonedExecution`'s cleanup CONTRACT -- the pre-patch
// version collapsed every `docker inspect` error into "absent" and
// silently swallowed every stop/remove failure, which could let a
// caller proceed to `docker create` believing reconciliation succeeded
// when the true resource state was simply unknown. Does NOT re-test
// the Docker isolation/behavioral properties already covered by
// docker-verify.test.ts.
//
// "Container already absent" and "successful cleanup" are exercised
// against a REAL Docker daemon (no fakes) -- both are realistic and
// safe to produce for real. "Stop failure followed by successful
// removal," "removal failure," and "daemon unavailable" are exercised
// via CONTROLLED FAULT INJECTION -- a fake `DockerExec` the function
// accepts as an explicit, test-only injection seam (`deps.exec`),
// never by restarting or otherwise disrupting the user's real Docker
// Desktop installation. One test additionally drives a REAL `docker`
// CLI call against a deliberately invalid `DOCKER_HOST` (the SAME
// technique used to empirically confirm the real daemon-unavailable
// error text during this patch's own investigation) to prove the
// classifier handles genuine CLI output, not just a hand-crafted
// string.
import { test, before } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  VERIFY_IMAGE_TAG,
  DOCKER_VERIFY_DEFAULTS,
  ReconciliationFailed,
  buildVerifyCreateArgs,
  reconcileAbandonedExecution,
  resolveVerifyImageId,
  type DockerExec,
} from "../../src/worker/docker-verify.js";
import { uniqueId } from "./helpers.js";

const execFileAsync = promisify(execFile);

before(async () => {
  try {
    await execFileAsync("docker", ["version", "--format", "{{.Server.Version}}"]);
  } catch (err) {
    throw new Error(
      `docker-reconcile.test.ts requires a real, reachable Docker daemon -- none found (${err instanceof Error ? err.message : String(err)}). ` +
        `This suite fails loudly rather than substituting a mock for Docker -- start Docker Desktop and retry.`,
    );
  }
  try {
    await resolveVerifyImageId(VERIFY_IMAGE_TAG);
  } catch (err) {
    throw new Error(
      `docker-reconcile.test.ts requires the pinned verification image "${VERIFY_IMAGE_TAG}" to already be built -- run "npm run docker:build-verify-image" first. (${err instanceof Error ? err.message : String(err)})`,
    );
  }
});

async function dockerInspectOrNull(nameOrId: string, format: string): Promise<string | null> {
  try {
    const { stdout } = await execFileAsync("docker", ["inspect", nameOrId, "--format", format]);
    return stdout.trim();
  } catch {
    return null;
  }
}

function tarBinary(): string {
  if (process.platform === "win32") {
    return path.join(process.env.SystemRoot ?? "C:\\Windows", "System32", "tar.exe");
  }
  return "tar";
}

/** Creates and starts a REAL, long-running container under `name`, standing in for a worker's genuinely abandoned execution. */
async function createRunningContainer(scratchRoot: string, name: string): Promise<string> {
  const stagingDir = path.join(scratchRoot, "staging");
  await mkdir(stagingDir, { recursive: true });
  await writeFile(path.join(stagingDir, "verify.js"), "setTimeout(() => {}, 60000);\n", "utf8");
  const inputDir = path.join(scratchRoot, "input");
  await mkdir(inputDir, { recursive: true });
  await execFileAsync(tarBinary(), ["-cf", path.join(inputDir, "candidate.tar"), "-C", stagingDir, "verify.js"]);
  const createArgs = buildVerifyCreateArgs({ name, inputDir, ...DOCKER_VERIFY_DEFAULTS });
  const { stdout } = await execFileAsync("docker", createArgs);
  const id = stdout.trim();
  await execFileAsync("docker", ["start", id]);
  return id;
}

test("reconcileAbandonedExecution: a genuinely absent container is a no-op (real Docker, never throws)", async () => {
  const name = `relaylab-verify-reconcile-absent-${uniqueId("x")}`;
  await reconcileAbandonedExecution(name); // must not throw
  assert.equal(await dockerInspectOrNull(name, "{{.Id}}"), null, "test setup check: this name must genuinely not exist");
});

test("reconcileAbandonedExecution: a real running abandoned container is stopped, removed, and the final state is independently confirmed (real Docker, successful cleanup)", async () => {
  const scratchRoot = await mkdtemp(path.join(tmpdir(), "relaylab-reconcile-test-"));
  try {
    const name = `relaylab-verify-reconcile-success-${uniqueId("x")}`;
    const containerId = await createRunningContainer(scratchRoot, name);
    assert.equal(await dockerInspectOrNull(containerId, "{{.State.Running}}"), "true", "test setup check: the container must actually be running before reconciliation");

    await reconcileAbandonedExecution(name); // must not throw

    assert.equal(await dockerInspectOrNull(name, "{{.Id}}"), null, "the container must be CONFIRMED gone after successful reconciliation, not merely assumed");
  } finally {
    await rm(scratchRoot, { recursive: true, force: true });
  }
});

test("reconcileAbandonedExecution: a stop failure is recovered by a successful forced removal (controlled fault injection)", async () => {
  const name = "relaylab-verify-fake-stop-then-remove";
  let removed = false;
  const fakeExec: DockerExec = async (_command, args) => {
    const a = args as string[];
    const [sub, ...rest] = a;
    if (sub === "inspect") {
      if (removed) {
        throw Object.assign(new Error("absent"), { stderr: `error: no such object: ${name}` });
      }
      const format = rest[rest.length - 1];
      // The post-"stop" running-check reports the container as no
      // longer running (modeling: the `docker stop` CLI invocation
      // itself errored, but the container had, in fact, already
      // stopped by the time anyone checked) -- `stopContainerAndWait`
      // never needs to escalate to `kill` in this scenario.
      if (format === "{{.State.Running}}") return { stdout: "false\n", stderr: "" };
      return { stdout: "fake-container-id\n", stderr: "" }; // {{.Id}} -- still present until removed
    }
    if (sub === "stop") {
      throw Object.assign(new Error("stop failed"), { stderr: "Error response from daemon: cannot stop container: a transient failure" });
    }
    if (sub === "rm") {
      removed = true;
      return { stdout: "", stderr: "" };
    }
    throw new Error(`unexpected docker call in test: ${a.join(" ")}`);
  };

  await reconcileAbandonedExecution(name, { exec: fakeExec }); // must NOT throw -- the forced removal recovered it
  assert.equal(removed, true, "test sanity check: the fake's rm branch must actually have been reached");
});

test("reconcileAbandonedExecution: a removal failure that leaves the container present produces an observable, typed ReconciliationFailed (controlled fault injection)", async () => {
  const name = "relaylab-verify-fake-remove-fails";
  const fakeExec: DockerExec = async (_command, args) => {
    const a = args as string[];
    const [sub, ...rest] = a;
    if (sub === "inspect") {
      const format = rest[rest.length - 1];
      if (format === "{{.State.Running}}") return { stdout: "false\n", stderr: "" };
      return { stdout: "fake-container-id\n", stderr: "" }; // ALWAYS present -- removal never actually worked
    }
    if (sub === "stop") return { stdout: "", stderr: "" };
    if (sub === "rm") {
      throw Object.assign(new Error("remove failed"), { stderr: "Error response from daemon: device or resource busy" });
    }
    throw new Error(`unexpected docker call in test: ${a.join(" ")}`);
  };

  await assert.rejects(
    () => reconcileAbandonedExecution(name, { exec: fakeExec }),
    (err: unknown) => {
      assert.ok(err instanceof ReconciliationFailed, `expected a ReconciliationFailed, got ${err}`);
      assert.match((err as Error).message, /still present/i);
      return true;
    },
  );
});

test("reconcileAbandonedExecution: a daemon-unavailable error on the INITIAL inspect is never treated as absence -- produces a typed failure, never a silent proceed (controlled fault injection)", async () => {
  const name = "relaylab-verify-fake-daemon-down-initial";
  const fakeExec: DockerExec = async () => {
    throw Object.assign(new Error("unavailable"), {
      stderr:
        "failed to connect to the docker API at npipe:////./pipe/relaylab_fake_pipe; check if the path is correct and if the daemon is running: open //./pipe/relaylab_fake_pipe: The system cannot find the file specified.",
    });
  };

  await assert.rejects(
    () => reconcileAbandonedExecution(name, { exec: fakeExec }),
    (err: unknown) => {
      assert.ok(err instanceof ReconciliationFailed, `expected a ReconciliationFailed, got ${err}`);
      assert.match((err as Error).message, /could not confirm whether a prior execution exists/i);
      return true;
    },
  );
});

test("reconcileAbandonedExecution: a REAL docker CLI call against a deliberately invalid DOCKER_HOST is classified as daemon-unavailable, never absent (real CLI, controlled fault injection -- never the user's real Docker installation)", async () => {
  const name = "relaylab-verify-real-bad-docker-host";
  const realExecWithBadHost: DockerExec = async (command, args) =>
    execFileAsync(command, args as string[], {
      env: { ...process.env, DOCKER_HOST: "npipe:////./pipe/relaylab_test_nonexistent_pipe_for_fault_injection" },
    });

  await assert.rejects(
    () => reconcileAbandonedExecution(name, { exec: realExecWithBadHost }),
    (err: unknown) => {
      assert.ok(err instanceof ReconciliationFailed, `expected a ReconciliationFailed, got ${err}`);
      return true;
    },
  );
});

test("reconcileAbandonedExecution: an unrecognized CLI failure (e.g. a permission error) on the final confirmation is never treated as success (controlled fault injection)", async () => {
  const name = "relaylab-verify-fake-unknown-final-state";
  let stage = 0;
  const fakeExec: DockerExec = async (_command, args) => {
    const a = args as string[];
    const [sub, ...rest] = a;
    if (sub === "inspect") {
      const format = rest[rest.length - 1];
      if (format === "{{.State.Running}}") return { stdout: "false\n", stderr: "" };
      // First inspect (initial check): present. Second inspect (final
      // confirmation, AFTER stop+rm): an unrecognized failure -- never
      // "no such object", never a clean success.
      stage++;
      if (stage === 1) return { stdout: "fake-container-id\n", stderr: "" };
      throw Object.assign(new Error("permission denied"), { stderr: "Error response from daemon: permission denied while trying to connect" });
    }
    if (sub === "stop") return { stdout: "", stderr: "" };
    if (sub === "rm") return { stdout: "", stderr: "" };
    throw new Error(`unexpected docker call in test: ${a.join(" ")}`);
  };

  await assert.rejects(
    () => reconcileAbandonedExecution(name, { exec: fakeExec }),
    (err: unknown) => {
      assert.ok(err instanceof ReconciliationFailed, `expected a ReconciliationFailed, got ${err}`);
      assert.match((err as Error).message, /could not confirm the final state/i);
      return true;
    },
  );
});
