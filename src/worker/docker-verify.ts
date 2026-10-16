// Phase 2C: the Docker verification adapter. Runs the SAME fixed,
// allowlisted command (`node verify.js`, src/worker/verify.ts's
// `VERIFICATION_COMMAND_IDENTITY`) inside an explicitly pinned,
// non-root, read-only, network-disabled Linux container -- never a
// task-supplied command, never a container the caller can influence
// beyond WHICH candidate tree is mounted in. Container isolation for
// THIS local prototype -- explicitly NOT a claim of VM-grade or hostile
// multi-tenant security (see docs/EXECUTION.md "What this does not
// claim").
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { createHash } from "node:crypto";
import path from "node:path";
import {
  boundedCollector,
  deterministicExecutionName,
  DEFAULT_VERIFY_TIMEOUT_MS,
  MAX_CAPTURED_OUTPUT_BYTES,
  type VerificationAdapter,
  type VerificationOutcome,
  type VerificationRequest,
  type VerificationResult,
} from "./verification.js";
import { catFileBlob, git, listTreeEntries } from "./git.js";
import { VERIFICATION_COMMAND_IDENTITY } from "./verify.js";
import { readTar, writeTar, type TarEntry } from "./tar-writer.js";

const execFileAsync = promisify(execFile);

/** The ONE explicitly pinned image tag this adapter runs -- see infra/verify-image/Dockerfile and `npm run docker:build-verify-image`. Never `relaylab-verify:latest`. */
export const VERIFY_IMAGE_TAG = "relaylab-verify:24.21.0-alpine3.24";

export interface DockerVerifyOptions {
  imageTag?: string;
  memoryLimit?: string; // docker --memory syntax, e.g. "256m"
  cpus?: string; // docker --cpus syntax, e.g. "1"
  pidsLimit?: number;
  scratchTmpfsSize?: string; // e.g. "16m"
  stopGraceSeconds?: number;
  /**
   * Phase 2C.1, Part 2: invoked once the container is CONFIRMED running
   * (polled via `docker inspect`, not merely "the `docker start` CLI
   * call returned"), with its real container ID -- before this function
   * starts racing for exit/timeout/cancellation. Exists so a caller
   * (`run-task.ts`) can report a structured crash-test readiness barrier
   * naming the REAL container ID alongside its own pid/task/attempt/
   * epoch identity (see `tests/integration/git-worker-crash.ts`
   * "crash-during-container-verification"). Omitted in every normal
   * (non-test) call; awaited, so a hook that never resolves (a hung
   * failpoint) correctly keeps this function from proceeding -- the
   * container itself keeps running throughout, unaffected by this
   * process being paused, exactly like the real crash scenario this
   * mirrors.
   */
  onContainerCreated?: (containerId: string) => void | Promise<void>;
}

const DEFAULTS: Required<Omit<DockerVerifyOptions, "onContainerCreated">> = {
  imageTag: VERIFY_IMAGE_TAG,
  memoryLimit: "256m",
  cpus: "1",
  pidsLimit: 128,
  scratchTmpfsSize: "16m",
  stopGraceSeconds: 5,
};

function elapsedMs(startedAt: bigint): number {
  return Number(process.hrtime.bigint() - startedAt) / 1_000_000;
}

/**
 * The shape of `execFileAsync("docker", args)` -- extracted to its own
 * type so tests can inject a FAKE implementation for error paths that
 * are impractical or unsafe to force against a real Docker daemon
 * (e.g. a permission error, or an unexpected CLI failure), per the
 * Phase 2C correctness patch's "use controlled fault injection for
 * error paths rather than restarting the user's Docker installation."
 * `reconcileAbandonedExecution`'s OPTIONAL `deps.exec` is the ONLY
 * injection seam; production code (`verifyInDocker`) never passes it,
 * always using the real `realDockerExec` default.
 */
export type DockerExec = (command: string, args: readonly string[]) => Promise<{ stdout: string; stderr: string }>;

const realDockerExec: DockerExec = (command, args) => execFileAsync(command, args as string[]);

/** Docker CLI errors carry their real message in `stderr` (via execFile's thrown error object); falls back to the error's own `message` for anything else (e.g. a fake exec in a test throwing a plain Error). */
function dockerErrorText(err: unknown): string {
  const e = err as { stderr?: string; message?: string };
  return (e?.stderr ?? e?.message ?? String(err)).toString();
}

export type DockerInspectOutcome =
  | { kind: "present"; value: string }
  | { kind: "absent" }
  | { kind: "unavailable"; detail: string }
  | { kind: "unknown"; detail: string };

/**
 * Phase 2C correctness patch, Part 1: classifies a `docker inspect`
 * attempt instead of collapsing EVERY failure into "the container is
 * absent," which is what this adapter did before this patch. Empirically
 * confirmed against this project's own real Docker CLI (not assumed):
 *
 *   - a genuinely absent object:  `error: no such object: <name>`
 *   - a daemon unreachable (simulated via a deliberately bad
 *     `DOCKER_HOST`, never by actually taking down the user's Docker
 *     Desktop installation): `failed to connect to the docker API at
 *     ...`
 *
 * Only the FIRST pattern is classified "absent." Daemon-unreachable
 * and any OTHER unrecognized failure (a permission error, a malformed
 * response, an unexpected CLI change) are both classified distinctly
 * and NEVER treated as proof of absence -- a caller that cannot tell
 * "the container never existed" apart from "I cannot currently tell
 * whether it exists" must not act as if it knows the answer.
 */
async function inspectDocker(exec: DockerExec, nameOrId: string, format: string): Promise<DockerInspectOutcome> {
  try {
    const { stdout } = await exec("docker", ["inspect", nameOrId, "--format", format]);
    return { kind: "present", value: stdout.trim() };
  } catch (err) {
    const detail = dockerErrorText(err);
    if (/no such (object|container)/i.test(detail)) return { kind: "absent" };
    if (/cannot connect to the docker (api|daemon)|failed to connect to the docker api|daemon is not running|dockerdesktoplinuxengine/i.test(detail)) {
      return { kind: "unavailable", detail };
    }
    return { kind: "unknown", detail };
  }
}

/**
 * Thrown by `reconcileAbandonedExecution` when it cannot establish
 * that the prior execution's resources are in a KNOWN, safe state --
 * never silently treated as success. Carries the execution name and a
 * human-readable reason so a caller (`verifyInDocker`) can report it
 * as a distinct `infrastructure_failure`, never confused with a real
 * verification outcome.
 */
export class ReconciliationFailed extends Error {
  constructor(
    readonly executionName: string,
    readonly reason: string,
  ) {
    super(
      `reconcileAbandonedExecution: could not establish a safe final state for "${executionName}" -- ${reason}. Refusing to let a caller proceed as though reconciliation succeeded.`,
    );
    this.name = "ReconciliationFailed";
  }
}

/**
 * Resolves the REAL image ID (a `sha256:...` digest) for `imageTag` --
 * "avoid an unrecorded mutable latest" means this adapter never trusts
 * the tag string alone as evidence of what actually ran; it always
 * records this resolved value. Throws if the image is not already
 * present locally -- this adapter NEVER implicitly builds or pulls it
 * (build-time dependency acquisition is a separate, explicit step --
 * `npm run docker:build-verify-image` -- per the milestone's own
 * "verification itself must work offline" rule).
 */
export async function resolveVerifyImageId(imageTag: string): Promise<string> {
  const { stdout } = await execFileAsync("docker", ["image", "inspect", imageTag, "--format", "{{.Id}}"]);
  const id = stdout.trim();
  if (!id.startsWith("sha256:")) {
    throw new Error(`docker image inspect did not return a real image ID for "${imageTag}": "${id}"`);
  }
  return id;
}

/** The one fixed filename `materializeCandidateInput` writes the archived tar into, and `buildVerifyCreateArgs` bind-mounts read-only -- a private implementation detail of this pair, never part of the public interface either side exposes beyond `MaterializedInput.exportDir`. */
const CANDIDATE_TAR_FILENAME = "candidate.tar";

/**
 * Phase 2C.1, Part 1: tree entry modes this adapter explicitly refuses
 * to materialize, rather than silently producing an incomplete or
 * unsafe input -- `120000` (symlink: could reference an absolute path
 * or escape outside the export once followed/read) and `160000`
 * (gitlink/submodule: `git archive` cannot actually materialize a
 * submodule's content at all, only a bare, useless gitlink marker). A
 * REGULAR file (`100644`), an EXECUTABLE file (`100755`), and a
 * subdirectory (`040000`) are the only entry kinds this fixture's own
 * tracked trees are expected to contain, and the only kinds supported.
 */
const UNSUPPORTED_TREE_MODES: ReadonlySet<string> = new Set(["120000", "160000"]);

export interface MaterializedInput {
  /** A directory containing EXACTLY one file, `candidate.tar` -- see `CANDIDATE_TAR_FILENAME`. Kept as a directory (not the tar path itself) so this shape can grow a second sidecar file later without an interface change. */
  readonly exportDir: string;
  /** The ACTUAL tree object ID (`git rev-parse <ref>^{tree}`) this call archived -- independent of whatever the caller BELIEVES `ref` resolves to. Callers must compare this against their own intended/pinned tree and refuse to trust the verification result on a mismatch (Phase 2C.1, Part 1's identity-mismatch requirement; see `assertMaterializedTreeMatches` below). */
  readonly treeSha: string;
  cleanup(): Promise<void>;
}

/** Thrown by `assertMaterializedTreeMatches` -- a distinct, named error type (never a bare `Error`) so a caller or test can distinguish "this specific invariant failed" from any other thrown error. */
export class VerificationIdentityMismatch extends Error {
  constructor(context: string, expectedTree: string, actualTree: string) {
    super(
      `verification identity mismatch (${context}): materializeCandidateInput archived tree ${actualTree}, but the intended tree is ${expectedTree} -- refusing to let a verification of a DIFFERENT tree authorize this result`,
    );
    this.name = "VerificationIdentityMismatch";
  }
}

/**
 * Phase 2C.1, Part 1: the identity-mismatch guard both `run-task.ts`
 * (A, binding a checkpoint) and `handoff.ts` (B, binding a restoration
 * acknowledgement) call immediately after materializing, BEFORE
 * trusting the verification result for anything. Extracted to one
 * shared, directly-testable function rather than two independent
 * inline copies -- see `tests/integration/docker-verify-exact-input.test.ts`
 * "identity-mismatch regression" for a direct test against this
 * function (materializing TWO different refs and confirming a
 * deliberately swapped expected-tree is rejected).
 */
export function assertMaterializedTreeMatches(materialized: MaterializedInput, expectedTree: string, context: string): void {
  if (materialized.treeSha !== expectedTree) {
    throw new VerificationIdentityMismatch(context, expectedTree, materialized.treeSha);
  }
}

/**
 * Phase 2C, Part 3 ("preserve exact-code verification"), hardened in
 * Phase 2C.1 (mode-bit fidelity) and AGAIN in the Phase 2C correctness
 * patch, Part 3 (archive-attribute fidelity): materializes EXACTLY the
 * candidate's tracked tree at `ref` (a real commit SHA -- the same Git
 * identity `run-task.ts`/`handoff.ts` already pin BEFORE calling this)
 * -- never the live worktree itself, never untracked/ignored/unrelated
 * host files.
 *
 * **Does NOT use `git archive`.** `git archive` ALWAYS applies
 * `.gitattributes` `export-ignore` (silently OMITS a tracked path) and
 * `export-subst` (silently REWRITES a tracked file's content) when
 * present in the archived tree -- this is a genuine CORRECTNESS
 * problem for a system whose entire purpose is "verify the EXACT
 * candidate tree," not a filtered/transformed view of it: a
 * checkpoint claims "this tree was verified," and if `git archive`
 * quietly dropped or rewrote a tracked file along the way, that claim
 * would be FALSE. This was a real mistake in this project's own
 * Phase 2C.1 work -- that patch's own tests CONFIRMED and CELEBRATED
 * `git archive`'s export-ignore/export-subst behavior as "fidelity,"
 * when it is actually the opposite of what "exact tree" requires. See
 * docs/EXECUTION.md "Archive-attribute handling" for the full
 * correction.
 *
 * Instead: `listTreeEntries` (`git ls-tree`) enumerates every tracked
 * entry; each SUPPORTED entry's RAW, STORED bytes are read directly
 * via `catFileBlob` (`git cat-file -p`, src/worker/git.ts) -- which
 * `.gitattributes` export directives never apply to at all, confirmed
 * empirically -- and packaged into `candidate.tar` by this module's
 * own minimal `writeTar` (`src/worker/tar-writer.ts`), carrying each
 * entry's exact git-recorded mode. The result: a tracked
 * `export-ignore`'d file STILL exists in the verification input with
 * its unmodified bytes; a tracked `export-subst`'d file's
 * `$Format:...$` marker is delivered UNCHANGED, not expanded. Bytes
 * come from the SELECTED git objects directly -- never from a SECOND,
 * independent archive export that could itself drift from what
 * `listTreeEntries` reported.
 *
 * Mode-bit fidelity (Phase 2C.1) is preserved: the tar is bind-mounted
 * read-only into the container and extracted THERE, on a real Linux
 * filesystem (`buildVerifyCreateArgs`'s bootstrap), never on the
 * Windows host -- confirmed: an executable tracked file arrives at
 * mode 755, a non-executable one at 644.
 *
 * Explicitly refuses (throws, never silently materializes an
 * incomplete input) if the tree contains a symlink or submodule
 * gitlink entry -- see `UNSUPPORTED_TREE_MODES`. This patch does not
 * broaden that -- a sparse-checkout cone, a `.gitmodules`-without-
 * submodule case, or any other unusual tree shape remains untested
 * and unsupported.
 */
export async function materializeCandidateInput(repoDir: string, ref: string, env: NodeJS.ProcessEnv, scratchRoot: string): Promise<MaterializedInput> {
  const entries = await listTreeEntries(repoDir, ref, env);
  const unsupported = entries.filter((e) => UNSUPPORTED_TREE_MODES.has(e.mode));
  if (unsupported.length > 0) {
    throw new Error(
      `materializeCandidateInput: tree "${ref}" contains ${unsupported.length} tree entry/entries this adapter does not support (symlink or submodule gitlink) -- refusing to silently produce an incomplete or unsafe verification input: ${unsupported.map((e) => `${e.mode} ${e.path}`).join(", ")}`,
    );
  }
  const treeSha = (await git(["rev-parse", `${ref}^{tree}`], repoDir, env)).stdout.trim();

  const tarEntries: TarEntry[] = await Promise.all(
    entries.map(async (entry) => ({
      path: entry.path,
      mode: Number.parseInt(entry.mode, 8),
      content: await catFileBlob(repoDir, entry.object, env),
    })),
  );

  await mkdir(scratchRoot, { recursive: true });
  const exportDir = await mkdtemp(path.join(scratchRoot, "verify-input-"));
  const tarPath = path.join(exportDir, CANDIDATE_TAR_FILENAME);
  await writeTar(tarEntries, tarPath);
  return {
    exportDir,
    treeSha,
    cleanup: () => rm(exportDir, { recursive: true, force: true }),
  };
}

/**
 * Phase 2C, Part 4: restart reconciliation for THIS adapter's own
 * abandoned execution resources. A deterministic container name
 * (`deterministicExecutionName`) means a worker process that crashed
 * mid-verification and restarted will resolve to the EXACT SAME name on
 * its next attempt -- `docker create --name` would fail outright
 * against a pre-existing container with that name, so this is called
 * BEFORE every create, stopping (if still running) and removing
 * EXACTLY that one name -- never a global prune, never touching a
 * container belonging to a different task/attempt/role.
 *
 * **Phase 2C correctness patch, Part 1 -- the contract this now
 * actually honors** (the pre-patch version collapsed every `docker
 * inspect` error into "absent" and silently swallowed every stop/
 * remove error, which could let a caller proceed to `docker create`
 * believing reconciliation succeeded when the true resource state was
 * simply unknown):
 *
 *   1. The INITIAL inspect is classified (`inspectDocker`) -- only a
 *      confirmed-absent result returns normally with nothing done.
 *      Daemon-unreachable or any unrecognized failure THROWS
 *      `ReconciliationFailed` immediately -- this function never
 *      guesses that "inspect failed" means "nothing to reconcile."
 *   2. If a container genuinely exists, stop (best-effort -- a stop
 *      failure alone is NOT fatal, since a subsequent FORCED removal
 *      can still succeed against a still-running container) then
 *      force-remove (also best-effort) are both attempted.
 *   3. The FINAL state is independently re-confirmed via the SAME
 *      classification, regardless of what the individual stop/remove
 *      calls reported. Only a confirmed-absent final state returns
 *      normally. A still-present container, or a final state that
 *      cannot be classified either way, THROWS `ReconciliationFailed`
 *      -- "confirm the final resource state before returning
 *      successful reconciliation."
 *
 * `deps.exec` is an injection seam for tests exercising failure paths
 * that are impractical to force against a real Docker daemon (see
 * `DockerExec`'s doc comment) -- production code never passes it.
 */
export async function reconcileAbandonedExecution(name: string, deps: { exec?: DockerExec } = {}): Promise<void> {
  const exec = deps.exec ?? realDockerExec;

  const initial = await inspectDocker(exec, name, "{{.Id}}");
  if (initial.kind === "absent") return;
  if (initial.kind !== "present") {
    throw new ReconciliationFailed(name, `could not confirm whether a prior execution exists (${initial.kind}: ${initial.detail})`);
  }

  await stopContainerAndWait(name, DEFAULTS.stopGraceSeconds, exec);
  await removeContainer(name, exec);

  const final = await inspectDocker(exec, name, "{{.Id}}");
  if (final.kind === "absent") return;
  if (final.kind === "present") {
    throw new ReconciliationFailed(name, "the container is still present after a stop-then-force-remove attempt");
  }
  throw new ReconciliationFailed(name, `could not confirm the final state after a stop-then-force-remove attempt (${final.kind}: ${final.detail})`);
}

/**
 * Stops a container FOR REAL and WAITS for it to actually stop --
 * "killing the Docker CLI alone is not proof that its container
 * stopped" is answered here by independently re-checking
 * `State.Running` via `docker inspect` AFTER `docker stop` returns,
 * and escalating to `docker kill` if it is somehow still running. Its
 * own `{stopped}` return value is a best-effort signal ONLY --
 * `reconcileAbandonedExecution` above never trusts it alone, always
 * re-confirming the true final state independently afterward.
 */
async function stopContainerAndWait(containerIdOrName: string, graceSeconds: number, exec: DockerExec = realDockerExec): Promise<{ stopped: boolean }> {
  await exec("docker", ["stop", "--time", String(graceSeconds), containerIdOrName]).catch(() => {});
  const runningNow = await inspectDocker(exec, containerIdOrName, "{{.State.Running}}");
  const running = runningNow.kind === "present" && runningNow.value === "true";
  if (running) {
    await exec("docker", ["kill", containerIdOrName]).catch(() => {});
    const stillRunningNow = await inspectDocker(exec, containerIdOrName, "{{.State.Running}}");
    const stillRunning = stillRunningNow.kind === "present" && stillRunningNow.value === "true";
    return { stopped: !stillRunning };
  }
  return { stopped: true };
}

/**
 * Phase 2C.1, Part 2: polls `docker inspect` until `State.Running` is
 * actually `true` (or `timeoutMs` elapses) -- `docker start --attach`
 * returning control to the CLI does not by itself guarantee the
 * container has entered the Running state yet. Used ONLY to decide
 * when it is safe to invoke `DockerVerifyOptions.onContainerCreated`,
 * so that hook's "the container is running" claim is independently
 * confirmed, not assumed from CLI return timing alone.
 */
async function waitUntilRunning(containerId: string, timeoutMs: number): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const outcome = await inspectDocker(realDockerExec, containerId, "{{.State.Running}}");
    if (outcome.kind === "present" && outcome.value === "true") return true;
    await new Promise((r) => setTimeout(r, 50));
  }
  return false;
}

async function removeContainer(containerIdOrName: string, exec: DockerExec = realDockerExec): Promise<{ removed: boolean; error?: string }> {
  try {
    await exec("docker", ["rm", "-f", containerIdOrName]);
    return { removed: true };
  } catch (err) {
    return { removed: false, error: dockerErrorText(err) };
  }
}

function infrastructureFailure(executionId: string, startedAt: bigint, message: string, cleanupError: string | null = null): VerificationResult {
  return {
    outcome: "infrastructure_failure",
    exitCode: null,
    durationMs: elapsedMs(startedAt),
    stdout: "",
    stderr: message,
    stdoutTruncated: false,
    stderrTruncated: false,
    executionId,
    adapterName: "docker",
    cleanupError,
  };
}

/**
 * Phase 3A, Part A: a bounded, post-verification INTEGRITY CHECK --
 * the before/after comparison this project did not previously have
 * (confirmed absent by direct inspection before writing this). Phase
 * 2C's correctness patch confirmed `chmod -R a-w` is permission-based,
 * not an immutability guarantee: the SAME user running the verified
 * command owns the extracted files and CAN `chmod` them back and
 * tamper with them (and still exit 0, claiming success). This check
 * does not try to PREVENT that at the container level (that would
 * need the privilege-separation already investigated and rejected --
 * see docs/EXECUTION.md "Actual input-protection guarantee") --
 * instead, it independently DETECTS it from the host/orchestrator
 * side, AFTER the verified command has exited but BEFORE the
 * container is removed, and refuses to let a tampered result be
 * reported as `passed`.
 *
 * **Why `docker exec`, not `docker cp`**: `docker cp` was tried
 * first and does NOT work against a `--tmpfs`-mounted path on this
 * project's own Docker setup -- confirmed empirically, including
 * against a container still RUNNING (`docker exec ... cat` reads the
 * same path successfully; `docker cp` of the identical path fails
 * with "Could not find the file"). `docker cp` reads from the
 * container's persisted filesystem layers, which tmpfs mounts are
 * never part of (they are in-memory, not graph-driver content) --
 * this is a general Docker limitation, not specific to this host.
 * `docker exec` runs INSIDE the container's real namespace instead,
 * with full access to every mount including tmpfs, but only works
 * against a RUNNING container -- which is why the bootstrap
 * (`verifyBootstrapScript`) no longer lets the container exit the
 * instant the verified command finishes; it idles
 * (`exec tail -f /dev/null`) so this check has a window to run
 * BEFORE `verifyInDocker` explicitly stops it.
 *
 * Compares the EXPECTED set -- read directly back from the ORIGINAL
 * `candidate.tar` via `readTar` (never a second, independent
 * re-materialization) -- against the ACTUAL final state of
 * `/tmp/verify`, walked and hashed INSIDE the container via a single
 * `docker exec ... node -e <script>` call (reusing the image's own
 * `node`, never a second runtime). Detects: changed bytes, added
 * paths, deleted paths, and changed executable semantics (the exec
 * bit specifically, not the full mode, since `chmod -R a-w`
 * legitimately strips write bits from every entry as part of normal
 * operation). `/tmp/scratch` (`SCRATCH_DIR`) is never inspected --
 * legitimate verifier output written there can never be mistaken for
 * input tampering.
 *
 * Returns a discriminated result rather than throwing: `"match"` (the
 * verified command's candidate was not an issue), `"mismatch"` (a
 * REAL, confirmed difference -- never a success), or `"check_failed"`
 * (the check itself could not be completed -- e.g. the `docker exec`
 * call failed -- which this project treats as a FAILURE, never as
 * success by default, per "treat inability to complete the integrity
 * check as failure, never success").
 */
type IntegrityCheckResult = { status: "match" } | { status: "mismatch"; detail: string } | { status: "check_failed"; detail: string };

interface WalkedEntry {
  path: string;
  execBit: boolean;
  sha256: string;
}

/**
 * Runs entirely INSIDE the container via `docker exec` -- a single
 * Node invocation, no shell, no file written to disk by this script
 * (its argv is one literal string, never built from task-supplied
 * input). Deliberately mirrors `materializeCandidateInput`'s own
 * notion of "a tracked entry" (files only -- this adapter never
 * materializes symlinks/gitlinks in the first place).
 */
const INTEGRITY_WALK_SCRIPT = `
const fs=require("fs"),crypto=require("crypto"),path=require("path");
function walk(dir,base){const out=[];for(const d of fs.readdirSync(dir,{withFileTypes:true})){const full=path.join(dir,d.name);const rel=base?base+"/"+d.name:d.name;if(d.isDirectory())out.push(...walk(full,rel));else if(d.isFile()){const c=fs.readFileSync(full);const st=fs.statSync(full);out.push({path:rel,execBit:(st.mode&0o111)!==0,sha256:crypto.createHash("sha256").update(c).digest("hex")});}}return out;}
process.stdout.write(JSON.stringify(walk("/tmp/verify","")));
`;

async function verifyInputIntegrity(containerId: string, originalTarPath: string): Promise<IntegrityCheckResult> {
  let expected: TarEntry[];
  try {
    expected = await readTar(originalTarPath);
  } catch (err) {
    return { status: "check_failed", detail: `could not read the original candidate.tar to determine the expected content: ${err instanceof Error ? err.message : String(err)}` };
  }

  let actual: WalkedEntry[];
  try {
    const { stdout } = await execFileAsync("docker", ["exec", containerId, "node", "-e", INTEGRITY_WALK_SCRIPT]);
    actual = JSON.parse(stdout) as WalkedEntry[];
  } catch (err) {
    return { status: "check_failed", detail: `docker exec integrity walk failed: ${err instanceof Error ? err.message : String(err)}` };
  }

  const actualByPath = new Map(actual.map((e) => [e.path, e]));
  const expectedPaths = new Set(expected.map((e) => e.path));
  const mismatches: string[] = [];

  for (const exp of expected) {
    const act = actualByPath.get(exp.path);
    if (!act) {
      mismatches.push(`DELETED: "${exp.path}" (expected, missing from the final input)`);
      continue;
    }
    const expectedHash = createHash("sha256").update(exp.content).digest("hex");
    if (act.sha256 !== expectedHash) {
      mismatches.push(`CHANGED BYTES: "${exp.path}"`);
    }
    const expectedExecBit = (exp.mode & 0o111) !== 0;
    if (act.execBit !== expectedExecBit) {
      mismatches.push(`CHANGED EXECUTABLE SEMANTICS: "${exp.path}" (expected exec=${expectedExecBit}, actual exec=${act.execBit})`);
    }
  }
  for (const act of actual) {
    if (!expectedPaths.has(act.path)) {
      mismatches.push(`ADDED: "${act.path}" (not part of the materialized candidate)`);
    }
  }

  if (mismatches.length > 0) {
    return { status: "mismatch", detail: mismatches.join("; ") };
  }
  return { status: "match" };
}

/**
 * Builds the EXACT `docker create` argument array `verifyInDocker` runs
 * -- extracted to its own pure function so tests can assert on the
 * real isolation flags (non-root, read-only root, network disabled,
 * capabilities dropped, no-new-privileges, resource limits) as DATA,
 * and so a test wanting real `docker inspect` evidence (which requires
 * the container to still exist, unlike `verifyInDocker`'s own
 * always-remove-at-the-end behavior) can create one with these SAME
 * args directly, with no risk of the test's copy drifting from what
 * production actually runs.
 */
/**
 * Phase 2C.1: the container's effective command is now a tiny,
 * fixed, non-task-influenced bootstrap -- `mkdir, tar -xf (from the
 * read-only-mounted tar FILE), cd, exec node verify.js` -- rather than
 * `node verify.js` directly against a bind-mounted directory. The
 * FINAL `exec` means the verified process still ends up as the
 * container's actual PID 1 (replacing the shell, not a child of it),
 * so exit-code/signal semantics are unchanged; `docker inspect
 * Config.Cmd` now reports this bootstrap script rather than the bare
 * `["node","verify.js"]` array, which is why this is documented
 * explicitly rather than left implicit. `VERIFICATION_COMMAND_IDENTITY`
 * itself (the fixed, allowlisted executable/args) is UNCHANGED and
 * still the only thing that ultimately runs -- see
 * docs/EXECUTION.md "Exact-input fidelity" for why this bootstrap
 * exists (Windows-host tar extraction silently discarded the
 * executable-bit distinction; extracting on the container's own Linux
 * filesystem does not).
 *
 * `chmod -R a-w` immediately after extraction, BEFORE the verified
 * command ever runs, restores "retain a read-only verification input"
 * -- extracting onto the tmpfs necessarily makes the EXTRACTION target
 * writable (that is what lets `tar` create the files at all), so
 * without this, a write into the verified command's own working
 * directory would silently succeed where it used to fail. Empirically
 * confirmed: a write attempt afterward fails with `EACCES` (permission
 * denied on the now-read-only directory/files), not `EROFS` (read-only
 * FILESYSTEM) as the pre-Phase-2C.1 directory-bind-mount design
 * produced -- both are a real, observable write REJECTION; the exact
 * errno differs because the mechanism enforcing it changed from a
 * read-only mount to read-only permissions on a writable mount. `/tmp`
 * OUTSIDE `/tmp/verify` remains genuinely writable scratch space, per
 * the "explicit writable scratch space where required" requirement --
 * this only locks down the verification INPUT itself, not all of `/tmp`.
 *
 * Phase 3A, Part A: `/tmp/scratch` is created explicitly as the
 * SANCTIONED home for any output the verified command legitimately
 * needs to write -- "keep verifier-generated output outside the
 * tracked input." `VERIFY_SCRATCH_DIR` env var tells the command where
 * it is, though this fixture's own `verify.js` does not currently use
 * it. The post-verification integrity check below only ever inspects
 * `/tmp/verify`, never `/tmp/scratch` -- a legitimate write there can
 * never be mistaken for tampering with the input.
 *
 * **Phase 3A, Part A, second change**: the verified command is no
 * longer the FINAL `exec`'d process. It still runs to completion
 * exactly as before (same argv, same working directory, same
 * environment), but its exit code is now captured explicitly and
 * reported via a marker line (`EXIT_MARKER_PREFIX`) on stdout, and the
 * container then idles (`exec tail -f /dev/null`) instead of exiting
 * immediately. This is REQUIRED for the integrity check above: a
 * `docker exec` (the only mechanism that can read a tmpfs-mounted
 * path -- see `verifyInputIntegrity`'s doc comment) only works
 * against a RUNNING container, and the container would otherwise stop
 * itself the instant the verified command finishes. `verifyInDocker`
 * watches the streamed stdout for the marker (never waits for the
 * container's own exit event on the success path), runs the integrity
 * check while the container is still alive, and ONLY THEN explicitly
 * stops it -- the exact same `stopContainerAndWait` already used for
 * timeout/cancellation, so cleanup ownership rules are unchanged. The
 * marker line itself is stripped from the `stdout` this adapter
 * returns -- never shown to a caller as if it were the verified
 * command's own output.
 */
const SCRATCH_DIR = "/tmp/scratch";
const EXIT_MARKER_PREFIX = "__RELAYLAB_VERIFY_EXIT__:";

function verifyBootstrapScript(): string {
  const argv = [VERIFICATION_COMMAND_IDENTITY.executable, ...VERIFICATION_COMMAND_IDENTITY.argsTemplate];
  return [
    "set -e",
    `mkdir -p /tmp/verify ${SCRATCH_DIR}`,
    "tar -xf /verify-input.tar -C /tmp/verify",
    "chmod -R a-w /tmp/verify",
    "cd /tmp/verify",
    "set +e",
    `env VERIFY_SCRATCH_DIR=${SCRATCH_DIR} ${argv.join(" ")}`,
    "code=$?",
    `echo "${EXIT_MARKER_PREFIX}$code"`,
    // Only idle (giving `verifyInDocker` a window to run the
    // `docker exec`-based integrity check before explicitly stopping
    // this container) when the command actually succeeded -- that is
    // the ONLY outcome the integrity check applies to. A failing
    // command exits immediately, same latency as before this patch.
    'if [ "$code" -eq 0 ]; then exec tail -f /dev/null; else exit "$code"; fi',
  ].join("; ");
}

export function buildVerifyCreateArgs(params: {
  name: string;
  inputDir: string;
  imageTag: string;
  memoryLimit: string;
  cpus: string;
  pidsLimit: number;
  scratchTmpfsSize: string;
}): string[] {
  const tarHostPath = path.join(params.inputDir, CANDIDATE_TAR_FILENAME);
  return [
    "create",
    "--name",
    params.name,
    "--user",
    "node",
    "--read-only",
    "--tmpfs",
    `/tmp:rw,noexec,nosuid,size=${params.scratchTmpfsSize}`,
    "--network",
    "none",
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    "--memory",
    params.memoryLimit,
    "--cpus",
    params.cpus,
    "--pids-limit",
    String(params.pidsLimit),
    // Bounds DOCKER'S OWN retained log storage for this container --
    // independent of (and in addition to) this adapter's own in-process
    // bounded collectors, so a noisy fixture cannot grow host disk
    // usage without limit via the daemon's own log file either.
    "--log-opt",
    "max-size=1m",
    "--log-opt",
    "max-file=1",
    // The ONLY mount: the archived tar FILE itself, read-only. Never a
    // directory, never the live worktree, never the host's Docker
    // socket/credentials/home directory.
    "--mount",
    `type=bind,source=${tarHostPath},target=/verify-input.tar,readonly`,
    params.imageTag,
    "/bin/sh",
    "-c",
    verifyBootstrapScript(),
  ];
}

/**
 * Runs the fixed verification command inside a fresh, isolated
 * container. `request.inputDir` must already be a materialized,
 * read-only-safe candidate export (see `materializeCandidateInput`) --
 * this function does not materialize it itself, so the SAME exported
 * input can be reused across a retry without re-exporting.
 */
export async function verifyInDocker(request: VerificationRequest, options: DockerVerifyOptions = {}): Promise<VerificationResult> {
  const opts = { ...DEFAULTS, ...options };
  const startedAt = process.hrtime.bigint();
  const name = deterministicExecutionName(request.taskId, request.attemptId, "run");

  // Phase 2C correctness patch, Part 1: a reconciliation failure means
  // the prior resource's state is UNKNOWN (not "confirmed clear") --
  // this must never proceed to `docker create` as though reconciliation
  // succeeded. Reported as `infrastructure_failure` (never a
  // verification outcome), with the reconciliation failure's own
  // message carried in BOTH `stderr` and the dedicated `cleanupError`
  // field, so it is observable without being confused with a real
  // verification result.
  try {
    await reconcileAbandonedExecution(name);
  } catch (err) {
    if (err instanceof ReconciliationFailed) {
      return infrastructureFailure(
        name,
        startedAt,
        `restart reconciliation could not establish a safe state before creating a new execution: ${err.message}`,
        err.message,
      );
    }
    throw err;
  }

  try {
    await resolveVerifyImageId(opts.imageTag);
  } catch (err) {
    return infrastructureFailure(name, startedAt, `failed to resolve verification image "${opts.imageTag}" (never implicitly built/pulled): ${err instanceof Error ? err.message : String(err)}`);
  }

  let containerId: string;
  try {
    const { stdout } = await execFileAsync("docker", buildVerifyCreateArgs({ name, inputDir: request.inputDir, ...opts }));
    containerId = stdout.trim();
  } catch (err) {
    return infrastructureFailure(name, startedAt, `docker create failed to launch the verification container: ${err instanceof Error ? err.message : String(err)}`);
  }

  const stdoutCollector = boundedCollector(MAX_CAPTURED_OUTPUT_BYTES);
  const stderrCollector = boundedCollector(MAX_CAPTURED_OUTPUT_BYTES);

  let startChild;
  try {
    startChild = spawn("docker", ["start", "--attach", containerId], { windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
  } catch (err) {
    const removal = await removeContainer(containerId);
    return infrastructureFailure(
      containerId,
      startedAt,
      `docker start failed to launch: ${err instanceof Error ? err.message : String(err)}`,
      removal.removed ? null : (removal.error ?? "container removal failed for an unknown reason"),
    );
  }
  startChild.stdout.on("data", stdoutCollector.onData);
  startChild.stderr.on("data", stderrCollector.onData);

  // Phase 3A, Part A: the verified command's REAL exit code now
  // arrives as a marker line on stdout (see `verifyBootstrapScript`'s
  // doc comment for why) -- a SEPARATE listener on the same stream,
  // never interfering with `stdoutCollector`'s own bounded capture.
  // Scans a small rolling buffer (bounded -- `MAX_CAPTURED_OUTPUT_BYTES`
  // -- so a noisy fixture's output cannot make this grow unbounded
  // even if the marker never appears, e.g. because the command never
  // exits at all).
  let markerScanBuffer = "";
  let markerResolved = false;
  let resolveMarker: ((code: number) => void) | undefined;
  const markerPromise = new Promise<number>((resolve) => {
    resolveMarker = resolve;
  });
  const onStdoutForMarker = (chunk: Buffer) => {
    if (markerResolved) return;
    markerScanBuffer += chunk.toString("utf8");
    if (markerScanBuffer.length > MAX_CAPTURED_OUTPUT_BYTES * 2) {
      markerScanBuffer = markerScanBuffer.slice(-MAX_CAPTURED_OUTPUT_BYTES);
    }
    const idx = markerScanBuffer.indexOf(EXIT_MARKER_PREFIX);
    if (idx < 0) return;
    const match = /^(-?\d+)/.exec(markerScanBuffer.slice(idx + EXIT_MARKER_PREFIX.length));
    if (!match) return;
    markerResolved = true;
    resolveMarker!(Number.parseInt(match[1]!, 10));
  };
  startChild.stdout.on("data", onStdoutForMarker);

  // Attached BEFORE the (possibly slow, possibly permanently hung --
  // see `onContainerCreated`'s doc comment) hook below, never after --
  // a fast-exiting container must not have its "exit" event missed
  // just because this function was busy awaiting a hook at the time.
  // Represents the CONTAINER's own process exiting -- with the new
  // idling bootstrap, this should only fire for an UNEXPECTED early
  // exit (e.g. the setup steps before the verified command failed
  // under `set -e`) or once `verifyInDocker` itself has explicitly
  // stopped the container below. It is never how a NORMAL
  // pass/fail/timeout outcome is detected anymore -- that is the
  // marker's job.
  const exitPromise = new Promise<number | null>((resolve) => {
    startChild.once("exit", (code) => resolve(code));
    startChild.once("error", () => resolve(null));
  });

  if (opts.onContainerCreated) {
    // Race "confirmed running" against "already exited" -- NEVER a
    // flat, unconditional wait. `onContainerCreated` is wired into
    // EVERY real worker run now (run-task.ts always passes it, calling
    // the normally-instant no-op `hitFailpoint`), so a fixed poll-until-
    // running bound here would otherwise add up to its full length as
    // real latency to the ORDINARY fast-verification path every single
    // time, whenever the container happens to finish before this
    // function gets around to polling. A container that has ALREADY
    // exited by the time we check is never reported as "running" --
    // the hook is simply skipped for that run (there is nothing
    // truthful left to confirm), costing at most one fast `docker
    // inspect` round-trip rather than a multi-second wait.
    const confirmedRunning = await Promise.race([waitUntilRunning(containerId, 5_000), exitPromise.then(() => false)]);
    if (confirmedRunning) {
      await opts.onContainerCreated(containerId);
    }
  }

  const timeoutMs = request.timeoutMs ?? DEFAULT_VERIFY_TIMEOUT_MS;
  type RaceResult = { kind: "exited"; code: number } | { kind: "timed_out" } | { kind: "cancelled" } | { kind: "container_exited_unexpectedly"; code: number | null };
  const race = await new Promise<RaceResult>((resolve) => {
    // CONFIRMED real defect, fixed here: `addEventListener("abort", ...)`
    // never fires for a signal that was ALREADY aborted before this
    // listener was added -- the DOM "abort" event is a one-shot
    // dispatch at the moment `.abort()` is called, not a retroactive
    // check of current state. Without this guard, a caller that passed
    // an already-cancelled signal (e.g. cancellation requested while
    // this function was still materializing the candidate, before
    // verification itself ever started) would have this race silently
    // ignore that and let verification run to completion normally.
    if (request.signal?.aborted) {
      resolve({ kind: "cancelled" });
      return;
    }
    const timer = setTimeout(() => resolve({ kind: "timed_out" }), timeoutMs);
    timer.unref();
    const onAbort = () => resolve({ kind: "cancelled" });
    request.signal?.addEventListener("abort", onAbort, { once: true });
    markerPromise.then((code) => resolve({ kind: "exited", code }));
    // Only wins the race if the container stops BEFORE the marker
    // ever appears -- an abnormal exit (e.g. a setup step failed
    // under `set -e`, before the verified command ever ran).
    exitPromise.then((code) => resolve({ kind: "container_exited_unexpectedly", code }));
  });

  let outcome: VerificationOutcome;
  let exitCode: number | null = null;
  let integrityStderrSuffix = "";
  if (race.kind === "exited") {
    exitCode = race.code;
    outcome = exitCode === 0 ? "passed" : "verification_failed";

    // Phase 3A, Part A: the post-verification integrity check -- ONLY
    // when the command itself would otherwise be reported `passed`.
    // A nonzero exit already refuses to checkpoint regardless, so
    // there is nothing this check could additionally protect there;
    // running it only on the one path that would otherwise claim
    // success is what "do not accept verification success when the
    // final input differs" actually requires. MUST run BEFORE the
    // container is stopped/removed below -- `docker exec` needs it
    // to still be running.
    if (outcome === "passed") {
      const integrity = await verifyInputIntegrity(containerId, path.join(request.inputDir, CANDIDATE_TAR_FILENAME));
      if (integrity.status === "mismatch") {
        outcome = "verification_failed";
        integrityStderrSuffix = `[integrity check] the final verification input differs from the materialized candidate -- refusing to accept this as a successful verification: ${integrity.detail}`;
      } else if (integrity.status === "check_failed") {
        // "Treat inability to complete the integrity check as
        // failure, never success" -- mapped to infrastructure_failure
        // (this project's OWN tooling could not establish the fact,
        // as opposed to the candidate's command itself failing).
        outcome = "infrastructure_failure";
        integrityStderrSuffix = `[integrity check] could not be completed, so this result is NOT accepted as a successful verification: ${integrity.detail}`;
      }
    }

    if (exitCode === 0) {
      // The bootstrap only idles (`tail -f /dev/null`) on a SUCCESSFUL
      // exit (see `verifyBootstrapScript`) -- explicitly stop it now,
      // the SAME way the timeout/cancellation path always has, then
      // reap the CLI child's own exit. A FAILING exit already made the
      // container self-terminate (`exit "$code"`), so none of this is
      // needed there -- `exitPromise` (awaited below regardless)
      // already resolved on its own, same latency as before this patch.
      await stopContainerAndWait(containerId, opts.stopGraceSeconds);
      if (startChild.exitCode === null && startChild.signalCode === null) {
        startChild.kill("SIGKILL");
      }
    }
    await exitPromise.catch(() => {});
  } else if (race.kind === "container_exited_unexpectedly") {
    // The container stopped on its own before the marker ever
    // appeared -- this adapter's OWN bootstrap/environment is what
    // broke (never the candidate's own logic, which never got to run
    // to completion), so this is infrastructure_failure, not
    // verification_failed. Already stopped; nothing further to stop.
    exitCode = race.code;
    outcome = "infrastructure_failure";
    integrityStderrSuffix = `the container exited unexpectedly (code ${race.code}) before the verification command's own completion marker was ever seen`;
  } else {
    outcome = race.kind === "timed_out" ? "timed_out" : "cancelled";
    // The decisive part: STOP THE ACTUAL CONTAINER and verify it
    // stopped -- never infer this from killing the CLI child alone.
    await stopContainerAndWait(containerId, opts.stopGraceSeconds);
    if (startChild.exitCode === null && startChild.signalCode === null) {
      startChild.kill("SIGKILL");
    }
    await exitPromise.catch(() => {});
  }

  const durationMs = elapsedMs(startedAt);
  // The exit marker line is OUR OWN bootstrap's bookkeeping, never
  // part of the verified command's real output -- stripped before
  // this adapter reports `stdout` to any caller.
  const { text: rawStdout, truncated: stdoutTruncated } = stdoutCollector.get();
  const markerLineRe = new RegExp(`\\n?${EXIT_MARKER_PREFIX}-?\\d+\\n?`);
  const stdout = markerLineRe.test(rawStdout) ? rawStdout.replace(markerLineRe, "\n") : rawStdout;
  const { text: stderrBase, truncated: stderrTruncatedBase } = stderrCollector.get();
  const stderr = integrityStderrSuffix ? `${stderrBase}\n${integrityStderrSuffix}`.trimStart() : stderrBase;
  const stderrTruncated = stderrTruncatedBase || Buffer.byteLength(stderr, "utf8") > MAX_CAPTURED_OUTPUT_BYTES;

  // ALWAYS remove -- scoped to exactly this container, never a global
  // prune. A cleanup failure is logged, visibly, but never overrides
  // the verification result already computed above.
  const removal = await removeContainer(containerId);
  if (!removal.removed) {
    console.error(`[docker-verify] WARNING: failed to remove container ${containerId} (${name}) after verification: ${removal.error}`);
  }

  return {
    outcome,
    exitCode,
    durationMs,
    stdout,
    stderr,
    stdoutTruncated,
    stderrTruncated,
    executionId: containerId,
    adapterName: "docker",
    cleanupError: removal.removed ? null : (removal.error ?? "container removal failed for an unknown reason"),
  };
}

/** Thin wrapper exposing the resolved image ID alongside the result, for the ONE caller (src/worker/handoff.ts's prepare/restore steps and src/worker/run-task.ts) that needs to bind it into the durable manifest. */
export async function verifyInDockerWithImageId(
  request: VerificationRequest,
  options: DockerVerifyOptions = {},
): Promise<{ result: VerificationResult; imageId: string | null }> {
  const opts = { ...DEFAULTS, ...options };
  let imageId: string | null = null;
  try {
    imageId = await resolveVerifyImageId(opts.imageTag);
  } catch {
    // Unresolvable -- `verifyInDocker` itself will independently hit and
    // report the SAME failure as `infrastructure_failure` below; this
    // wrapper does not duplicate that error, just reports `imageId` as
    // unavailable alongside it.
  }
  const result = await verifyInDocker(request, options);
  return { result, imageId };
}

export const dockerVerificationAdapter: VerificationAdapter = {
  name: "docker",
  verify: (request) => verifyInDocker(request),
};

export { DEFAULTS as DOCKER_VERIFY_DEFAULTS };
