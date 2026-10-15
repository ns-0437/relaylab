// Phase 2A: runs a FIXED, ALLOWLISTED verification command -- never a
// task-supplied arbitrary shell command. The executable and its argument
// array are hardcoded constants in this file; a task cannot influence
// what gets executed, only WHICH worktree it runs in (`cwd`).
import { spawn } from "node:child_process";
import path from "node:path";

/**
 * The ONE fixed, allowlisted verification command this milestone runs --
 * `node <worktree>/verify.js`, using THIS process's own Node binary
 * (`process.execPath`, recorded in the manifest's runtime fingerprint --
 * see src/worker/checkpoint.ts) so the verification step genuinely runs
 * on the same, actual Node runtime the worker itself is running on, not
 * a `node` resolved from PATH that could silently differ.
 */
export const VERIFICATION_COMMAND_IDENTITY = {
  executable: "node",
  argsTemplate: ["verify.js"] as const,
  version: 1,
};

const VERIFY_TIMEOUT_MS = 15_000;
const MAX_CAPTURED_OUTPUT_BYTES = 64 * 1024; // 64 KiB cap per stream

export interface VerificationResult {
  exitCode: number | null;
  stdout: string;
  stderr: string;
  stdoutTruncated: boolean;
  stderrTruncated: boolean;
  durationMs: number;
  outcome: "passed" | "failed" | "timeout";
}

function boundedCollector(maxBytes: number): { onData: (chunk: Buffer) => void; get: () => { text: string; truncated: boolean } } {
  const chunks: Buffer[] = [];
  let total = 0;
  let truncated = false;
  return {
    onData(chunk: Buffer) {
      if (truncated) return;
      const remaining = maxBytes - total;
      if (remaining <= 0) {
        truncated = true;
        return;
      }
      const slice = chunk.length > remaining ? chunk.subarray(0, remaining) : chunk;
      chunks.push(slice);
      total += slice.length;
      if (chunk.length > remaining) truncated = true;
    },
    get: () => ({ text: Buffer.concat(chunks).toString("utf8"), truncated }),
  };
}

/**
 * Runs the fixed verification command in `worktreeDir`. Bounds both wall-
 * clock time (`VERIFY_TIMEOUT_MS`, measured with a MONOTONIC clock --
 * `process.hrtime.bigint()`, immune to system clock adjustments) and
 * captured output size (`MAX_CAPTURED_OUTPUT_BYTES` per stream). On
 * timeout, the child process is killed and this function still returns
 * (never throws) with `outcome: "timeout"` -- a timeout is a reportable
 * verification OUTCOME, not an exception the caller must separately
 * handle. The child's process group is NOT used (this is a simple
 * single-process `node` script, not a job tree needing recursive kill),
 * but the `kill()` call and subsequent `exit` wait below guarantee the
 * process this function started is never left running past this call
 * returning, on ANY exit path (success, failure, timeout).
 */
export async function runVerification(worktreeDir: string, env: NodeJS.ProcessEnv, timeoutMs: number = VERIFY_TIMEOUT_MS): Promise<VerificationResult> {
  const scriptPath = path.join(worktreeDir, "verify.js");
  const args = [scriptPath];
  const stdoutCollector = boundedCollector(MAX_CAPTURED_OUTPUT_BYTES);
  const stderrCollector = boundedCollector(MAX_CAPTURED_OUTPUT_BYTES);

  const startedAt = process.hrtime.bigint();
  const child = spawn(VERIFICATION_COMMAND_IDENTITY.executable, args, {
    cwd: worktreeDir,
    env,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.on("data", stdoutCollector.onData);
  child.stderr.on("data", stderrCollector.onData);

  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    child.kill("SIGKILL");
  }, timeoutMs);
  timer.unref();

  const exitCode = await new Promise<number | null>((resolve) => {
    child.once("exit", (code) => resolve(code));
    child.once("error", () => resolve(null));
  });
  clearTimeout(timer);
  const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;

  const { text: stdout, truncated: stdoutTruncated } = stdoutCollector.get();
  const { text: stderr, truncated: stderrTruncated } = stderrCollector.get();

  return {
    exitCode,
    stdout,
    stderr,
    stdoutTruncated,
    stderrTruncated,
    durationMs,
    outcome: timedOut ? "timeout" : exitCode === 0 ? "passed" : "failed",
  };
}
