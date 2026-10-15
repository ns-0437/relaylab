// Phase 2C: the narrow verification interface every adapter implements
// -- extracted from the Phase 2A host-process implementation
// (src/worker/verify.ts, UNCHANGED, kept as its own adapter for tests
// that do not need Docker) so a SECOND adapter (src/worker/docker-verify.ts)
// can implement the exact same contract. Deliberately NOT a general
// plugin framework -- two concrete adapters, one shared result shape,
// no registry/factory indirection beyond that.
import { createHash } from "node:crypto";

/**
 * Five outcomes, not three -- this is the actual point of extracting
 * the interface (Part 1): `verification_failed` (the command ran to
 * completion and returned nonzero) must never be confused with
 * `infrastructure_failure` (the execution environment itself could not
 * be launched -- e.g. the Docker daemon is unreachable, the image is
 * missing, `docker create`/`docker start` itself failed) -- a worker
 * must NEVER treat the latter as proof the candidate is bad, and must
 * NEVER publish a checkpoint/acknowledgement for either. `cancelled` is
 * distinct from `timed_out`: a caller-requested abort (AbortSignal)
 * versus this adapter's OWN wall-clock bound expiring.
 */
export type VerificationOutcome = "passed" | "verification_failed" | "timed_out" | "cancelled" | "infrastructure_failure";

export interface VerificationResult {
  readonly outcome: VerificationOutcome;
  /** The verified command's real exit code, or null if it never ran to completion (timeout/cancel/infrastructure failure) or was killed by a signal. */
  readonly exitCode: number | null;
  readonly durationMs: number;
  readonly stdout: string;
  readonly stderr: string;
  readonly stdoutTruncated: boolean;
  readonly stderrTruncated: boolean;
  /**
   * The identity of THIS specific execution -- a real container ID for
   * the Docker adapter, a real child-process pid for the host-process
   * adapter. Distinct per invocation (even when the container/process
   * NAME is deterministic and reused across retries) -- this is what
   * lets two different executions of the exact same candidate (e.g.
   * A's own verification and B's independent re-verification of the
   * restored result) be told apart in evidence/logs.
   */
  readonly executionId: string;
  readonly adapterName: string;
  /**
   * Phase 2C correctness patch, Part 1: `null` means cleanup of this
   * execution's own resources was either not needed or CONFIRMED
   * successful; a non-null string names a cleanup failure that could
   * NOT be resolved (e.g. the container could not be confirmed
   * removed). This is deliberately a SEPARATE, independently
   * observable field from `outcome` -- a cleanup failure must never
   * overwrite or be confused with the actual verification outcome
   * already computed (a `passed` verification whose container
   * stubbornly failed to remove is still a `passed` verification, with
   * a cleanup problem recorded alongside it, not instead of it).
   */
  readonly cleanupError: string | null;
}

export interface VerificationRequest {
  /** The directory containing EXACTLY the materialized candidate input (see src/worker/docker-verify.ts "Preserve exact-code verification" for how this is produced) -- never the live worktree itself for the Docker adapter. */
  readonly inputDir: string;
  readonly taskId: string;
  readonly attemptId: string;
  readonly timeoutMs?: number;
  readonly signal?: AbortSignal;
}

export interface VerificationAdapter {
  readonly name: string;
  verify(request: VerificationRequest): Promise<VerificationResult>;
}

export const DEFAULT_VERIFY_TIMEOUT_MS = 15_000;
export const MAX_CAPTURED_OUTPUT_BYTES = 64 * 1024; // 64 KiB cap per stream, shared by every adapter

export function boundedCollector(maxBytes: number): { onData: (chunk: Buffer) => void; get: () => { text: string; truncated: boolean } } {
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

/** A short, deterministic, Docker-name-safe identity derived from (taskId, attemptId, role) -- used both for the container's `--name` (so restart reconciliation can find an abandoned one by name) and is stable across retries of the SAME logical execution. `taskId`/`attemptId` are already restricted to `[A-Za-z0-9_-]` by src/worker/workspace.ts's `assertSafePathSegment`, so no further sanitization is needed here -- this function does not re-validate that, it is the caller's job (see docker-verify.ts). */
export function deterministicExecutionName(taskId: string, attemptId: string, role: string): string {
  const raw = `relaylab-verify-${role}-${taskId}-${attemptId}`;
  if (raw.length <= 120) return raw;
  // Extremely long taskId/attemptId (beyond this fixture's normal
  // range): fall back to a stable hash suffix rather than silently
  // truncating in a way that could collide between two different
  // (taskId, attemptId) pairs sharing a long common prefix.
  const hash = createHash("sha256").update(raw).digest("hex").slice(0, 16);
  return `relaylab-verify-${role}-${hash}`;
}
