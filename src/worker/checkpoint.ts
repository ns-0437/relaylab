// Phase 2A: the COMMITTED-STATE checkpoint manifest. Explicitly does
// NOT capture dirty working-tree edits, staged changes, or untracked
// files -- only the state of a real, local Git COMMIT. That broader
// capture (Phase 2's "real Git worktree snapshots: local commits,
// working-tree changes, index state, explicitly allowlisted untracked
// files" from docs/ROADMAP.md) is explicitly a LATER milestone's scope,
// not this one's.
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { Effect } from "effect";
import { fingerprint, stableStringify } from "../domain/fingerprint.js";
import { TaskStore, type CommandReceipt } from "../db/task-store.js";
import { VERIFICATION_COMMAND_IDENTITY, type VerificationResult } from "./verify.js";

const execFileAsync = promisify(execFile);

/**
 * Phase 2C: bumped 1 -> 2 for the new, OPTIONAL
 * `verification.containerImageId` field (below). Backward compatible by
 * construction: the field is nullable/optional everywhere it is read
 * (`src/db/task-store.ts`'s shape validator never requires it), so a
 * manifest built under version 1 (no such field at all) and a manifest
 * built under version 2 with `containerImageId: null` (host-process
 * adapter) validate identically. No existing column/field was removed
 * or repurposed.
 */
export const MANIFEST_VERSION = 2;

/**
 * The manifest's OWN schema. Three DIFFERENT kinds of "ID" appear here,
 * deliberately kept distinct and never conflated:
 *   - Git object IDs (`baseCommit`, `resultCommit`, `resultTree`) --
 *     identities Git itself assigns, verifiable independently with
 *     `git cat-file`/`git rev-parse` against the real repository.
 *   - The manifest's OWN content digest (`manifestDigest`, computed by
 *     `computeManifestDigest` below, over the CANONICAL encoding of
 *     every OTHER field in this interface) -- an identity THIS PROJECT
 *     assigns to the manifest document itself, unrelated to any Git
 *     object hash.
 *   - Verification EVIDENCE (`verification.result`) -- the actual
 *     command/exit-code/output record proving the outcome, not an
 *     identity/hash at all.
 */
export interface CheckpointManifest {
  manifestVersion: number;
  taskId: string;
  attemptId: string;
  epoch: number;
  baseCommit: string;
  resultCommit: string;
  resultTree: string;
  verification: {
    command: { executable: string; args: string[]; version: number };
    outcome: "passed" | "failed";
    exitCode: number | null;
    durationMs: number;
    /**
     * Phase 2C: the RESOLVED Docker image ID (`sha256:...`, never a
     * mutable tag) the verification command actually ran under, or
     * `null` for the host-process adapter (which has no image at all --
     * see src/worker/verify.ts, still used by tests that don't need
     * Docker). This is the "record enough evidence to associate the
     * result with ... image ID" requirement -- never the tag string
     * alone, which could silently point at a different build over time.
     */
    containerImageId: string | null;
  };
  runtimeFingerprint: {
    nodeVersion: string;
    execPath: string;
    platform: string;
    gitVersion: string;
  };
}

/** sha256 over the manifest's STABLE (recursively key-sorted) JSON encoding -- the same `fingerprint()` helper this project already uses for command-idempotency fingerprints (src/domain/fingerprint.ts), reused here as the "stable encoding" Part F asks for rather than inventing a second canonicalization scheme. */
export function computeManifestDigest(manifest: CheckpointManifest): string {
  return fingerprint(manifest);
}

export function manifestToStableJson(manifest: CheckpointManifest): string {
  return stableStringify(manifest);
}

export async function buildRuntimeFingerprint(env: NodeJS.ProcessEnv): Promise<CheckpointManifest["runtimeFingerprint"]> {
  const { stdout } = await execFileAsync("git", ["--version"], { env });
  return {
    nodeVersion: process.version,
    execPath: process.execPath,
    platform: process.platform,
    gitVersion: stdout.trim(),
  };
}

export function buildManifest(args: {
  taskId: string;
  attemptId: string;
  epoch: number;
  baseCommit: string;
  resultCommit: string;
  resultTree: string;
  verification: VerificationResult;
  runtimeFingerprint: CheckpointManifest["runtimeFingerprint"];
  /** Phase 2C: the resolved Docker image ID, or `null` for the host-process adapter. Never a tag string. */
  containerImageId: string | null;
}): CheckpointManifest {
  if (args.verification.outcome === "timeout") {
    throw new Error("buildManifest: refusing to build a checkpoint manifest for a timed-out verification -- the caller must not publish a checkpoint in this case");
  }
  return {
    manifestVersion: MANIFEST_VERSION,
    taskId: args.taskId,
    attemptId: args.attemptId,
    epoch: args.epoch,
    baseCommit: args.baseCommit,
    resultCommit: args.resultCommit,
    resultTree: args.resultTree,
    verification: {
      command: { executable: VERIFICATION_COMMAND_IDENTITY.executable, args: [...VERIFICATION_COMMAND_IDENTITY.argsTemplate], version: VERIFICATION_COMMAND_IDENTITY.version },
      outcome: args.verification.outcome,
      exitCode: args.verification.exitCode,
      durationMs: args.verification.durationMs,
      containerImageId: args.containerImageId,
    },
    runtimeFingerprint: args.runtimeFingerprint,
  };
}

/**
 * Persists a built manifest via `TaskStore.recordGitCheckpoint` -- the
 * SAME transactional, fencing-checked, idempotent path every other
 * durable task mutation in this project goes through (see
 * src/db/task-store.ts). `commandId` is the caller's chosen idempotency
 * key (see src/worker-main.ts for how it is derived deterministically
 * per task/attempt, which is what makes a retried publish after a lost
 * acknowledgement safe -- Part G).
 *
 * Phase 2A.2, section 3: this function passes ONLY `manifestDigest` and
 * `manifestJson` (plus the request's own taskId/ownerId/epoch/commandId/
 * attemptId) to the store -- it no longer ALSO flattens
 * baseCommit/resultCommit/resultTree/verification/runtimeFingerprint out
 * of `args.manifest` into separate arguments. The store itself now
 * parses, validates, and derives every persisted column from
 * `manifestJson` (see `parseAndValidateGitCheckpointManifest` in
 * src/db/task-store.ts) -- this is the single source of truth, so there
 * is no second, independently-contradictable representation for this
 * function to keep in sync.
 */
export function publishCheckpoint(args: {
  taskId: string;
  ownerId: string;
  epoch: number;
  commandId: string;
  attemptId: string;
  manifest: CheckpointManifest;
}): Effect.Effect<CommandReceipt, unknown, TaskStore> {
  return Effect.gen(function* () {
    const store = yield* TaskStore;
    const digest = computeManifestDigest(args.manifest);
    return yield* store.recordGitCheckpoint({
      taskId: args.taskId,
      ownerId: args.ownerId,
      epoch: args.epoch,
      commandId: args.commandId,
      attemptId: args.attemptId,
      manifestDigest: digest,
      manifestJson: manifestToStableJson(args.manifest),
    });
  });
}
