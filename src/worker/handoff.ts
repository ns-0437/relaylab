// Phase 2B: orchestrates the committed-checkpoint handoff protocol
// (docs/decisions/0011-handoff-protocol.md) on top of the existing
// TaskStore/artifact primitives. Three composable pieces, matching the
// three roles in the protocol: A prepares an artifact and requests
// transfer; B restores and acknowledges. Reuses OpenTelemetry the same
// way src/worker/run-task.ts already does -- one span per protocol
// step, task/handoff IDs as span attributes, never a new dashboard or a
// task ID in a metric label (Part H).
import path from "node:path";
import type mysql from "mysql2/promise";
import { Effect } from "effect";
import { TaskStore, TaskStoreLive, type TransferReceipt, type CommandReceipt } from "../db/task-store.js";
import { createHandoffArtifact, restoreHandoffArtifact, validateHandoffArtifact, type HandoffArtifactManifest } from "./artifact.js";
import { getTracer } from "../telemetry/init.js";
import { hitFailpoint } from "../testing/failpoint.js";
import { assertMaterializedTreeMatches, materializeCandidateInput, verifyInDockerWithImageId } from "./docker-verify.js";
import type { VerificationResult } from "./verification.js";

export function handoffCommandId(taskId: string, handoffId: string): string {
  return `handoff-${taskId}-${handoffId}`;
}

export function restoreAckCommandId(taskId: string, handoffId: string): string {
  return `restore-ack-${taskId}-${handoffId}`;
}

/** A-side, Part D: builds the one transferable artifact for an already-published checkpoint. Purely local -- no DB write happens here. */
export async function prepareHandoffArtifact(args: {
  taskId: string;
  handoffId: string;
  worktreeDir: string;
  env: NodeJS.ProcessEnv;
  resultRef: string;
  sourceCheckpointCommandId: string;
  sourceManifestDigest: string;
  baseCommit: string;
  resultCommit: string;
  resultTree: string;
  outDir: string;
}): Promise<{ artifactPath: string; manifestPath: string; manifest: HandoffArtifactManifest }> {
  const tracer = getTracer();
  return tracer.startActiveSpan("relaylab.handoff.prepare_artifact", async (span) => {
    try {
      span.setAttribute("relaylab.task_id", args.taskId);
      span.setAttribute("relaylab.handoff_id", args.handoffId);
      return await createHandoffArtifact(args);
    } finally {
      span.end();
    }
  });
}

/** A-side (via the coordinator), Part B step 3: the one atomic ownership-changing call. Thin wrapper purely for a consistent span name/attributes alongside every other protocol step. */
export async function requestOwnershipTransfer(
  pool: mysql.Pool,
  args: { taskId: string; handoffId: string; fromOwnerId: string; fromEpoch: number; toOwnerId: string; checkpointDigest: string; artifactDigest: string; leaseMs: number },
): Promise<TransferReceipt> {
  const tracer = getTracer();
  return tracer.startActiveSpan("relaylab.handoff.transfer_ownership", async (span) => {
    try {
      span.setAttribute("relaylab.task_id", args.taskId);
      span.setAttribute("relaylab.handoff_id", args.handoffId);
      const program = Effect.gen(function* () {
        const store = yield* TaskStore;
        return yield* store.transferOwnership(args);
      });
      return await Effect.runPromise(Effect.provide(program, TaskStoreLive(pool)));
    } finally {
      span.end();
    }
  });
}

export type RestoreOutcome =
  | { kind: "restored"; receipt: CommandReceipt; manifest: HandoffArtifactManifest; restoredCommit: string; restoredTree: string; verification: VerificationResult; imageId: string | null }
  | { kind: "verification_failed"; verification: VerificationResult }
  | { kind: "verification_timeout"; verification: VerificationResult }
  | { kind: "verification_cancelled"; verification: VerificationResult }
  | { kind: "verification_infrastructure_failure"; verification: VerificationResult };

/**
 * B-side, Part B steps 4-6 / Part F: validate the artifact, restore it
 * into B's own independent repo, rerun the FIXED verification command
 * against B's OWN freshly-restored workspace (never trusting A's prior
 * verification result as proof that B's environment works), and
 * publish an idempotent restore acknowledgement under B's CURRENT
 * epoch. Every step after validation binds explicitly to the artifact's
 * manifest values -- B never "declares restore success" before both the
 * artifact validation AND the checkout-result equality checks
 * (src/worker/artifact.ts's `restoreHandoffArtifact`) have actually
 * passed.
 */
export async function restoreAndAcknowledge(
  pool: mysql.Pool,
  args: {
    taskId: string;
    handoffId: string;
    ownerId: string;
    epoch: number;
    artifactPath: string;
    manifestPath: string;
    destRepoDir: string;
    env: NodeJS.ProcessEnv;
    /** Demo UI milestone: propagated into B's Docker re-verification. Checked again, explicitly, immediately before the acknowledgement write below -- but NEVER after that write succeeds: a durably recorded acknowledgement is never retroactively treated as cancelled. */
    signal?: AbortSignal;
    /** Test-only deterministic barrier: invoked at the exact moment B's re-verification container is confirmed running -- never a sleep-based guess. Never used by any real caller. */
    onVerifyContainerCreated?: (containerId: string) => void | Promise<void>;
  },
): Promise<RestoreOutcome> {
  const tracer = getTracer();
  return tracer.startActiveSpan("relaylab.handoff.restore", async (span): Promise<RestoreOutcome> => {
    try {
      span.setAttribute("relaylab.task_id", args.taskId);
      span.setAttribute("relaylab.handoff_id", args.handoffId);

      const manifest = await tracer.startActiveSpan("relaylab.handoff.validate_artifact", async (s) => {
        try {
          return await validateHandoffArtifact(args.artifactPath, args.manifestPath);
        } finally {
          s.end();
        }
      });

      const { restoredCommit, restoredTree } = await tracer.startActiveSpan("relaylab.handoff.destination_restore", async (s) => {
        try {
          return await restoreHandoffArtifact({ artifactPath: args.artifactPath, manifest, destRepoDir: args.destRepoDir, env: args.env });
        } finally {
          s.end();
        }
      });

      await hitFailpoint("restore-after-checkout-before-ack", {
        taskId: args.taskId,
        handoffId: args.handoffId,
        ownerId: args.ownerId,
        epoch: args.epoch,
        restoredCommit,
        restoredTree,
      });

      // Phase 2C: B re-verifies in its OWN isolated container, against
      // a FRESH export of the restored commit -- never trusting A's
      // prior verification result as proof B's environment works, and
      // never verifying against B's live destination repo directly
      // (same "materialize, don't mount the live tree" discipline as
      // worker A's run-task.ts).
      const { result: verification, imageId } = await tracer.startActiveSpan("relaylab.handoff.verify", async (s) => {
        try {
          const materialized = await materializeCandidateInput(args.destRepoDir, restoredCommit, args.env, path.join(path.dirname(args.destRepoDir), ".verify-scratch"));
          try {
            // Phase 2C.1, Part 1: same identity-mismatch regression as
            // run-task.ts (A) -- B must never let a verification of a
            // DIFFERENT tree than the one it just restored authorize a
            // restoration acknowledgement.
            assertMaterializedTreeMatches(materialized, restoredTree, `task ${args.taskId} handoff ${args.handoffId}`);
            return await verifyInDockerWithImageId(
              { taskId: args.taskId, attemptId: args.handoffId, inputDir: materialized.exportDir, ...(args.signal ? { signal: args.signal } : {}) },
              {
                onContainerCreated: async (containerId) => {
                  // Test-only deterministic barrier -- see
                  // tests/integration/cancellation-barriers.test.ts.
                  await args.onVerifyContainerCreated?.(containerId);
                },
              },
            );
          } finally {
            await materialized.cleanup();
          }
        } finally {
          s.end();
        }
      });

      if (verification.outcome === "timed_out") {
        span.setAttribute("relaylab.outcome", "verification_timeout");
        return { kind: "verification_timeout", verification };
      }
      if (verification.outcome === "cancelled") {
        span.setAttribute("relaylab.outcome", "verification_cancelled");
        return { kind: "verification_cancelled", verification };
      }
      if (verification.outcome === "infrastructure_failure") {
        span.setAttribute("relaylab.outcome", "verification_infrastructure_failure");
        return { kind: "verification_infrastructure_failure", verification };
      }
      if (verification.outcome !== "passed") {
        span.setAttribute("relaylab.outcome", "verification_failed");
        return { kind: "verification_failed", verification };
      }

      // Checked explicitly HERE, one last time -- re-verification
      // genuinely passed, but if cancellation arrived during it, the
      // acknowledgement write below must not happen. This check is
      // NEVER repeated after the write below succeeds: a durably
      // recorded acknowledgement is never retroactively undone or
      // reported as cancelled.
      if (args.signal?.aborted) {
        span.setAttribute("relaylab.outcome", "verification_cancelled");
        return { kind: "verification_cancelled", verification };
      }

      const receipt = await tracer.startActiveSpan("relaylab.handoff.acknowledge_restore", async (s) => {
        try {
          const program = Effect.gen(function* () {
            const store = yield* TaskStore;
            return yield* store.recordRestoreAcknowledgement({
              taskId: args.taskId,
              // `task_handoffs.handoff_id` is keyed by whatever string
              // `transferOwnership` was called with -- every call site in
              // this codebase passes `handoffCommandId(taskId, handoffId)`
              // there (see `requestOwnershipTransfer`'s callers), so the
              // lookup key here MUST be wrapped the same way, not the raw
              // `args.handoffId` this function's own caller uses to build
              // the ACK's unrelated `commandId` below.
              handoffId: handoffCommandId(args.taskId, args.handoffId),
              ownerId: args.ownerId,
              epoch: args.epoch,
              commandId: restoreAckCommandId(args.taskId, args.handoffId),
              artifactDigest: manifest.artifactDigest,
              restoredCommit,
              restoredTree,
            });
          });
          return await Effect.runPromise(Effect.provide(program, TaskStoreLive(pool)));
        } finally {
          s.end();
        }
      });

      span.setAttribute("relaylab.outcome", "restored");
      return { kind: "restored", receipt, manifest, restoredCommit, restoredTree, verification, imageId };
    } finally {
      span.end();
    }
  });
}
