// Pure domain model: no I/O, no Effect, no DB. Transition rules and invariants only.

export type TaskStatus = "unclaimed" | "owned" | "completed";

export interface TaskState {
  readonly taskId: string;
  readonly ownerId: string | null;
  readonly epoch: number;
  readonly leaseExpiresAtMs: number | null;
  readonly status: TaskStatus;
  readonly checkpointDigest: string | null;
  readonly candidateId: string | null;
  readonly verificationStatus: "pending" | "passed" | "failed" | null;
  readonly verificationAttempt: number;
  readonly seq: number;
}

export function newTask(taskId: string): TaskState {
  return {
    taskId,
    ownerId: null,
    epoch: 0,
    leaseExpiresAtMs: null,
    status: "unclaimed",
    checkpointDigest: null,
    candidateId: null,
    verificationStatus: null,
    verificationAttempt: 0,
    seq: 0,
  };
}

/**
 * Commands are the only way to mutate task state. Each command carries the
 * epoch the caller believes is current; appliers must re-check this against
 * authoritative DB state at the fencing boundary (see TaskStore), not here.
 */
export type Command =
  | { readonly type: "Checkpoint"; readonly digest: string }
  | { readonly type: "SubmitCandidate"; readonly candidateId: string }
  | { readonly type: "RecordVerification"; readonly candidateId: string; readonly passed: boolean }
  | { readonly type: "Complete" }
  | {
      /**
       * Phase 2B: the DESTINATION worker's acknowledgement that it
       * restored an already-published checkpoint from a transferable
       * artifact and re-verified it. Deliberately does NOT touch
       * `checkpointDigest`/`baseCommit`/`resultCommit`/`resultTree` --
       * those belong to the ORIGINAL checkpoint publish and must keep
       * their original provenance (requirement: "do not rewrite its
       * epoch to pretend B created it"). This command only records that
       * B's restoration happened, fenced under B's CURRENT epoch via the
       * SAME `applyTaskCommand` path every other command uses -- no
       * special-cased ownership logic needed here.
       */
      readonly type: "RestoreAcknowledged";
      readonly handoffId: string;
      readonly artifactDigest: string;
      readonly restoredCommit: string;
      readonly restoredTree: string;
    };

export type DomainEvent =
  | { readonly type: "CheckpointRecorded"; readonly taskId: string; readonly digest: string }
  | { readonly type: "CandidateSubmitted"; readonly taskId: string; readonly candidateId: string }
  | {
      readonly type: "VerificationRecorded";
      readonly taskId: string;
      readonly candidateId: string;
      readonly passed: boolean;
      readonly attempt: number;
    }
  | { readonly type: "TaskCompleted"; readonly taskId: string }
  | {
      readonly type: "RestoreAcknowledged";
      readonly taskId: string;
      readonly handoffId: string;
      readonly artifactDigest: string;
      readonly restoredCommit: string;
      readonly restoredTree: string;
    };

export class DomainRejection extends Error {
  readonly _tag = "DomainRejection";
  constructor(reason: string) {
    super(reason);
  }
}

/**
 * Pure state transition. Ownership/epoch/lease fencing happens OUTSIDE this
 * function, at the storage boundary, because it depends on wall-clock time
 * and authoritative DB state. This function only encodes rules that are
 * decidable from state + command alone (invariant set A-D, J).
 */
export function applyCommand(
  state: TaskState,
  command: Command,
): { state: TaskState; events: DomainEvent[] } {
  switch (command.type) {
    case "Checkpoint": {
      const next: TaskState = { ...state, checkpointDigest: command.digest, seq: state.seq + 1 };
      return { state: next, events: [{ type: "CheckpointRecorded", taskId: state.taskId, digest: command.digest }] };
    }
    case "SubmitCandidate": {
      const next: TaskState = {
        ...state,
        candidateId: command.candidateId,
        verificationStatus: "pending",
        seq: state.seq + 1,
      };
      return {
        state: next,
        events: [{ type: "CandidateSubmitted", taskId: state.taskId, candidateId: command.candidateId }],
      };
    }
    case "RecordVerification": {
      // Invariant I: verification cannot silently become stale. A result for
      // a candidate that is no longer the task's current candidate must be
      // rejected, not silently accepted or silently dropped.
      if (state.candidateId !== command.candidateId) {
        throw new DomainRejection(
          `verification result for stale candidate ${command.candidateId}, current candidate is ${state.candidateId ?? "none"}`,
        );
      }
      const attempt = state.verificationAttempt + 1;
      const next: TaskState = {
        ...state,
        verificationStatus: command.passed ? "passed" : "failed",
        verificationAttempt: attempt,
        seq: state.seq + 1,
      };
      return {
        state: next,
        events: [
          {
            type: "VerificationRecorded",
            taskId: state.taskId,
            candidateId: command.candidateId,
            passed: command.passed,
            attempt,
          },
        ],
      };
    }
    case "Complete": {
      if (state.verificationStatus !== "passed") {
        throw new DomainRejection("cannot complete a task without a passed verification");
      }
      const next: TaskState = { ...state, status: "completed", seq: state.seq + 1 };
      return { state: next, events: [{ type: "TaskCompleted", taskId: state.taskId }] };
    }
    case "RestoreAcknowledged": {
      const next: TaskState = { ...state, seq: state.seq + 1 };
      return {
        state: next,
        events: [
          {
            type: "RestoreAcknowledged",
            taskId: state.taskId,
            handoffId: command.handoffId,
            artifactDigest: command.artifactDigest,
            restoredCommit: command.restoredCommit,
            restoredTree: command.restoredTree,
          },
        ],
      };
    }
  }
}
