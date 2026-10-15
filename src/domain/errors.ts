// Structured, typed failures. Every failure that can cross a service boundary
// is one of these so callers can switch on `_tag` instead of string-matching.

export class EpochStale extends Error {
  readonly _tag = "EpochStale";
  readonly retryable = false;
  constructor(
    readonly taskId: string,
    readonly suppliedEpoch: number,
    readonly currentEpoch: number,
  ) {
    super(`epoch ${suppliedEpoch} is stale for task ${taskId}, current epoch is ${currentEpoch}`);
  }
}

export class NotOwner extends Error {
  readonly _tag = "NotOwner";
  readonly retryable = false;
  constructor(
    readonly taskId: string,
    readonly suppliedOwnerId: string,
  ) {
    super(`${suppliedOwnerId} is not the current owner of task ${taskId}`);
  }
}

export class LeaseExpired extends Error {
  readonly _tag = "LeaseExpired";
  readonly retryable = false;
  constructor(readonly taskId: string) {
    super(`lease for task ${taskId} has expired`);
  }
}

export class ClaimLost extends Error {
  readonly _tag = "ClaimLost";
  readonly retryable = true;
  constructor(readonly taskId: string) {
    super(`task ${taskId} is owned by another worker with a current lease`);
  }
}

export class NotAuthorized extends Error {
  readonly _tag = "NotAuthorized";
  readonly retryable = false;
  constructor(
    readonly taskId: string,
    readonly commandId: string,
  ) {
    super(
      `command ${commandId} for task ${taskId} was originally committed by a different owner identity; ` +
        `refusing to return its receipt to this caller`,
    );
  }
}

export class CommandConflict extends Error {
  readonly _tag = "CommandConflict";
  readonly retryable = false;
  constructor(
    readonly taskId: string,
    readonly commandId: string,
  ) {
    super(`command ${commandId} for task ${taskId} was already used with a different payload`);
  }
}

export class CommandRejected extends Error {
  readonly _tag = "CommandRejected";
  readonly retryable = false;
  constructor(readonly reason: string) {
    super(reason);
  }
}

export class TaskNotFound extends Error {
  readonly _tag = "TaskNotFound";
  readonly retryable = false;
  constructor(readonly taskId: string) {
    super(`task ${taskId} not found`);
  }
}

export class TransientStoreError extends Error {
  readonly _tag = "TransientStoreError";
  readonly retryable = true;
  constructor(
    message: string,
    readonly sourceError: unknown,
  ) {
    super(message);
  }
}

export class PermanentStoreError extends Error {
  readonly _tag = "PermanentStoreError";
  readonly retryable = false;
  constructor(
    message: string,
    readonly sourceError: unknown,
  ) {
    super(message);
  }
}

export class SchemaValidationFailed extends Error {
  readonly _tag = "SchemaValidationFailed";
  readonly retryable = false;
  constructor(message: string) {
    super(message);
  }
}

export type StoreError =
  | EpochStale
  | NotOwner
  | LeaseExpired
  | ClaimLost
  | NotAuthorized
  | CommandConflict
  | CommandRejected
  | TaskNotFound
  | TransientStoreError
  | PermanentStoreError;
