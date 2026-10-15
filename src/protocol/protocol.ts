import { Schema } from "effect";

// Runtime-validated envelopes for anything crossing a process or message
// boundary (NATS payloads, command submissions). Internal in-process calls
// use the plain TS types in domain/domain.ts directly.

export const CommandEnvelope = Schema.Struct({
  version: Schema.Literal(1),
  taskId: Schema.String.pipe(Schema.minLength(1)),
  commandId: Schema.String.pipe(Schema.minLength(1)),
  epoch: Schema.Int.pipe(Schema.nonNegative()),
  ownerId: Schema.String.pipe(Schema.minLength(1)),
  payload: Schema.Union(
    Schema.Struct({ type: Schema.Literal("Checkpoint"), digest: Schema.String }),
    Schema.Struct({ type: Schema.Literal("SubmitCandidate"), candidateId: Schema.String }),
    Schema.Struct({
      type: Schema.Literal("RecordVerification"),
      candidateId: Schema.String,
      passed: Schema.Boolean,
    }),
    Schema.Struct({ type: Schema.Literal("Complete") }),
  ),
});
export type CommandEnvelope = typeof CommandEnvelope.Type;

// Event envelope published to the outbox / NATS subject. `seq` is the
// task-scoped sequence number used for per-task ordering and gap detection
// (invariant G); `eventId` is the globally stable identity used for
// consumer-side deduplication (invariant E/F).
export const EventEnvelope = Schema.Struct({
  version: Schema.Literal(1),
  eventId: Schema.String.pipe(Schema.minLength(1)),
  taskId: Schema.String.pipe(Schema.minLength(1)),
  seq: Schema.Int.pipe(Schema.nonNegative()),
  type: Schema.String,
  payload: Schema.Unknown,
  createdAtMs: Schema.Int,
});
export type EventEnvelope = typeof EventEnvelope.Type;

export const decodeCommandEnvelope = Schema.decodeUnknownEither(CommandEnvelope);
export const decodeEventEnvelope = Schema.decodeUnknownEither(EventEnvelope);
