-- Phase 1.2 audit, Part B: adds a nullable trace-context carrier column
-- to outbox_events. Additive only -- 0001/0002 are untouched, so their
-- recorded checksums in schema_migrations still match exactly.
--
-- Deliberately a SEPARATE column from `payload`, not merged into it:
-- `payload` is the immutable business event that command/event
-- idempotency fingerprints and content hashes are computed over (see
-- src/domain/fingerprint.ts, src/messaging/consumer.ts content_hash).
-- Telemetry metadata must never be able to perturb either of those --
-- changing trace IDs across a retry/redelivery must never look like a
-- content change. `trace_carrier` holds at most two W3C fields
-- (traceparent, tracestate) and nothing else; see
-- src/telemetry/propagation.ts for the one place that reads/writes it.
ALTER TABLE outbox_events
  ADD COLUMN trace_carrier JSON NULL AFTER payload;
