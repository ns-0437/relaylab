-- Phase 1.2.1 patch, Part A.2: durable dead-letter quarantine. Additive
-- only -- 0001/0002/0003 are untouched, their recorded checksums in
-- schema_migrations still match exactly.
--
-- Previously, "dead-lettering" in src/messaging/consumer.ts meant: log a
-- line containing the word "dead-lettering" and ack the message. There
-- was no durable record of WHICH message, WHY it was rejected, or what
-- it contained -- an operator investigating a dead-lettered message had
-- nothing to query. This table is that durable record.
--
-- Identity: `nats_stream_seq` (the message's position in its JetStream
-- stream, stable across redeliveries of the SAME physical message) is
-- the dedup key, NOT the application `event_id` -- a malformed-JSON
-- message may not even contain a parseable event_id, so stream sequence
-- is the one identity that is ALWAYS available regardless of how badly
-- the payload is broken. `event_id`/`task_id` are recorded when known
-- (schema-invalid and max-redeliveries-exceeded cases decode far enough
-- to have them; malformed-JSON does not).
--
-- `payload_excerpt`: a BOUNDED (<=2000 bytes) copy of the raw message
-- bytes, stored here ONLY -- never in an application log line or a
-- Prometheus metric/label (see docs/OBSERVABILITY.md "dead-letter
-- quarantine: data-handling policy" for the explicit policy this
-- implements, including why this table is NOT redacted the way spans
-- are: diagnosing a malformed/invalid message requires seeing what was
-- actually sent, and there is no FIXED, typed shape (unlike
-- RelaylabSpanAttributes) that could redact an arbitrary quarantined
-- payload while still being useful for the diagnosis this table exists
-- for). Treat this table as holding the same class of business data as
-- the rest of this database -- restrict DB access accordingly; do not
-- assume its contents are credential-free.
CREATE TABLE IF NOT EXISTS consumer_quarantine (
  consumer_name    VARCHAR(191) NOT NULL,
  nats_stream_seq  BIGINT UNSIGNED NOT NULL,
  reason           ENUM('malformed_json', 'schema_invalid', 'max_redeliveries_exceeded') NOT NULL,
  event_id         VARCHAR(191) NULL,
  task_id          VARCHAR(191) NULL,
  delivery_attempt INT NOT NULL,
  payload_excerpt  VARBINARY(2000) NULL,
  payload_truncated TINYINT(1) NOT NULL DEFAULT 0,
  quarantined_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (consumer_name, nats_stream_seq),
  KEY idx_quarantine_reason (consumer_name, reason, quarantined_at)
) ENGINE=InnoDB;
