-- Phase 1.2.1 monitoring-reliability patch, section 3: the quarantine
-- table's dedup key (consumer_name, nats_stream_seq) was NOT safe across
-- a NATS stream delete-and-recreate -- a recreated stream hands out LOW
-- sequence numbers again starting from 1, and nothing in this project
-- couples NATS stream lifecycle to the MySQL quarantine table's
-- lifecycle (tests, in particular, can reset one independently of the
-- other -- see tests/integration/helpers.ts `truncateAll` vs
-- `resetStream`). A new quarantine record from a NEW stream incarnation
-- could therefore silently collide with, and be discarded by `INSERT
-- IGNORE` in favor of, an OLD row left over from a PREVIOUS incarnation
-- that happened to reuse the same low sequence number.
--
-- Fixed by widening the identity to include the stream's own name AND
-- its NATS-assigned creation timestamp ("generation" -- see
-- src/messaging/nats-client.ts `StreamIdentity`, src/messaging/quarantine.ts
-- `StreamQuarantineIdentity`) -- the simplest identifier that already
-- exists, requires no new generation-tracking subsystem, and changes
-- exactly when it needs to (a stream delete+recreate), never otherwise.
--
-- Additive, but NOT purely so -- the PRIMARY KEY itself must change, so
-- this uses DROP PRIMARY KEY / ADD PRIMARY KEY rather than only adding
-- columns. Existing rows (from before this migration, if any existed in
-- a given environment) get `stream_name`/`stream_generation` default
-- values of '' -- acceptable because this table has no cross-row
-- business meaning tied to those defaults (each row is independently
-- diagnostic), and this is dev-only evidence data, not state any
-- invariant in this project depends on. 0001-0004 remain untouched;
-- their recorded checksums in schema_migrations still match exactly.
ALTER TABLE consumer_quarantine
  ADD COLUMN stream_name VARCHAR(191) NOT NULL DEFAULT '' AFTER consumer_name,
  ADD COLUMN stream_generation VARCHAR(64) NOT NULL DEFAULT '' AFTER stream_name;

ALTER TABLE consumer_quarantine
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (consumer_name, stream_name, stream_generation, nats_stream_seq);
