-- Phase 2A: durable Git checkpoint manifests. Additive only -- 0001-0006
-- are untouched, their recorded checksums in schema_migrations still
-- match exactly. Does NOT change src/domain/domain.ts's Command/
-- DomainEvent union -- the existing `Checkpoint { digest }` command and
-- `CheckpointRecorded` event are reused unchanged (the manifest's own
-- content digest, see src/worker/checkpoint.ts, becomes that command's
-- `digest`), so the task/receipt/outbox protocol itself is unmodified.
-- This table is where the FULL manifest (everything the existing
-- `Checkpoint` command's single `digest` string cannot carry) actually
-- lives, written in the SAME transaction as the receipt/task-update/
-- outbox-insert via TaskStore.recordGitCheckpoint (src/db/task-store.ts).
--
-- Primary key (task_id, command_id) deliberately reuses the SAME
-- idempotency identity as command_receipts -- `command_id` is the
-- caller-chosen deterministic id (see src/worker/checkpoint.ts), so a
-- checkpoint publish retried after a lost acknowledgement is detected as
-- a duplicate by BOTH tables under the exact same key, not two separate
-- idempotency schemes that could drift apart.
CREATE TABLE IF NOT EXISTS task_checkpoints (
  task_id               VARCHAR(191) NOT NULL,
  command_id            VARCHAR(191) NOT NULL,
  manifest_version       INT          NOT NULL,
  attempt_id             VARCHAR(191) NOT NULL,
  epoch                  INT          NOT NULL,
  base_commit            CHAR(40)     NOT NULL,
  result_commit          CHAR(40)     NOT NULL,
  result_tree            CHAR(40)     NOT NULL,
  verification_command   JSON         NOT NULL,
  verification_outcome   ENUM('passed', 'failed') NOT NULL,
  runtime_fingerprint     JSON         NOT NULL,
  manifest_digest         CHAR(64)     NOT NULL,
  manifest_json           JSON         NOT NULL,
  created_at              DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (task_id, command_id),
  KEY idx_task_checkpoints_result_commit (result_commit)
) ENGINE=InnoDB;
