-- Phase 2B: durable record of each committed-checkpoint ownership
-- handoff between two workers. Additive only -- 0001-0007 are
-- untouched. Does NOT add a new Command/DomainEvent to
-- src/domain/domain.ts for the TRANSFER itself: unlike a checkpoint
-- publish (which is business state under an UNCHANGED epoch),
-- transferring ownership changes `tasks.owner_id`/`tasks.epoch`
-- themselves, so it is modeled as its own TaskStore method
-- (`transferOwnership`, src/db/task-store.ts) alongside `claimTask` --
-- NOT through `applyTaskCommand`. It reuses the EXISTING
-- `command_receipts` idempotency ledger (same table, same
-- (task_id, command_id) identity, `command_id` = the caller-chosen
-- `handoffId`) for duplicate-request detection and conflict rejection,
-- exactly as `recordGitCheckpoint` already does -- this table is where
-- the full transfer detail (which existing project convention calls
-- for -- see task_checkpoints' own header comment) lives, written in
-- the SAME transaction as the receipt insert and the `tasks` row update.
--
-- `RestoreAcknowledged` (the DESTINATION worker's post-restore
-- acknowledgement) needed NO new table: it is a genuine domain Command/
-- Event (src/domain/domain.ts), fenced under the CURRENT epoch through
-- the existing `applyTaskCommand` path exactly like `SubmitCandidate`/
-- `RecordVerification` -- its receipt lives in `command_receipts` and
-- its event in `outbox_events`, same as any other command.
CREATE TABLE IF NOT EXISTS task_handoffs (
  task_id            VARCHAR(191) NOT NULL,
  handoff_id         VARCHAR(191) NOT NULL,
  from_owner_id      VARCHAR(191) NOT NULL,
  to_owner_id        VARCHAR(191) NOT NULL,
  from_epoch         INT          NOT NULL,
  to_epoch           INT          NOT NULL,
  checkpoint_digest  CHAR(64)     NOT NULL,
  created_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (task_id, handoff_id),
  KEY idx_task_handoffs_to_owner (to_owner_id)
) ENGINE=InnoDB;
