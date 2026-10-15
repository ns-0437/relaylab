-- Phase 1.1 audit fixes:
--   1. command_receipts gains owner_id + committed_epoch, so a replay can be
--      authorized against the identity that originally committed it,
--      independently of current ownership/epoch fencing (see
--      docs/decisions/0004-receipt-authorization.md).
--   2. consumer_processed gains content_hash, so an event_id reused with
--      different content is detected instead of silently treated as an
--      ordinary duplicate (see docs/decisions/0005-event-identity-conflict.md).
--   3. task_consumer_pending + consumer_event_conflicts are new tables
--      supporting the durable-pending-event gap policy (see
--      docs/decisions/0003-event-ordering.md, revised).
--   4. task_consumer_state.last_seq is renamed to last_applied_seq (seq up
--      to which the projection is CONTIGUOUSLY complete -- it no longer
--      advances past a gap) and gains pending_count for visibility.

ALTER TABLE command_receipts
  ADD COLUMN owner_id VARCHAR(191) NOT NULL DEFAULT '' AFTER command_id,
  ADD COLUMN committed_epoch INT NOT NULL DEFAULT 0 AFTER owner_id;

ALTER TABLE consumer_processed
  ADD COLUMN content_hash CHAR(64) NOT NULL DEFAULT '' AFTER seq;

CREATE TABLE IF NOT EXISTS task_consumer_pending (
  consumer_name VARCHAR(191) NOT NULL,
  task_id       VARCHAR(191) NOT NULL,
  seq           INT          NOT NULL,
  event_id      VARCHAR(191) NOT NULL,
  payload       JSON         NOT NULL,
  received_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (consumer_name, task_id, seq)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS consumer_event_conflicts (
  consumer_name         VARCHAR(191) NOT NULL,
  event_id              VARCHAR(191) NOT NULL,
  first_content_hash    CHAR(64)     NOT NULL,
  conflicting_content_hash CHAR(64)  NOT NULL,
  detected_at           DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (consumer_name, event_id, conflicting_content_hash)
) ENGINE=InnoDB;

ALTER TABLE task_consumer_state
  CHANGE COLUMN last_seq last_applied_seq INT NOT NULL DEFAULT 0,
  ADD COLUMN pending_count INT NOT NULL DEFAULT 0 AFTER gap_detected;

-- Publisher leader lock (section 4): a singleton row claimed the same way
-- a task is claimed (epoch + lease), so only one publisher process is
-- ever the active leader at a time. See
-- docs/decisions/0006-publisher-leader-lock.md.
CREATE TABLE IF NOT EXISTS publisher_leader (
  lock_name        VARCHAR(191) NOT NULL PRIMARY KEY,
  owner_id         VARCHAR(191) NULL,
  epoch            INT          NOT NULL DEFAULT 0,
  lease_expires_at DATETIME(3)  NULL
) ENGINE=InnoDB;

INSERT IGNORE INTO publisher_leader (lock_name, epoch) VALUES ('default', 0);
