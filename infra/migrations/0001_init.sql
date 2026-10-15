-- RelayLab Phase 1 schema. MySQL 8.0+. InnoDB throughout (default) so that
-- row-level locking provides the mutual-exclusion guarantees the ownership
-- and idempotency invariants depend on.

CREATE TABLE IF NOT EXISTS tasks (
  task_id             VARCHAR(191)  NOT NULL PRIMARY KEY,
  owner_id            VARCHAR(191)  NULL,
  epoch               INT           NOT NULL DEFAULT 0,
  lease_expires_at    DATETIME(3)   NULL,
  status              ENUM('unclaimed', 'owned', 'completed') NOT NULL DEFAULT 'unclaimed',
  checkpoint_digest   VARCHAR(191)  NULL,
  candidate_id        VARCHAR(191)  NULL,
  verification_status ENUM('pending', 'passed', 'failed') NULL,
  verification_attempt INT          NOT NULL DEFAULT 0,
  seq                 INT           NOT NULL DEFAULT 0,
  updated_at          DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;

-- Command receipts: the idempotency ledger. Unique (task_id, command_id)
-- is the mechanism that makes "retry the same command" safe to re-send
-- concurrently -- the second insert fails fast with a duplicate-key error
-- rather than racing the transition logic.
CREATE TABLE IF NOT EXISTS command_receipts (
  task_id       VARCHAR(191) NOT NULL,
  command_id    VARCHAR(191) NOT NULL,
  fingerprint   CHAR(64)     NOT NULL,
  result        JSON         NOT NULL,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (task_id, command_id)
) ENGINE=InnoDB;

-- Outbox: written in the SAME transaction as the task mutation and the
-- command receipt (see TaskStore.applyCommand). `id` is a global monotonic
-- key the single Phase-1 publisher polls in order; `seq` is per-task order.
CREATE TABLE IF NOT EXISTS outbox_events (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_id      VARCHAR(191) NOT NULL,
  task_id       VARCHAR(191) NOT NULL,
  seq           INT          NOT NULL,
  type          VARCHAR(191) NOT NULL,
  payload       JSON         NOT NULL,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  published_at  DATETIME(3)  NULL,
  UNIQUE KEY uq_outbox_event_id (event_id),
  UNIQUE KEY uq_outbox_task_seq (task_id, seq),
  KEY idx_outbox_unpublished (published_at, id)
) ENGINE=InnoDB;

-- Consumer-side durable dedup record. Written in the SAME transaction as
-- the local effect below (task_counters), and only ACKed to the broker
-- after that transaction commits. This is what makes redelivery safe.
CREATE TABLE IF NOT EXISTS consumer_processed (
  consumer_name VARCHAR(191) NOT NULL,
  event_id      VARCHAR(191) NOT NULL,
  task_id       VARCHAR(191) NOT NULL,
  seq           INT          NOT NULL,
  processed_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (consumer_name, event_id)
) ENGINE=InnoDB;

-- The consumer's local durable effect: a per-task, per-consumer projection
-- used to make "did redelivery cause a duplicate effect" observable in
-- tests. last_seq supports gap detection (invariant G).
CREATE TABLE IF NOT EXISTS task_consumer_state (
  consumer_name VARCHAR(191) NOT NULL,
  task_id       VARCHAR(191) NOT NULL,
  last_seq      INT          NOT NULL DEFAULT 0,
  event_count   INT          NOT NULL DEFAULT 0,
  gap_detected  TINYINT(1)   NOT NULL DEFAULT 0,
  updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (consumer_name, task_id)
) ENGINE=InnoDB;
