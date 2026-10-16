-- Demo UI milestone: the ONE small table the local HTTP API needs to
-- track run lifecycle across requests and across its own restarts --
-- not a general job queue, not a distributed-execution record. One row
-- per demo run; `detail_json` carries the structured, bounded evidence
-- (attempts, commit/tree identifiers, verification/handoff outcomes,
-- a sanitized error) the UI renders, rather than growing new columns
-- per field.
CREATE TABLE IF NOT EXISTS demo_runs (
  run_id       VARCHAR(191) NOT NULL PRIMARY KEY,
  task_id      VARCHAR(191) NOT NULL,
  mode         VARCHAR(32)  NOT NULL,
  status       VARCHAR(32)  NOT NULL,
  owner_id     VARCHAR(191) NOT NULL,
  created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  detail_json  JSON         NOT NULL
) ENGINE=InnoDB;
