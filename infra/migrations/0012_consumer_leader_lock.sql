-- Phase 3A.1: reuses the EXISTING `publisher_leader` table/mechanism
-- (epoch + lease, one UPDATE gated by InnoDB row locking --
-- src/messaging/publisher-lock.ts, docs/decisions/0006) as an explicit
-- single-instance ownership lock for the consumer too, under a
-- different `lock_name`. Deliberately NOT a new table/mechanism: the
-- smallest suitable existing primitive, reused by name.
INSERT IGNORE INTO publisher_leader (lock_name, epoch) VALUES ('consumer-default', 0);
