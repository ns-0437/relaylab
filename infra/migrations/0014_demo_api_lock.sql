-- Demo UI milestone: reuses the EXISTING publisher_leader table/
-- mechanism (epoch + lease, one UPDATE gated by InnoDB row locking --
-- src/messaging/publisher-lock.ts, docs/decisions/0006), the SAME
-- primitive migration 0012 already reused for the consumer lock, now
-- under a THIRD lock_name for the local demo API's single-instance
-- singleton. Deliberately not a new lock mechanism or a distributed
-- scheduler -- this is the smallest suitable existing primitive,
-- appropriate to a single-process local demo, reused by name.
INSERT IGNORE INTO publisher_leader (lock_name, epoch) VALUES ('demo-api', 0);
