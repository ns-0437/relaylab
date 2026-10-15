-- Phase 1.2.1 monitoring-reliability patch, section 6: makes an
-- UNSUPPORTED multiple-consumer-instance topology VISIBLY DETECTABLE
-- rather than silently producing duplicate/summed gauge series.
--
-- Unlike the publisher side (a REAL, already-enforced DB-backed leader
-- lock -- `publisher_leader`, docs/decisions/0006 -- only the genuine
-- lock-holder ever runs the outbox-gauge refresher), there is currently
-- NO equivalent election mechanism for consumer instances (horizontal
-- consumer scaling is an explicit non-goal of this project -- see
-- CLAUDE.md). This patch deliberately does NOT add one (no generalized
-- distributed election framework) -- it adds the SMALLEST thing that
-- makes the unsupported case OBSERVABLE: every consumer process
-- refreshing gap/quarantine gauges heartbeats its own instance identity
-- here on the SAME cadence as its gauge refresh; the refresher then
-- exposes a count of DISTINCT, RECENTLY-heartbeating instances for its
-- own consumer_name as `relaylab_consumer_gauge_exporter_count`. A value
-- > 1 is the unsupported topology, made visible via that gauge and
-- `RelaylabDuplicateConsumerGaugeExporters` (infra/prometheus-alerts.yml)
-- instead of two processes silently exposing identical/duplicate series.
CREATE TABLE IF NOT EXISTS consumer_gauge_exporters (
  consumer_name      VARCHAR(191) NOT NULL,
  instance_id        VARCHAR(191) NOT NULL,
  last_heartbeat_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (consumer_name, instance_id)
) ENGINE=InnoDB;
