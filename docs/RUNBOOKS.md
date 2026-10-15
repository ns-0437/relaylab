# Runbooks (development-only alert thresholds)

Every alert here and in `infra/prometheus-alerts.yml` uses a threshold
labeled a DEVELOPMENT DEFAULT, chosen to be obviously actionable at this
project's current scale (single-digit concurrent tasks/processes in all
testing so far -- see CLAUDE.md). None of these have been validated
against production traffic, and none should be read as a production SLO.

## RelaylabServiceScrapeDown

**Means:** Prometheus has failed to scrape `relaylab-publisher` or
`relaylab-consumer`'s `/metrics` endpoint for over a minute.

**Inspect:**
- Is the process actually running (`Get-CimInstance Win32_Process` /
  `ps`, filtered on `publisher-main`/`consumer-main`)?
- Is it bound to the expected port (`RELAYLAB_METRICS_PORT`, default
  9464/9465)? Check its own startup log line: `[metrics] started for
  "...", serving Prometheus scrapes on :PORT/metrics`.
- Can the Prometheus CONTAINER reach the HOST port at all (Docker
  Desktop's `host.docker.internal`, see `infra/prometheus.yml`)?

**Safe recovery:** Restart the affected process (`npm run publisher` /
`npm run consumer`). No data is lost by a missed scrape window -- counters
simply resume from wherever they were (or 0, if the process restarted);
durable gauges are DB-backed and unaffected.

**Does NOT prove:** that the process is actually making progress --
only that its metrics endpoint answers. A process can be up and
scrapeable while stuck (e.g. blocked on a DB query); check
`relaylab_command_attempts_total`/`relaylab_consumer_attempts_total`
trending flat as a separate signal.

## RelaylabOutboxBacklogHigh

**Means:** more than 100 outbox rows have sat unpublished for 5+
minutes.

**Inspect:**
- Is a publisher process actually running and currently the leader
  (`relaylab_publisher_leadership_state == 1` on exactly one instance)?
- Is NATS reachable from the publisher (`relaylab_publish_attempts_total{outcome="failed"}`
  increasing)?
- Is MySQL healthy / is the outbox table growing unexpectedly fast
  relative to publish throughput?

**Safe recovery:** Restart the publisher if it is not the leader anywhere
(no running instance currently holds the lock) or if it is leader but
clearly stuck; confirm NATS/MySQL are reachable first before restarting,
since restarting will not help if the underlying dependency is down.

**Does NOT prove:** data loss. The outbox is a durable table; every row
in it still exists and will be published once the publisher resumes
working. This is a LATENCY signal (events are not yet visible to
consumers), not a durability signal.

## RelaylabOldestUnpublishedEventTooOld

**Means:** the single oldest unpublished outbox row is itself over 60s
old (distinguishes "a sudden large backlog" from "one specific row
stuck," which `RelaylabOutboxBacklogHigh`'s raw count alone cannot).

**Inspect:** query `SELECT * FROM outbox_events WHERE published_at IS
NULL ORDER BY id ASC LIMIT 1` directly -- is this one row failing
validation, or just waiting behind a large but otherwise healthy queue?

**Safe recovery:** same as `RelaylabOutboxBacklogHigh`. If investigation
shows this ONE row is permanently failing (e.g. `publishOne`'s retries
are exhausted and the publisher's main loop logs `iteration failed`
repeatedly referencing it), that is a genuine bug requiring code
investigation, not a metrics/alerting fix.

**Does NOT prove:** which specific row is old, or why -- the gauge is a
single number; the actual row requires the direct query above.

## RelaylabGapBacklogPersisting

**Means:** at least one event is sitting durably buffered in
`task_consumer_pending`, waiting for a missing lower-`seq` event, for
10+ minutes continuously.

**Inspect:** `SELECT * FROM task_consumer_state WHERE gap_detected = 1`
to find which task(s); then check whether the MISSING event was ever
actually published (`outbox_events` for that task_id/seq) -- if it was
never published at all, this is a publisher-side bug upstream of the
consumer, not a consumer bug.

**Safe recovery:** none automatic -- gaps close only when the missing
event is actually delivered (see docs/decisions/0003-event-ordering.md).
If the missing event was lost upstream (never written to the outbox at
all, which should not be possible given invariant: state+receipt+outbox
commit in one transaction -- see CLAUDE.md), that is a serious
correctness bug requiring code-level investigation, not an operational
fix.

**Does NOT prove:** that the gap will ever close on its own -- only that
it has not yet. Past `maxPendingPerTask` (default 50), further events for
that task stop being buffered and the condition becomes a loud, explicit
operator-attention case (see `src/messaging/consumer.ts`).

## RelaylabDurableGaugeRefreshStale

**Means:** a gauge group (`outbox`, `gaps`, or `quarantine` -- see the
`group` label) has not completed a successful refresh in 30+ seconds.
**While this fires, every OTHER durable gauge in that group must be
treated as possibly stale, not current.**

**Inspect:** the owning process's own logs for `[durable-gauges] <group>
refresh failed: <classification>` -- this names the error class (e.g.
`Error(ECONNREFUSED)`) without leaking a raw DB error message. Is MySQL
reachable from that process at all?

**Safe recovery:** restore MySQL connectivity; the refresher resumes on
its next 5s interval automatically, no restart needed, UNLESS the
process itself has crashed (then restart it).

**Does NOT prove:** that the underlying durable count is actually zero
or healthy -- only that this process's LAST SUCCESSFUL observation of it
is now old. This is precisely the alert that exists so a dashboard
viewer never mistakes "stopped updating" for "stayed healthy."

## RelaylabQuarantinedMessagesPresent

**Means:** at least one message sits in `consumer_quarantine` (malformed
JSON, schema-invalid, or redelivered past the max-delivery bound).

**Inspect:** `npm run quarantine:inspect` (optionally `-- <consumerName>
<limit>`) -- shows reason, event/task ID if known, delivery attempt
count, and a bounded raw-payload excerpt for diagnosis.

**Safe recovery:** quarantined messages are NOT automatically retried
(see docs/OBSERVABILITY.md "dead-letter quarantine") -- a human must
decide whether to fix the producer and resend, or accept the loss. There
is no code path in this project that re-injects a quarantined message
back into the stream.

**Does NOT prove:** that the SAME underlying bug is still happening --
only that at least one message was quarantined at some point and has not
been cleared from the table (nothing in this project currently deletes
quarantine rows; see docs/OBSERVABILITY.md "retention").

## RelaylabPublisherOutboxFreshnessMissing

**Means:** the publisher is up and believes it is the current leader,
but `relaylab_gauge_last_refresh_success_timestamp_seconds{group="outbox"}`
has NEVER appeared for this instance -- distinct from stale (see
`RelaylabDurableGaugeRefreshStale`): this fires when the series never
existed at all, which that alert structurally cannot detect (`time() -
<absent series>` yields no result).

**Inspect:** this instance's own logs for `[durable-gauges] outbox
refresh failed: ...` -- if NO such line ever appeared either, the
refresher may not have started at all (check `wasLeader`/leadership
acquisition timing around process start).

**Safe recovery:** same as `RelaylabDurableGaugeRefreshStale` -- restore
MySQL connectivity; if the refresher never started, restart the process.

**Does NOT prove:** that the outbox backlog is actually large -- only
that this instance has never successfully reported it. A standby
publisher (not leader) is EXPECTED to never export this series and must
never trigger this alert (the rule explicitly requires
`relaylab_publisher_leadership_state == 1`).

## RelaylabConsumerGapsFreshnessMissing

**Means:** the consumer is up, but
`relaylab_gauge_last_refresh_success_timestamp_seconds{group="gaps"}`
has never appeared for this instance.

**Inspect / safe recovery / does NOT prove:** identical in kind to
`RelaylabPublisherOutboxFreshnessMissing` above, for the consumer's
gap/quarantine gauge group instead of the publisher's outbox group.

## RelaylabDuplicateConsumerGaugeExporters

**Means:** more than one consumer PROCESS INSTANCE is currently
heartbeating as a gap/quarantine-gauge exporter for the same
`consumer_name` -- an UNSUPPORTED topology (see docs/METRICS.md "gauge
ownership"; this project does not support horizontal consumer scaling).

**Inspect:** `SELECT * FROM consumer_gauge_exporters WHERE consumer_name
= '<name>' ORDER BY last_heartbeat_at DESC` to see every instance
heartbeating recently, and (outside this database) confirm how many
`consumer-main.ts` OS processes are actually running against that same
consumer name.

**Safe recovery:** stop all but ONE consumer instance for that
`consumer_name`. There is no automatic election/arbitration -- a human
must choose which instance stays running.

**Does NOT prove:** which instance's gauge values are "correct" while
this fires -- with two exporters both writing the SAME underlying
durable counts (the counts themselves are accurate per-instance queries
against the SAME database, so both instances' values AGREE with each
other), the actual problem this alert flags is that a DASHBOARD/query
naively summing across `instance` labels for these specific gauges would
DOUBLE-COUNT a single real backlog, not that either instance is lying.
