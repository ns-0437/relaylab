# Metrics (Prometheus via OpenTelemetry Metrics)

Added in the Phase 1.2.1 patch, Part B-E, after Part A's correctness
fixes were verified. Covers application metrics and their Prometheus/
Grafana presentation only -- no new tracing work (docs/OBSERVABILITY.md
is unchanged in scope) and no change to the orchestration system itself.

## Part B: architecture decision

**Chosen path: OpenTelemetry Metrics API + `@opentelemetry/exporter-prometheus`**,
not a separate/dedicated Prometheus client library (e.g. `prom-client`).

- **Why this path is appropriate.** This project already standardized on
  OpenTelemetry for tracing (`@opentelemetry/api`, `@opentelemetry/sdk-node`,
  pinned exact versions -- see CLAUDE.md "runtime and dependency
  strategy"). `@opentelemetry/sdk-metrics@2.12.0` and
  `@opentelemetry/exporter-prometheus@0.223.0` are EXACTLY the same
  version lines already pinned for the tracing packages
  (`@opentelemetry/core`/`resources`/`sdk-trace-base`@2.12.0 and
  `sdk-node`/`exporter-trace-otlp-http`@0.223.0), and both declare a
  peer dependency on `@opentelemetry/api` compatible with the
  already-installed `1.9.1` (verified via `npm view <pkg>@<version>
  peerDependencies` before installing -- see evidence/ for the exact
  output). Adding a second, unrelated metrics library (`prom-client`)
  would mean two different instrumentation APIs, two different
  attribute/label conventions, and two dependency trees to keep
  compatible, for no benefit this project needs. "Consult official
  current documentation before choosing compatible packages" was done
  via `npm view`'s own peer-dependency metadata, which is this registry's
  authoritative current compatibility statement for these exact package
  versions.
- **How separate publisher and consumer processes expose metrics.**
  Each process runs its OWN `PrometheusExporter` (a pull-based
  `MetricReader` that also runs a small HTTP server), on its OWN port:
  publisher defaults to `9464`, consumer to `9465` (override with
  `RELAYLAB_METRICS_PORT`). This mirrors how the two processes already
  work independently (separate `*-main.ts` entrypoints, separate OTLP
  trace exporters) -- there is no shared metrics process or sidecar.
  Prometheus scrapes both ports directly (see `infra/prometheus.yml`).
- **How it coexists with the existing tracing provider.** A completely
  separate global registration: `metrics.setGlobalMeterProvider(...)`
  (new) alongside the existing `trace.setGlobalTracerProvider(...)` (via
  `NodeSDK.start()`, unchanged). These are two different global slots in
  `@opentelemetry/api` -- registering one never touches or overwrites
  the other. `src/telemetry/metrics.ts` is a NEW, separate module from
  `src/telemetry/init.ts` (tracing); both are started/stopped from each
  `*-main.ts` entrypoint, independently, specifically so a Prometheus
  exporter problem can never affect the trace pipeline and vice versa
  (see "shutdown and monitoring outages" below).
- **How metric names/units remain consistent.** Every metric name is
  prefixed `relaylab_`, uses Prometheus's own unit-suffix convention
  (`_total` for counters, `_seconds` for durations/ages, `_count` for
  gauge counts), and every duration is recorded in SECONDS (not ms) as a
  float, per Prometheus/OpenTelemetry convention, even though internal
  code elsewhere in this project uses milliseconds (`LEADER_LEASE_MS`,
  etc.) -- the metrics layer converts at the instrumentation call site,
  never exposing raw milliseconds as a `_seconds`-suffixed metric. See
  the dictionary below for the exact name/type/unit of every metric.
- **How shutdown and monitoring outages behave.** `shutdownMetrics()`
  (src/telemetry/metrics.ts) closes the `PrometheusExporter`'s HTTP
  server and shuts the `MeterProvider` down, called from each
  `*-main.ts`'s `stop()` alongside (not instead of) `shutdownTelemetry()`
  -- see src/messaging/publisher-main.ts / consumer-main.ts. Because the
  Prometheus exporter is PULL-based (nothing pushes metrics out over the
  network the way the OTLP trace exporter pushes spans), there is no
  network call on the command/event hot path for metrics at all:
  recording a counter/histogram observation is a synchronous in-process
  operation, and a Prometheus server being down, unreachable, or slow to
  scrape has NO effect on command/event processing latency or
  correctness -- it only means the next scrape sees stale or absent
  data, which Prometheus itself surfaces as the target going `down` in
  its own `up` metric (see Part D "scrape health"). This is also why
  metrics stay available even when tracing's OTLP export is failing:
  the two exporters are fully independent pipelines (see Part F
  validation item 10).
- **No two parallel metric stacks.** Only the instruments defined in
  the dictionary below exist; no second metrics library, no duplicate
  instrumentation of the same operation under two different names.

## Part C: metric dictionary

All label VALUES below are drawn from FINITE, explicit sets baked into
this codebase (domain error tags, a fixed small set of command/event
types, a fixed small set of outcome enums) -- never a task ID, event ID,
command ID, file path, trace ID, or raw error message. See "Label
discipline" below the table.

| Name | Type | Unit | Meaning | Exact increment/update point | Labels (all finite/bounded) | Restart behavior | Attempts or durable state? |
|---|---|---|---|---|---|---|---|
| `relaylab_command_attempts_total` | Counter | count | One `applyTaskCommand` call finished | `src/db/task-store.ts`, inside `withRelaylabSpan`'s success/catch paths for the `relaylab.command.process` span, once per call | `outcome` {committed, replayed, rejected, **infrastructure_failure**}; `reason` {n/a, EpochStale, ClaimLost, NotAuthorized, CommandConflict, CommandRejected, TaskNotFound, InfrastructureError} (n/a unless outcome=rejected or infrastructure_failure); `command_type` {Checkpoint, SubmitCandidate, RecordVerification, Complete, unknown} | Resets to 0 on process restart (in-memory counter) -- an attempt count, not a durable total; see "Counters vs durable state" below | Attempts (process-observed) |
| `relaylab_ownership_attempts_total` | Counter | count | One `claimTask`/`renewLease` call finished | Same `withRelaylabSpan` hook, for the `relaylab.ownership.claim` / `relaylab.ownership.renew` spans | `operation` {claim, renew}; `outcome` {acquired, renewed, rejected, **infrastructure_failure**}; `reason` (same finite set as above, n/a unless rejected/infrastructure_failure) | Resets on restart | Attempts |
| `relaylab_publish_attempts_total` | Counter | count | One outbox row's publish attempt finished (after `publishOne`'s own internal retry loop settles) | `src/messaging/publisher.ts`, end of `publishOne`, once per outbox row per call | `outcome` {published, failed} | Resets on restart | Attempts |
| `relaylab_consumer_attempts_total` | Counter | count | One message's processing attempt finished | `src/messaging/consumer.ts`'s `handleMessage`, once per delivered message (including redeliveries -- each is counted again) | `outcome` {applied, deduped, buffered, conflict, dead_lettered} | Resets on restart | Attempts -- **a redelivery increments this again; it is NOT a count of distinct business events, and a high `deduped`/`dead_lettered` rate is not itself "recovery"** |
| `relaylab_quarantine_attempts_total` | Counter | count | One durable quarantine write was attempted | `src/messaging/quarantine.ts`'s `writeQuarantineRecord`, success and failure both counted | `reason` {malformed_json, schema_invalid, max_redeliveries_exceeded}; `result` {recorded, write_failed} | Resets on restart | Attempts |
| `relaylab_gauge_refresh_failures_total` | Counter | count | A periodic durable-gauge DB refresh query failed | `src/telemetry/durable-gauges.ts`, refresh loop catch block | `group` {outbox, gaps, quarantine} | Resets on restart | Attempts (of the refresh job itself, not of business work) |
| `relaylab_command_processing_duration_seconds` | Histogram | seconds | Wall-clock duration of one `applyTaskCommand` call | Same hook as `relaylab_command_attempts_total`, span start to end | `outcome` {committed, replayed, rejected} | Resets on restart | Attempts |
| `relaylab_publish_attempt_duration_seconds` | Histogram | seconds | Wall-clock duration of one outbox row's publish attempt | `publisher.ts`'s `publishOne`, start to settle | `outcome` {published, failed} | Resets on restart | Attempts |
| `relaylab_consumer_processing_duration_seconds` | Histogram | seconds | Wall-clock duration of one message's processing attempt | `consumer.ts`'s `handleMessage`, span start to end | `outcome` {applied, deduped, buffered, conflict, dead_lettered} | Resets on restart | Attempts |
| `relaylab_outbox_unpublished_count` | Gauge | count | Durable count of `outbox_events` rows with `published_at IS NULL`, as of the last successful refresh | `durable-gauges.ts`'s periodic refresh (every 5s, bounded query via `idx_outbox_unpublished`); exposed ONLY by the current publisher LEADER (see "Gauge ownership" below) | (none -- global, single-owner) | Reflects real durable DB state; a restarted process simply re-queries and gets the true current value (not reset to 0) | **Durable state**, not an attempt count |
| `relaylab_outbox_oldest_unpublished_age_seconds` | Gauge | seconds | Age of the oldest unpublished outbox row, as of the last successful refresh | Same refresh as above | (none) | Durable state | Durable state |
| `relaylab_pending_gap_events_count` | Gauge | count | Durable count of ALL `task_consumer_pending` rows (every task) | `durable-gauges.ts` periodic refresh; exposed by the consumer process | (none) | Durable state | Durable state |
| `relaylab_tasks_with_unresolved_gaps_count` | Gauge | count | Durable count of `task_consumer_state` rows with `gap_detected = 1` | Same refresh | (none) | Durable state | Durable state |
| `relaylab_quarantined_messages_count` | Gauge | count | Durable count of ALL `consumer_quarantine` rows | Same refresh | (none) | Durable state | Durable state |
| `relaylab_gauge_last_refresh_success_timestamp_seconds` | Gauge | unix seconds | When a gauge group's refresh last SUCCEEDED | `durable-gauges.ts`, only on success (never touched on failure, so staleness becomes visible as `time() - this` grows) | `group` {outbox, gaps, quarantine} | Durable observation about freshness, not reset to 0 on restart timing alone (a fresh process has no value until its first successful refresh) | Freshness signal |
| `relaylab_nats_connected` | Gauge | boolean (0/1) | Whether THIS process currently has a live NATS connection | `src/messaging/nats-client.ts`'s `watchConnectionState`, event-driven via `nc.status()` (never polled); `1` at connect/reconnect, `0` at disconnect/reconnecting | (none -- per-instance) | Resets to the real current state on restart (not durable, not meant to be) | **The bounded readiness/connection signal distinguishing "process alive" from "able to process work"** -- a healthy `/metrics` scrape (the HTTP server starts before `connectNats` even runs) does NOT imply this is 1. See "NATS reconnect and recovery contract" in docs/OBSERVABILITY.md. |
| `relaylab_consumer_gauge_exporter_count` | Gauge | count | Durable count of consumer instances that have heartbeated recently (within 3x the refresh interval) as a gap/quarantine-gauge exporter for THIS process's `consumer_name` | `durable-gauges.ts`'s consumer refresher, every refresh cycle, reading `consumer_gauge_exporters` | (none -- scoped to the calling instance's own `consumer_name`, not exposed as a label; see "Gauge ownership" below) | Durable state (DB-backed heartbeat table), not reset by a single process's restart | **The visible diagnostic for an unsupported multi-consumer-instance topology** (section 6) -- a value > 1 means two or more consumer processes are exporting the SAME gap/quarantine gauges for the same `consumer_name`, which is NOT supported (see CLAUDE.md non-goals) and would otherwise silently risk duplicate/summed series. |
| `relaylab_publisher_leadership_state` | Gauge | boolean (0/1) | Whether THIS publisher instance currently believes it holds the leader lock | `publisher-main.ts`'s main loop, every iteration | (none -- per-instance; distinguished by Prometheus's own `instance` label from the scrape target) | Reflects current in-process belief; a restarted process starts at 0 (standby) until it next acquires leadership | Attempts/instance-local state, not durable (the durable leadership truth is `publisher_leader` table, which this gauge is observing, not replacing) |

### Operational failures vs. domain rejections (monitoring-reliability patch, section 2)

`relaylab_command_attempts_total`/`relaylab_ownership_attempts_total`'s
`outcome` label now has FOUR values, not three: `committed`/`replayed`
(or `acquired`/`renewed`) for success, `rejected` for an EXPECTED domain
rejection (the same finite set `isExpectedDomainRejection` already uses
for span-status policy -- EpochStale, ClaimLost, NotAuthorized,
CommandConflict, CommandRejected, TaskNotFound), and
`infrastructure_failure` for anything else (a DB timeout, a connection
drop, an unclassified throw). Before this patch, EVERY non-success
outcome was folded into `outcome="rejected"`, making a plain
`sum by (outcome) (rate(relaylab_command_attempts_total[5m]))` query
unable to tell "the system is correctly fencing stale commands" apart
from "the database is unreachable" without ALSO filtering on the
`reason` label. Verified by `tests/integration/metrics-outcomes.test.ts`
with a REAL unreachable-MySQL connection (not a mock) producing
`outcome="infrastructure_failure"`, and a real stale-epoch rejection
producing `outcome="rejected", reason="EpochStale"` -- each asserted to
NEVER appear under the other's outcome value.

**Cascade-draining and the attempt vs. applied-event distinction** (also
section 2): `relaylab_consumer_attempts_total{outcome="applied"}` counts
DELIVERY ATTEMPTS that resulted in at least one event being applied --
NOT the total number of events applied to the projection. When a
message's arrival closes a gap, `applyEventWithDedup` (`consumer.ts`)
cascade-drains and applies every contiguous BUFFERED event in the SAME
transaction -- one message's attempt can cause MULTIPLE events to
transition from buffered to applied, and this project does NOT currently
expose a separate "events applied to projection" counter distinguishing
that from "delivery attempts recorded." This is a stated, honest
limitation, not a silent undercount dressed up as something else:
operators reading `relaylab_consumer_attempts_total{outcome="applied"}`
should understand it as "attempts that succeeded," not "rows written to
`task_consumer_state`." Quarantined (`dead_lettered`), buffered, deduped,
and conflict outcomes are NEVER folded into `outcome="applied"` --
`consumer.ts`'s `handleMessage` calls `recordConsumerAttempt` with the
EXACT outcome string for each branch (verified directly by
`tests/integration/metrics-outcomes.test.ts`'s fourth test).

### Bounded query strategy: what a query timeout actually does (section 1)

`durable-gauges.ts` issues every refresh query with mysql2's own
`timeout` option. **This does NOT cancel the query on the MySQL
SERVER** -- mysql2's own documentation states that reaching this timeout
"will be destroyed" (the CLIENT connection, not the server-side
statement); the server may continue executing to completion, consuming
server resources, until it independently notices the client
disconnected. This is the SAME property this project's own telemetry
shutdown code (`src/telemetry/init.ts`) already states for its
`Promise.race`-against-a-timer pattern -- `shutdownTelemetry` never
claims the timeout cancels the underlying OTel SDK shutdown operation,
and `durable-gauges.ts` makes the identical statement about its own
bounded queries, not a stronger one.

### Overlapping-refresh prevention (section 1)

The refresh scheduler (`makeRefresher`, `src/telemetry/durable-gauges.ts`)
is COMPLETION-BASED (`setTimeout` scheduled only after the previous
refresh settles), not interval-based (`setInterval` racing an async
function) -- the ORIGINAL implementation used the latter, which allowed
a query slower than the refresh interval to overlap with the next
scheduled one, with no guarantee the faster (newer) one's result
wouldn't be overwritten by a slower (older) one completing later. With
completion-based scheduling, "at most one in-flight refresh" and "no
older refresh can overwrite newer cached state" are both structurally
true (only one `refresh()` call object is ever alive at a time), not
something a lock flag or generation counter needs to separately enforce.
`stop()` returns a `Promise` that resolves only once any in-flight
refresh has actually settled, specifically so a caller about to call
`pool.end()` can safely assume no further query against that pool is
outstanding. Verified by `tests/integration/durable-gauges.test.ts`
tests 1-4, including a REAL `SELECT SLEEP()` query proving max
concurrency never exceeds 1.

### Gauge ownership and aggregation (why no gauge is ever double-counted)

Every DURABLE (DB-backed) gauge above has exactly ONE AUTHORITATIVE
EXPORTER at a time, by two DIFFERENT mechanisms depending on the gauge
group:

- **Outbox gauges (`relaylab_outbox_unpublished_count`,
  `relaylab_outbox_oldest_unpublished_age_seconds`): ENFORCED**, not
  merely detected. Exposed ONLY by whichever publisher instance
  currently holds the REAL, DB-backed leader lock (`publisher_leader`
  table, `src/messaging/publisher-lock.ts`, docs/decisions/0006) --
  `wasLeader` in `publisher-main.ts`'s loop gates the refresher
  entirely: a standby publisher never starts it, so its `/metrics`
  endpoint does not expose these series at all (never a stale or zero
  value that could be mistaken for real data). **Leadership changes and
  stale old-leader samples**: when leadership is lost, the refresher is
  stopped and AWAITED before the reference is discarded
  (`publisher-main.ts`); if this instance later re-acquires leadership
  (or a different instance becomes leader), a BRAND NEW refresher with a
  BRAND NEW, empty cache is constructed -- a value cached during a
  PREVIOUS leadership period can never leak forward into a new one,
  because the old refresher object (and its cache closure) is discarded
  entirely, not reused or reset-in-place.
- **Gap/quarantine gauges (`relaylab_pending_gap_events_count`,
  `relaylab_tasks_with_unresolved_gaps_count`,
  `relaylab_quarantined_messages_count`): NOT enforced -- VISIBLY
  DETECTABLE instead.** There is no election mechanism for consumer
  instances (horizontal consumer scaling is an explicit non-goal -- see
  CLAUDE.md). Rather than silently assume single-ownership, every
  consumer instance heartbeats its own identity into
  `consumer_gauge_exporters` on each refresh cycle and exposes
  `relaylab_consumer_gauge_exporter_count` -- the count of DISTINCT,
  RECENTLY-heartbeating instances for its OWN `consumer_name`. A value
  of 1 is the supported, single-owner case this project's other gap/
  quarantine gauges assume; a value > 1 is the UNSUPPORTED case made
  visible via that gauge and the `RelaylabDuplicateConsumerGaugeExporters`
  alert (infra/prometheus-alerts.yml, docs/RUNBOOKS.md), rather than two
  processes quietly exposing what looks like one normal series each.
  Verified with two REAL, simultaneously-running consumer-gauge-
  refresher instances against the same `consumer_name`
  (`tests/integration/durable-gauges.test.ts` "section 6"): the count
  correctly reports 2 while both are active, and the underlying
  heartbeat-staleness query correctly excludes a stopped instance once
  its heartbeat ages past 3x the refresh interval.
- **No generalized election framework was added** for the consumer
  side, deliberately -- the instruction this responds to was explicit
  ("do not add a generalized distributed election framework
  unnecessarily"). **What remains unsupported, stated exactly**: running
  more than one consumer process against the same durable consumer name
  is NOT supported by this patch -- it is only made OBSERVABLE, not
  prevented, not load-balanced, and not deduplicated at the gauge level
  (the two instances' OWN counts of pending/gap/quarantine rows would be
  IDENTICAL, since both query the same database -- the risk this alert
  guards against is a DASHBOARD or query naively `sum()`-ing across
  `instance` labels for these specific series and double-counting a
  single real backlog, not the underlying numbers themselves being
  wrong).

Because of the above, a dashboard/alert query never needs (and must
never use) `sum()` across instances for ANY of these five gauges -- that
would only be correct if exactly one instance is ever exposing a
non-stale value for the outbox group (enforced) or exactly one instance
is running for a given `consumer_name` (the supported, alert-monitored
assumption for the gap/quarantine group).

### Label discipline

No metric in this project EVER carries `task_id`, `event_id`,
`command_id`, a file path, a trace/span ID, or a raw (unclassified)
error message as a label -- every label value above is drawn from a
FIXED, small, explicitly-listed set (an outcome enum, a domain error
tag already used for span attributes, or a fixed command/event type).
This mirrors the tracing redaction mechanism in
`src/telemetry/attributes.ts` (docs/OBSERVABILITY.md "redaction rules"):
a label is only ever one of these finite enum members, never
interpolated from request data. High-cardinality labels on a counter
are also a well-known Prometheus operational hazard independent of any
confidentiality concern (unbounded label cardinality degrades every
Prometheus server that scrapes it) -- this rule serves both purposes at
once.

### Counters vs. durable-state gauges (the distinction Part C's rules require)

A counter above (`_attempts_total`, `_failures_total`) counts **process
observations**: how many times THIS process noticed a particular
outcome since it last started. It can UNDER-count if the process is
killed mid-operation (the increment call and the business effect are
not atomic with each other -- see docs/OBSERVABILITY.md's identical
point about span export). A durable gauge above (`_count`, `_seconds`
age) reflects **actual current database state**, re-queried fresh on
every refresh, and is NOT affected by a process restart (a new process
simply re-reads the same durable truth). Do not read a counter as if it
were durable state, and do not read a durable gauge's `Restart behavior`
column as "resets to zero" -- it does not.

## Part D/E: see docs/PROGRESS.md and infra/ for the Compose/dashboard/alert
implementation itself; this file is the design record Part C asked to
exist BEFORE coding, kept as the dictionary of record afterward too.
