# Observability (OpenTelemetry tracing)

Added in the Phase 1.2 audit; the lifecycle/shutdown contract, consumer
context-isolation semantics, Collector evidence, and sensitive-data test
coverage described below were strengthened in the Phase 1.2.1 patch
(Part A) after a review identified specific gaps in each -- that same
patch's Part B-E ALSO added Prometheus metrics, which is a SEPARATE
document: see docs/METRICS.md. This file covers distributed tracing and
the dead-letter quarantine mechanism only -- no logs pipeline, no tracing
UI/backend beyond the Collector's own debug-exporter output (all
explicitly deferred; see "Scope" at the end of this file).

## Actual initialization order

`src/telemetry/init.ts`'s `startTelemetry(serviceName)` is called as the
first statement in each `*-main.ts` entrypoint
(`src/messaging/publisher-main.ts`, `src/messaging/consumer-main.ts`),
textually before the other application imports, and executes before
`main()`'s own body runs. This project uses ONLY explicit, hand-written
spans -- no `instrumentations` are registered on the `NodeSDK` -- so there
is no require-time module-patching step that would make import order
relative to e.g. `mysql2`/`nats` imports matter functionally the way it
would with auto-instrumentation. Initializing first is still done, and
documented, as the safe default for if auto-instrumentation is ever added
later.

There is exactly ONE global tracer provider per process: the one
`NodeSDK.start()` registers via `@opentelemetry/api`'s global registry.
`@effect/opentelemetry` is NOT installed or used anywhere in this
codebase, specifically so Effect's own tracing integration can never
register a second, competing provider. `getTracer()` (`init.ts`) is the
single call site every other module uses to obtain a tracer.

## Service names

| Process | Service name |
|---|---|
| `publisher-main.ts` | `relaylab-publisher` |
| `consumer-main.ts` | `relaylab-consumer` |
| ad hoc scripts / tests that call `startTelemetry` directly | caller-chosen (e.g. `relaylab-collector-proof-test`) |

Each also carries `service.version` (`0.1.0`, kept in sync with
`package.json` by hand), `deployment.environment.name` (`RELAYLAB_ENV`,
default `dev`), and `service.instance.id` (`${serviceName}-${pid}-${random}`)
as resource attributes -- see `startTelemetry`.

## Disabled mode

Set `RELAYLAB_OTEL_DISABLED=1` and `startTelemetry` becomes a complete
no-op: no `NodeSDK` is constructed, nothing attempts to reach an OTLP
endpoint, and `getTracer()` returns `@opentelemetry/api`'s own built-in
no-op tracer. Application code never branches on whether tracing is
enabled -- it always calls `tracer.startActiveSpan(...)`, which is simply
harmless when no real provider is registered.

**This is the default for the automated integration test suite**
(`tests/integration/process-harness.ts`'s `testChildEnv`, and the manual
env blocks in `recovery-scenario.ts`): those tests are not ABOUT
telemetry and must not depend on a Collector being reachable or pay its
shutdown-flush timeout. The telemetry-specific test files
(`telemetry.test.ts`, `telemetry-resilience.test.ts`,
`telemetry-collector.manual.ts`) explicitly re-enable it.

## Lifecycle, shutdown, and reinitialization contract

`src/telemetry/init.ts` tracks an explicit lifecycle state
(`"idle" | "running" | "shutting_down" | "shutdown"`), replacing an
earlier design that used a single `started` boolean. The boolean could
not distinguish "never started" from "a shutdown is currently in
flight" from "fully shut down and safe to restart" -- and resetting it to
`false` before the real shutdown had settled could let a new
`startTelemetry()` call construct a second `NodeSDK` while the first
one's async shutdown was still running.

- `startTelemetry()` is a no-op while already `"running"`; it is REFUSED
  (logged, not thrown) while `"shutting_down"`, specifically to prevent a
  second `NodeSDK` being constructed before the first one's teardown
  finishes.
- `shutdownTelemetry(timeoutMs = 5000)` returns a `ShutdownOutcome`, which
  is deliberately precise about THREE different things a casual
  "shutdown succeeded" claim would otherwise conflate:
  - **(A) `completedWithinBound`** -- did THIS CALL's bounded wait end
    because the real shutdown operation settled, or because `timeoutMs`
    elapsed first? This is the ONLY thing the return value states.
  - **(B) the process actually exiting** -- callers (`publisher-main.ts`/
    `consumer-main.ts`) decide this separately: if `completedWithinBound`
    is `false` they log a warning and exit anyway, rather than silently
    hiding that the bound was exceeded.
  - **(C) all spans successfully exported to the Collector** -- even a
    `true` result only means the SDK's own shutdown sequence finished (it
    may itself have logged and swallowed an export failure -- see
    `classifyShutdownError` below); it does NOT retroactively prove the
    Collector received everything. Only
    `telemetry-collector.manual.ts`-style verification against the
    Collector's own structured output proves that.
- Concurrent/repeated `shutdownTelemetry()` calls share the SAME
  in-flight operation (`inFlightShutdown`) rather than each starting
  their own `target.shutdown()` call -- calling `NodeSDK#shutdown()`
  twice concurrently is not a documented-safe pattern in the installed
  SDK. Each caller still races that SHARED promise against its OWN
  `timeoutMs` bound, so two callers with different timeouts both get a
  correct, independent `completedWithinBound` answer.
- The timeout timer is always cleared in a `finally`, regardless of which
  side of the `Promise.race` wins -- a timer left running after the real
  shutdown wins would otherwise sit there as a dangling handle (harmless
  to the already-settled race, but able to keep the event loop alive).
- `Promise.race` does not cancel its losing side: if the timeout fires
  first, the underlying `target.shutdown()` call is NOT abandoned -- it
  keeps running in the background inside the shared `inFlightShutdown`
  and is still fully awaited there before global state is unregistered.
  A later `startTelemetry()` call is refused until that background work
  actually finishes (state reaches `"shutdown"`), so there is no window
  where a new SDK could be constructed mid-teardown.
- A late rejection from `target.shutdown()` is caught inside the one real
  operation (not inside each bounded caller), so it can never become an
  unhandled rejection even if every bounded caller already timed out and
  returned. It is logged via `classifyShutdownError(err)` -- the error's
  constructor name plus, if present, its `.code` (e.g.
  `"FetchError(ECONNREFUSED)"`) -- NEVER the raw error object or its
  `.message`, which can embed exporter/network detail such as a URL or
  connection string.
- **Same-process reinitialization IS supported**, but ONLY after a prior
  `shutdownTelemetry()` call has fully settled (state `"shutdown"`).
  This is the part that actually matters and was a real, verified gap in
  the original design: `NodeSDK.shutdown()` on its own does NOT call
  `trace.disable()` / `context.disable()` / `propagation.disable()` --
  confirmed by reading the installed SDK's own source
  (`node_modules/@opentelemetry/sdk-node/build/src/sdk.js`;
  `shutdown()` only calls `this._tracerProvider.shutdown()`) and
  empirically (starting a second `NodeSDK` after a first one's
  `.shutdown()`, without an explicit `disable()` in between, silently
  produced ZERO exported spans, because `@opentelemetry/api`'s global
  registration ignores a second `setGlobalTracerProvider`/
  `setGlobalPropagator`/`setGlobalContextManager` call once one is
  already registered -- shut down or not). `shutdownTelemetry` therefore
  calls all three `disable()` functions itself, but only AFTER the real
  shutdown attempt has fully settled (success or failure) -- disabling
  earlier could tear the global API down out from under spans the
  shutdown sequence itself is still trying to flush. Do not describe
  resetting a boolean (or any flag) as equivalent to unregistering the
  global OTel providers; only this explicit `disable()` sequence does
  that.
- Verified by `telemetry-resilience.test.ts` (7 tests): a fast successful
  shutdown does not leave its timer pending and does not wait out the
  full bound (test 1); a Collector-unavailable (`ECONNREFUSED`) shutdown
  is bounded (test 2); a deliberately slow/non-responding exporter path
  (a controlled local `node:http` "black hole" server that accepts
  connections but never responds -- chosen after an attempted
  non-routable-IP approach resolved too fast to be a reliable test
  signal in this environment) actually triggers the timeout branch,
  requiring a real queued span first since an empty batch processor
  flushes instantly regardless of endpoint reachability (test 3); a REAL
  child process, not just the in-process SDK, exits within the stated
  bound with telemetry enabled (test 4); concurrent `shutdownTelemetry()`
  calls share one operation and all resolve consistently (test 5);
  `startTelemetry()` refuses to run again mid-shutdown and reinit
  succeeds once settled (test 6); and a captured `console.error` line
  during a real shutdown-error path never contains a synthetic
  credential-bearing value (test 7).

## Context propagation and span relationships

Three propagation boundaries, three different mechanisms:

1. **Within one process, across `await`s** (e.g. inside
   `applyTaskCommand`'s transaction): ordinary OTel `context.active()`,
   backed by Node's `AsyncLocalStorage` (the SDK's default context
   manager). No custom code needed.
2. **Across the durable outbox** (one process writes a row, a LATER,
   possibly different process reads and publishes it):
   `src/telemetry/propagation.ts`'s `captureCurrentCarrier()` /
   `contextFromCarrier()`. The carrier (`{traceparent, tracestate}` --
   exactly the two standard W3C fields, nothing else) is persisted in
   `outbox_events.trace_carrier`, a column added by the additive
   migration `infra/migrations/0003_otel_trace_carrier.sql` (0001/0002
   untouched, their checksums in `schema_migrations` unchanged). It is
   captured with `context.active()` in the SAME transaction that commits
   the business event (`src/db/task-store.ts`), no network call. It is
   deliberately a SEPARATE column from `payload`: `payload` is the
   immutable business event that `src/domain/fingerprint.ts` and
   `consumer.ts`'s `content_hash` are computed over, and trace metadata
   must never be able to perturb either (verified by
   `telemetry.test.ts` test 5: changing which spans/traces are active
   around a command never changes its fingerprint).
3. **Across NATS** (publisher process to consumer process, or a
   redelivery): W3C trace-context headers (`traceparent`/`tracestate`),
   injected into `MsgHdrs` at publish time (`src/messaging/publisher.ts`)
   and extracted at consume time (`src/messaging/consumer.ts`'s
   `carrierFromNatsHeaders`).

**Parent vs. link, and why each was chosen:**

- **Command → outbox publish: PARENT/CHILD, same trace.** There is
  exactly one logical publish lineage per outbox row (even across the
  publisher's own internal retries -- `publishOne`'s retry loop stays
  inside one span). The carrier injected into NATS headers is captured
  AFTER the publish span is created (not the original command-time
  carrier), so the thing a consumer links back to is the actual publish
  attempt that put the message on the wire, not the (possibly
  long-finished) command.
- **Outbox publish → consumer processing: LINK, NEW trace.** A parent/
  child relationship would be wrong here: an unbounded number of
  redeliveries can occur for one message, and modeling each as a child of
  the same producer span would make a single trace grow without bound and
  conflate "the same message, redelivered" with "a normal nested call."
  Each processing ATTEMPT gets its own fresh trace, cross-referenced back
  to the producer via exactly one `Link`. This follows the OTel messaging
  semantic-conventions guidance to prefer links over parent context for
  the "process" operation when delivery is not a strict 1:1 call/return
  (see https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/).
  **This is implemented EXPLICITLY, not left as an implicit side effect
  of nothing else being active.** `consumer.ts`'s `handleMessage` calls
  the 4-argument `tracer.startActiveSpan(name, { links }, ROOT_CONTEXT,
  fn)` overload -- passing `ROOT_CONTEXT` as the parent context
  explicitly -- rather than the 3-argument `(name, options, fn)` form,
  which would silently derive this span's parent from `context.active()`
  AT THE CALL SITE. Relying on the 3-argument form would make "each
  attempt starts a new root trace" an accident of there being nothing
  else ambiently active in the process, not a guarantee -- and it would
  break the instant this code ran under any ambient active span (e.g. a
  future batching/retry wrapper). Verified by `telemetry.test.ts` test
  "2b", which deliberately keeps an UNRELATED parent span active around
  the entire `processBatch` call and asserts the resulting consumer span
  has no parent span context, a traceId different from both the
  unrelated span's and the producer's, and a link that still correctly
  points at the producer (publish-attempt) span.
- **Missing/invalid headers are handled safely, not specially.**
  `contextFromCarrier(null)` / an unparseable carrier simply returns
  `ROOT_CONTEXT`; the publish span becomes an unparented trace root
  instead of failing, and the consumer still links to THAT span (see
  `telemetry.test.ts` test 6 -- graceful degradation loses one hop of
  ancestry, not the whole mechanism, and never blocks business
  processing).
- **Concurrent-task isolation**: `contextFromCarrier` always builds its
  result from `ROOT_CONTEXT`, never from `context.active()`. Extracting
  onto the ambient active context would silently graft one task's trace
  onto whatever (unrelated) span happens to be active in the process at
  that moment; rooting at `ROOT_CONTEXT` guarantees two tasks' traces
  never cross-contaminate regardless of what else is running
  concurrently (verified by `telemetry.test.ts` test 3, two tasks
  processed via `Promise.all`).
- **Trace context is never authorization.** Nothing in this codebase
  checks a trace carrier to decide whether an operation is allowed;
  ownership/idempotency fencing (`task-store.ts`) is entirely independent
  of and upstream of any telemetry concern. Only the two standard W3C
  fields are ever propagated -- no baggage propagator is registered
  (`init.ts` uses `W3CTraceContextPropagator` alone, not the default
  composite-with-baggage propagator), so there is no channel for
  arbitrary application data to ride along with a trace.

## Span-status policy

Every span this project creates sets `relaylab.outcome` (see
`src/telemetry/attributes.ts`'s `RelaylabOutcome`) and, where relevant,
`relaylab.error_type` -- a STABLE tag (`err._tag`, e.g. `"EpochStale"`),
never a raw exception message. Span STATUS (`SpanStatusCode.ERROR` vs.
unset/OK) is set according to one rule:

> **Expected domain rejections do not mark the span an error. Unexpected
> infrastructure failures do.**

`isExpectedDomainRejection` (`attributes.ts`) lists the "expected"
category: `EpochStale`, `ClaimLost`, `NotAuthorized`, `CommandConflict`,
`CommandRejected`, `TaskNotFound`. These are normal, anticipated outcomes
of a system built around fencing and idempotency -- a trace full of "ERROR"
spans for every stale-epoch rejection would make the genuinely unusual
failures (a dropped DB connection, an unreachable NATS server, an
`UnknownError`) impossible to distinguish by severity alone. An
`EventIdentityConflict` (an `event_id` reused with different content --
see `docs/decisions/0005`) IS marked as an error on the consumer span:
unlike the command-side rejections, this should never happen if the
publisher is correct, so it is a tripwire for a protocol bug, not a
routine business outcome.

`withRelaylabSpan` (`src/telemetry/with-span.ts`) implements this policy
once, shared by `claimTask`, `renewLease`, and `applyTaskCommand`.

## Exporter / Collector configuration

- Exporter: `@opentelemetry/exporter-trace-otlp-http`, OTLP/HTTP, default
  endpoint `http://127.0.0.1:4318/v1/traces` (override with
  `RELAYLAB_OTEL_EXPORTER_OTLP_ENDPOINT`), 5s per-export timeout.
- Collector: `otel/opentelemetry-collector-contrib:0.116.1`, started via
  an optional Compose profile (`infra/compose.yaml`'s `otel-collector`
  service, `profiles: ["observability"]` -- not started by a plain
  `docker compose up`). Config: `infra/otel-collector-config.yaml` --
  `memory_limiter` (128 MiB limit / 32 MiB spike) then `batch` (2s
  timeout, 512-span batches, 1024-span cap) in front of TWO exporters:
  `debug` (`verbosity: detailed`, prints every received span to the
  Collector's own container logs -- human-readable, not what any test
  parses) and `file` (writes the same spans as structured OTLP-JSON
  lines to `/var/lib/otel-evidence/spans.jsonl`, bind-mounted to
  `infra/otel-evidence/` on the host -- `*.jsonl` gitignored, this is
  generated evidence, not committed). The `file` exporter's structured
  output is what `telemetry-collector.manual.ts` actually parses (full
  trace IDs, span IDs, parent/link relationships, attributes) -- the
  "Collector output suitable for inspecting local trace evidence" this
  phase needs, without a dashboard stack (explicitly deferred -- see
  "Scope" below).
- Ports: `127.0.0.1:4318` (OTLP/HTTP, what this project actually uses)
  and `127.0.0.1:4317` (OTLP/gRPC, part of the standard receiver,
  unused), both localhost-only.

### Exact commands

```bash
# Start the Collector (optional profile, not part of the default stack)
docker compose -f infra/compose.yaml --profile observability up -d otel-collector

# Watch what it receives
docker logs -f relaylab-otel-collector-1

# Run the publisher/consumer with tracing enabled (RELAYLAB_OTEL_DISABLED is NOT set)
npm run publisher
npm run consumer

# Point at a different Collector/endpoint
RELAYLAB_OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318/v1/traces npm run publisher

# Explicitly disable tracing for a process
RELAYLAB_OTEL_DISABLED=1 npm run publisher

# Tests: the automated suite runs with tracing OFF by default
npm run test:integration

# The ONE test that verifies real Collector receipt (requires the profile above to be running first)
npm run test:telemetry-collector

# Stop the Collector
docker compose -f infra/compose.yaml --profile observability down
```

## Sampling policy

None configured -- every span is recorded (the SDK default `AlwaysOnSampler`).
Acceptable at this project's current scale (single-digit concurrent
tasks/processes in all testing so far); revisit before any higher-volume
usage. No performance/throughput numbers are claimed anywhere in this
repository, including here.

## Redaction rules

`src/telemetry/attributes.ts`'s `setRelaylabAttributes` only accepts a
FIXED, typed interface (`RelaylabSpanAttributes`): task/event/command IDs,
seq, epoch, command/event type, delivery attempt, a closed-enum outcome,
and a stable error tag. There is no "pass an arbitrary object" escape
hatch anywhere else in the codebase -- every call site that annotates a
span goes through this one function. That is the redaction mechanism: it
is structurally impossible to attach a credential, a connection string,
raw SQL, file contents, a prompt/tool payload, or an environment dump to
a span, because there is no parameter that would carry one.

Verified by `telemetry.test.ts` tests 9a-9c, each covering a DIFFERENT,
explicitly stated output surface -- do not infer universal redaction from
any single one of these, or from a single payload:
- **9a** (normal/success path): a sentinel value is planted in a NORMAL
  business payload and the test asserts it never appears in any exported
  span's name, attributes, OR events (span events are a distinct surface
  from attributes -- this project does not currently add span events, but
  the test checks for the sentinel there too, so the assertion holds even
  if one is added later without updating this test).
- **9b** (error/domain-rejection path): a synthetic credential-bearing
  value (e.g. a fake `Authorization: Bearer ...`-style string and a fake
  URL with embedded credentials -- never a real secret) is planted
  alongside a `CommandRejected` domain error, and the test asserts it
  never appears in span attributes OR in the span's status `message`
  (the status description is a separate surface from attributes and is
  checked explicitly, since `isExpectedDomainRejection` already means
  this path does not mark the span `ERROR` -- see "Span-status policy"
  above -- but the status `message` field is still set and must still be
  checked).
- **9c** (unit-level, no infrastructure): directly unit-tests
  `classifyError` (`attributes.ts`) and `classifyShutdownError`
  (`init.ts`) against synthetic credential-bearing error messages/URLs
  and asserts the classification string returned is always just a stable
  tag/constructor name (plus an error `.code` for `classifyShutdownError`),
  never the raw message.

These three surfaces -- span attributes, span names, span events, span
status descriptions -- plus captured `console.error` lines (see
`telemetry-resilience.test.ts` test 7 above) are the surfaces actually
covered. Logs outside these specific `console.error` call sites, and any
future telemetry output surface not listed here, are NOT covered by these
tests; do not claim universal redaction beyond what is explicitly
verified above.

Task/event IDs ARE recorded on spans (they are necessary for trace
investigation) but are NEVER used as a Prometheus metric label -- see
docs/METRICS.md "label discipline," now a currently-enforced rule (the
metrics pipeline exists as of Phase 1.2.1 Part B-E), not just a
forward-looking one.

Trace/span IDs are appended to a handful of EXISTING `console.error`
lines (`src/telemetry/log.ts`'s `traceLogSuffix()`) -- this does not
replace or wrap the logging system, it is a plain string suffix added at
a few call sites in `consumer.ts` where a span is already active.

### Phase 1.2.1 Part A.3: further log-path sanitization

Two additional raw-data leaks were found and fixed in this patch, neither
previously covered by the redaction mechanism above because neither is a
SPAN attribute -- they are plain `console.log`/`console.error` lines:

- **The configured OTLP endpoint URL was logged in full.**
  `RELAYLAB_OTEL_EXPORTER_OTLP_ENDPOINT` is an operator-controlled env
  var that could carry embedded userinfo credentials or a revealing
  path/query. `startTelemetry`'s startup log line now calls
  `sanitizeEndpointForLogging()` (`src/telemetry/init.ts`), which logs
  ONLY `scheme://host:port` -- parsed via the standard `URL` class, never
  string-matched -- and a fixed placeholder if the value isn't even a
  parseable URL. Verified by `telemetry-resilience.test.ts` test 8, which
  plants a synthetic credential-bearing endpoint and asserts the full
  string never appears in ANY captured log line, only the sanitized form.
- **A generic infrastructure error's `.code`/`constructor.name` was
  logged without a bound on which codes are "safe."** The ORIGINAL
  `classifyShutdownError` (`init.ts`) logged any `.code` value verbatim
  alongside the constructor name. Per this patch's explicit instruction
  not to treat an arbitrary `.code`/`constructor.name` as guaranteed
  safe, `classifyShutdownError` now checks `.code` against a FINITE
  allowlist of well-known, data-free POSIX/libuv network codes
  (`SAFE_ERROR_CODES` in `init.ts`: `ECONNREFUSED`, `ECONNRESET`,
  `ETIMEDOUT`, `ENOTFOUND`, `EPIPE`, `EAI_AGAIN`, `ECONNABORTED`) and
  falls back to the constructor name ALONE for anything not on that
  list. This same classifier is now also used by
  `publisher-main.ts`/`consumer-main.ts`'s own iteration-failure logging
  (previously `err.message` was logged directly there -- a DB/NATS
  failure's message can embed a connection string).
- **Schema-invalid JSON's raw ParseError was never logged** (this was
  already correct in the original code, but is now explicitly tested):
  `decodeEventEnvelope`'s Effect Schema `ParseError` routinely embeds the
  actual (possibly sensitive) rejected input values in its formatted
  `.message` -- `consumer.ts` logs only a fixed classification string
  (`"schema validation failed"`), never `decoded.left` itself.

Verified by `telemetry-resilience.test.ts` tests 8 and 9 (credential-
bearing endpoint; malformed-message sentinel), which check captured
`console.log`/`console.error` output specifically, not just spans -- see
"Dead-letter quarantine" below for why the malformed-message sentinel is
explicitly allowed to persist in the DATABASE even though it must never
reach a log line.

## Dead-letter quarantine

Phase 1.2.1 Part A.2: a review correctly identified that "dead-lettering"
previously meant only a log line plus an ack -- no durable record of
WHAT was rejected or WHY. `src/messaging/quarantine.ts` and
`infra/migrations/0004_consumer_quarantine.sql` (`consumer_quarantine`
table) fix this.

**Identity and dedup.** The table's primary key is
`(consumer_name, nats_stream_seq)` -- the JetStream message's own stream
sequence number, NOT the application `event_id`, because malformed JSON
may never decode far enough to HAVE an `event_id`. A redelivery of the
same physical message (same stream sequence) is therefore naturally
deduplicated by `INSERT IGNORE`, whether the redelivery happens because
quarantine-write failed and never acked, or because quarantine-write
succeeded but the broker never received the ack.

**Three distinguished reasons**, previously conflated into one
"dead-lettered" bucket: `malformed_json` (payload is not even valid
JSON), `schema_invalid` (valid JSON, wrong shape for `EventEnvelope`),
`max_redeliveries_exceeded` (decodes fine, but redelivered past
`maxDeliverBeforeDeadLetter`). Only the last two ever have an
`event_id`/`task_id` recorded -- malformed JSON structurally cannot.

**Write-before-ack, not ack-then-hope.** `handleMessage`
(`src/messaging/consumer.ts`) calls `writeQuarantineRecord` and AWAITS
it BEFORE calling `msg.ack()`. If the write throws (a real DB failure),
the exception propagates, the message is NOT acked, and the broker will
redeliver -- this is what makes a "did quarantine actually get stored"
failure distinguishable from a success, rather than silently acking as
if evidence were safely recorded when it was not. Verified by
`quarantine.test.ts`'s "if durable quarantine-write fails" test, which
simulates a write failure by temporarily renaming the table away,
confirms the message is not acked, then restores the table and confirms
redelivery succeeds and records exactly once.

**Data-handling policy for `payload_excerpt`.** Up to 2000 raw bytes of
the rejected message are stored in the quarantine row -- deliberately
NOT run through the span-attribute redaction mechanism above, because
there is no FIXED, typed shape (unlike `RelaylabSpanAttributes`) that
could redact an arbitrary malformed/invalid payload while still leaving
it useful for the diagnosis this table exists for. This is a conscious,
narrow, documented exception: `consumer_quarantine` may contain the same
class of sensitive business data as the rest of this database, and must
be treated with the same access restrictions -- it is NEVER logged, NEVER
exported as a span attribute, and NEVER exposed as a metric or metric
label (see docs/METRICS.md). Verified never to leak into logs/spans
specifically (as opposed to the DB row, which is allowed to have it) by
`telemetry-resilience.test.ts` test 9.

**Inspection, not a UI.** `npm run quarantine:inspect [consumerName]
[limit]` (`src/db/quarantine-cli.ts`) lists recent rows including the raw
excerpt -- an operator-invoked local CLI reading directly from the
database, not an application log line or a metric, which is exactly the
distinction the policy above cares about.

**Retention.** Nothing in this project currently deletes
`consumer_quarantine` rows -- they persist indefinitely until an
operator manually removes them. This is a stated limitation, not an
implemented retention policy; a future phase adding a retention job
would need to decide and document a retention window, which Phase 1.2.1
deliberately does not invent without a real requirement driving it.

**Not a "discard" policy.** Every one of the three reasons above results
in a DURABLE row before ack -- nothing in this patch silently discards a
message without a trace of it ever having existed.

## NATS reconnect and recovery contract

Added in the monitoring-reliability patch, after a review found that a
NATS broker outage left `publisher-main.ts`/`consumer-main.ts` alive but
permanently unable to process work, with no path back. This section is
the full causal diagnosis (captured with REAL evidence, not assumed) and
the resulting documented recovery contract.

### Root cause: trigger -> failing code path -> resulting state -> why work did not resume

- **Trigger**: the NATS broker becomes unreachable (`docker stop` of the
  real container in testing).
- **Failing code path**: nats.js's OWN reconnect loop retries at a fixed
  interval (`reconnectTimeWait`, default 2000ms) up to a fixed attempt
  count (`maxReconnectAttempts`, default 10) -- observed directly via a
  standalone diagnostic script consuming the client's own `nc.status()`
  event stream: a `disconnect` event, followed by `reconnecting` events
  roughly 2.1s apart for an IDLE connection, consistently matching the
  documented defaults (installed `nats` client `2.29.3` against
  `nats-server 2.10.29`).
- **Resulting state**: once that budget is exhausted, the client
  transitions to PERMANENTLY closed -- `nc.isClosed()` becomes `true`
  and every subsequent operation (`fetch()`, `consumers.info()`,
  `publish()`) throws `NatsError CONNECTION_CLOSED`. Confirmed to remain
  `true` even ~17s after the broker was independently confirmed back up
  and healthy -- nats.js provides no automatic "resurrection" of an
  already-permanently-closed connection object; this is documented
  nats.js behavior, not a bug in the library.
- **Why work did not resume**: prior to this patch, NEITHER
  `publisher-main.ts` nor `consumer-main.ts` ever checked
  `nc.isClosed()` or called `connectNats()` again. Their main loops
  simply kept retrying the SAME dead handles forever, logging "iteration
  failed"/"batch failed" on every pass with no code path back to a
  working connection. **This was not a "waited too little" timing
  issue** -- it was a missing recovery code path, demonstrated by
  `nc.isClosed()` staying `true` indefinitely regardless of how long the
  broker had already been back up.
- **A second, related finding**: an ACTIVE consumer/publisher loop
  (continuously calling `fetch()`/`publish()` while reconnecting)
  measurably exhausts the SAME nominal reconnect budget FASTER than an
  idle connection does (observed directly: a real consumer/publisher
  pair reached permanent closure within roughly a second of an outage
  starting, while an idle diagnostic connection with the IDENTICAL
  `maxReconnectAttempts`/`reconnectTimeWait` options took the full
  documented ~20s). The exact internal mechanism was not traced further
  (out of scope for this patch), but the measured difference is real and
  is why this project's own tests configure an EXPLICIT reconnect budget
  (see below) rather than relying on the nominal idle-connection timing.
- **A third finding, also from real evidence**: a brief "within budget"
  outage can resolve with ZERO observed `relaylab_publish_attempts_total{outcome="failed"}`
  increments -- while STILL within its reconnect budget (not yet
  permanently closed), nats.js appears to hold/queue a `publish()` call
  rather than reject it immediately, so a short enough outage can simply
  add latency, not an observable error. This is good news for a brief
  outage (section 3.A below), not a test gap.

### The recovery contract (two distinct scenarios)

**A. Outage within the supported reconnect budget** (nats.js's own
`maxReconnectAttempts`/`reconnectTimeWait`, defaults 10/2000ms unless
overridden -- see below): the process remains alive, the connection
resumes AUTOMATICALLY via nats.js's own reconnect logic, consumer
processing resumes, the durable backlog drains, and durable effects
remain deduplicated exactly as invariant F already guarantees (dedup is
independent of how many times a publish/fetch was retried). No
application code intervention needed or performed in this case.

**B. Outage beyond the reconnect budget**: the CHOSEN, DELIBERATE policy
is **application-level reconnect/recreation with bounded backoff** --
NOT process termination requiring external supervision. At the top of
each main-loop iteration, both `publisher-main.ts` and `consumer-main.ts`
check `nc.isClosed()`; if `true`, they call `connectNats()` again (a
fresh TCP/NATS handshake, entirely independent of the dead object) and
replace `nc`/`js`/`jsm`. This reconnect ATTEMPT is wrapped in the SAME
try/catch as the rest of the iteration, so if it fails (the broker is
still down), the existing bounded exponential backoff already used for
every other iteration failure applies -- there is no second, separate,
unbounded retry mechanism layered on top. Why this policy over
"terminate and let a supervisor restart": it requires no new process-
management dependency (explicitly out of scope for this patch), keeps
the existing test harness's process model unchanged, and the real
server-side state this recovery depends on (see below) is already
proven safe to re-attach to.

- **Durable consumer identity is preserved across reconnects.**
  `ensureDurableConsumer` (idempotent) is re-run after a reconnect, but
  this does NOT reset delivery position: a JetStream durable consumer's
  delivery/ack state lives on the NATS SERVER, never the client
  connection. A client-side reconnect-and-recreate call is a no-op
  against server state that already exists (confirmed via the real
  broker-outage test: after an application-level reconnect, the
  consumer resumes exactly where the durable consumer's server-side
  state left off, with no redelivery of already-acked messages and no
  loss of the pending one).
- **Duplicate processing loops are prevented structurally, not by an
  added guard.** The `nc.isClosed()` check and reconnect happen ONCE per
  iteration, inside the single sequential `while (!stopping)` loop
  (never awaited concurrently with itself) -- there is no window where
  two reconnect attempts, or two fetch/publish loops against two
  different connection objects, could run at once.
- **A bounded readiness/connection signal** (distinct from "process
  alive"): `relaylab_nats_connected` (see docs/METRICS.md), event-driven
  via `nc.status()`, exposed by both processes. A healthy `/metrics`
  scrape does NOT prove a process can reach NATS -- the metrics HTTP
  server starts before `connectNats` ever runs (see "actual
  initialization order" above) -- this gauge is what actually answers
  "is this process able to do work," confirmed to go to `0` during a
  real outage and back to `1` after either recovery path.
- **A small, test-only configurable reconnect budget**
  (`RELAYLAB_NATS_MAX_RECONNECT_ATTEMPTS`/`RELAYLAB_NATS_RECONNECT_TIME_WAIT_MS`,
  `src/messaging/nats-client.ts` `defaultReconnectBudget`) exists SOLELY
  so `tests/integration/monitoring-scenarios.manual.ts` can exercise
  BOTH recovery paths deterministically in seconds rather than relying
  on nats.js's nominal ~20s idle budget (which, per the finding above,
  does not even reliably predict an ACTIVE loop's real exhaustion time).
  Production deployment never sets these env vars, so production keeps
  nats.js's own unmodified defaults.
- **Rejected alternative**: `maxReconnectAttempts: -1` (retry forever)
  was tried and reverted -- it changed `js.publish()`'s own behavior
  during an active reconnect attempt from "fail fast" (after
  `publisher.ts`'s own bounded `maxRetries`) to "buffer/block until
  reconnected," which would make `publishOne`'s own retry+backoff no
  longer the single source of truth for publish-attempt timing. Rejected
  as outside this patch's scope (a monitoring RELIABILITY patch, not a
  NATS-client-behavior redesign), not because it was untested.

### Validation (real MySQL/NATS/Prometheus, both scenarios)

`tests/integration/monitoring-scenarios.manual.ts` "A1"
(within-budget) and "A2" (beyond-budget, using the small test budget
above) each: confirm both processes are initially ready and connected
(`relaylab_nats_connected` = 1), interrupt NATS, commit real outbox
work, observe the durable backlog and/or failed-attempt signal, restore
NATS, observe recovery (automatic in A1, the logged
`"application-level NATS reconnect succeeded"` line in A2), confirm the
backlog drains to 0, and confirm EXACTLY ONE durable local effect and
ONE `consumer_processed` dedup record -- no duplicate from either
recovery path. See docs/PROGRESS.md for the actual captured Prometheus
values from a real run.

### Shutdown: distinguishing expected already-closed state from unexpected failures

`nc.drain()` on a connection that already exhausted its reconnect
budget (expectedly, permanently closed) THROWS `NatsError
CONNECTION_CLOSED` rather than resolving -- found via the real
broker-outage test. The shutdown path now checks `nc.isClosed()`
EXPLICITLY first: if `true`, `drain()` is skipped entirely (there is
nothing left to drain, and this is a POSITIVE check of a known,
expected state, not a blanket catch-and-ignore); if `false` (the
connection was open/connecting and `drain()` failed for a genuinely
unexpected reason), the failure IS logged loudly, not silently treated
as success. Gauge refreshers are stopped (and AWAITED) BEFORE
`pool.end()` is called, in both `publisher-main.ts` and
`consumer-main.ts` -- an earlier ordering had this backwards, which
could have left a refresh query racing a closed pool.

## Collector failure behavior

- Export failures (Collector unreachable, timeout, etc.) are caught and
  logged by the OTLP exporter / `shutdownTelemetry`'s own `.catch`; they
  NEVER propagate into application code. No business command is rolled
  back, no message is left unprocessed, because of a telemetry export
  failure. Verified against a REAL `NodeSDK` pointed at an unreachable
  port (`telemetry-resilience.test.ts` test 7 -- not just the in-memory
  test double).
- Telemetry export is never part of a business transaction's commit
  path -- `captureCurrentCarrier()` only ever reads already-active
  in-process span context; it makes no network call, so even a totally
  dead Collector cannot slow down or fail a database transaction.
- Export failure never triggers a business-command retry. The two retry
  loops in this codebase (`publisher.ts`'s `publishOne`, bounded by
  `maxRetries`) are about the NATS publish call, not telemetry export;
  they are entirely independent of whether spans are being exported
  successfully.

## Graceful shutdown vs. hard-kill limitations

`shutdownTelemetry(timeoutMs)` flushes the batch span processor and ends
the exporter, bounded by `timeoutMs` (default 5000ms; both `-main.ts`
entrypoints call it with the default, right after draining NATS and
ending the MySQL pool, right before `process.exit(0)`), returning a
`ShutdownOutcome` (`{ completedWithinBound: boolean }` -- see "Lifecycle,
shutdown, and reinitialization contract" above for exactly what this
does and does not prove). Both entrypoints now check this return value
explicitly and log a warning rather than silently exiting as if nothing
happened when the bound was exceeded:

```typescript
const { completedWithinBound } = await shutdownTelemetry();
if (!completedWithinBound) {
  console.error("[publisher] telemetry shutdown did not complete within its bound; exiting anyway, some spans may be lost");
}
process.exit(0);
```

Verified to return well within its bound even when the exporter cannot
reach anything (`telemetry-resilience.test.ts` test 2), and verified to
actually exercise the TIMEOUT branch (not just the fast-success path)
against a controlled non-responding exporter (test 3).

**A hard kill (`SIGKILL` / Windows `TerminateProcess`) gives this no
chance to run at all.** Any spans still sitting in the batch processor's
buffer at that instant are lost -- permanently, with no retry on restart.
This is a STATED, ACCEPTED limitation, not a bug: nothing in this
codebase's correctness depends on a span having been exported. Tracing is
a diagnostic aid for understanding what happened; the durable audit trail
is the database (`command_receipts`, `consumer_processed`,
`task_consumer_pending`, `consumer_event_conflicts`, `outbox_events`) and
the NATS stream itself, all of which survive a hard kill by construction
(that is the entire point of Phase 1's durability work). Do not use
tracing as a substitute for those durable records, and do not infer
"this event was processed" from "a span for it exists" -- infer it from
the database rows, exactly as every other test in this repository already
does.

## Scope (what this phase deliberately does NOT include)

No React dashboard, no Prometheus/Grafana, no metrics pipeline, no
sampling tuning beyond the SDK default, no automatic instrumentation
(`mysql2`/`nats`/`http` auto-patching) -- explicit domain/messaging spans
only, per this phase's own instructions. No tracing UI/backend beyond the
Collector's own `debug`-exporter log output. None of these are implied to
exist by anything in this document.
