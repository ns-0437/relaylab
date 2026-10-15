import { connect, type NatsConnection, type JetStreamClient, type JetStreamManager, AckPolicy, RetentionPolicy } from "nats";
import { eventsDurable, eventsStream, eventsSubject } from "../config.js";

export interface NatsHandles {
  nc: NatsConnection;
  js: JetStreamClient;
  jsm: JetStreamManager;
}

/**
 * Monitoring-reliability patch, section 2 diagnostic finding (see
 * docs/OBSERVABILITY.md "NATS reconnect and recovery contract" for the
 * full causal writeup, captured with REAL evidence from
 * `nc.status()`/`nc.isClosed()` during an actual `docker stop`/`docker
 * start` of the NATS container, not assumed):
 *
 *   trigger: the NATS broker becomes unreachable.
 *   failing code path: nats.js's OWN reconnect loop retries at a fixed
 *     interval (`reconnectTimeWait`, default 2000ms) up to a fixed
 *     attempt count (`maxReconnectAttempts`, default 10) -- observed via
 *     `nc.status()` emitting a `disconnect` event followed by repeated
 *     `reconnecting` events roughly 2s apart.
 *   resulting state: once that budget is exhausted (observed at ~19s in
 *     a real test, matching 10 x ~2s), the client transitions to
 *     PERMANENTLY closed -- `nc.isClosed()` becomes `true` and EVERY
 *     subsequent operation (`fetch()`, `consumers.info()`, publish)
 *     throws `NatsError CONNECTION_CLOSED`, observed to remain `true`
 *     even ~17s after the broker was confirmed back up and healthy.
 *   why work does not resume: nats.js provides NO automatic
 *     "resurrection" of an already-permanently-closed connection object
 *     -- this is documented nats.js behavior, not a bug in the library --
 *     and prior to this patch, NEITHER `publisher-main.ts` NOR
 *     `consumer-main.ts` ever checked `nc.isClosed()` or called
 *     `connectNats()` again; their main loops simply kept retrying the
 *     SAME dead handles forever, logging "iteration failed"/"batch
 *     failed" on every pass with no path back to a working connection.
 *     This is NOT a "waited too little" timing issue -- it is a missing
 *     recovery code path, demonstrated by the fact that `nc.isClosed()`
 *     stayed `true` indefinitely regardless of how long the broker had
 *     already been back up.
 *
 * Fixed with an APPLICATION-LEVEL reconnect: `publisher-main.ts`/
 * `consumer-main.ts` now check `nc.isClosed()` at the top of each
 * iteration and, if true, call `connectNats()` again (a fresh TCP/NATS
 * handshake, independent of the dead object) with the SAME bounded
 * backoff pattern already used for DB/NATS iteration failures elsewhere
 * in those files -- not a new subsystem, not unlimited retries, not a
 * second competing reconnect mechanism layered on top of nats.js's own
 * (which still handles ordinary transient drops within its own budget,
 * unchanged). `ensureEventsStream`/`ensureDurableConsumer` are
 * idempotent and safe to re-run after a reconnect: the durable
 * consumer's delivery position lives on the NATS SERVER, not the client
 * connection, so a client-side reconnect-and-recreate never resets it
 * (verified: see docs/OBSERVABILITY.md and
 * tests/integration/monitoring-scenarios.manual.ts).
 */
export interface ReconnectBudget {
  maxReconnectAttempts: number;
  reconnectTimeWaitMs: number;
}

/**
 * nats.js's own documented defaults (10 attempts, 2000ms apart, ~19-20s
 * total budget) -- used unless overridden. A test-only override exists
 * SPECIFICALLY so `tests/integration/monitoring-scenarios.manual.ts` can
 * exercise the "beyond budget" application-reconnect path in seconds
 * rather than ~20s real time, via `RELAYLAB_NATS_MAX_RECONNECT_ATTEMPTS`/
 * `RELAYLAB_NATS_RECONNECT_TIME_WAIT_MS` -- production deployment never
 * sets these, so production keeps nats.js's own defaults unchanged.
 */
export function defaultReconnectBudget(): ReconnectBudget {
  return {
    maxReconnectAttempts: Number(process.env.RELAYLAB_NATS_MAX_RECONNECT_ATTEMPTS ?? 10),
    reconnectTimeWaitMs: Number(process.env.RELAYLAB_NATS_RECONNECT_TIME_WAIT_MS ?? 2000),
  };
}

export async function connectNats(servers: string, budget: ReconnectBudget = defaultReconnectBudget()): Promise<NatsHandles> {
  const nc = await connect({
    servers,
    maxReconnectAttempts: budget.maxReconnectAttempts,
    reconnectTimeWait: budget.reconnectTimeWaitMs,
  });
  const jsm = await nc.jetstreamManager();
  const js = nc.jetstream();
  return { nc, js, jsm };
}

/**
 * The bounded readiness/connection signal section 5 asked for: a
 * Prometheus /metrics endpoint answering 200 proves the PROCESS is
 * alive, never that it can actually reach NATS and do work (the metrics
 * HTTP server starts before `connectNats` even runs -- see
 * src/messaging/{publisher,consumer}-main.ts). This tracks the REAL
 * connection state via `nc.status()`'s own event stream (not polled --
 * event-driven, so it reflects a transition the instant nats.js reports
 * it) and exposes it as a plain boolean getter for a Prometheus gauge
 * (`relaylab_nats_connected`, see publisher-main.ts/consumer-main.ts).
 * Returns the SAME boolean-state object a caller should keep reading
 * after a reconnect replaces `nc` -- callers construct a NEW tracker for
 * a NEW `nc` (see `connectNats` call sites in *-main.ts), so there is
 * never ambiguity about which connection's state is being observed.
 */
export function watchConnectionState(nc: NatsConnection): { isConnected: () => boolean } {
  let connected = true; // nc is already connected at the moment this is called
  (async () => {
    for await (const s of nc.status()) {
      if (s.type === "disconnect" || s.type === "reconnecting") connected = false;
      else if (s.type === "reconnect") connected = true;
      // A permanently closed connection's status() iterator itself ends
      // (see the diagnostic finding above) -- the loop simply exits,
      // leaving `connected` at its last value (false, since a
      // disconnect/reconnecting event always precedes permanent
      // closure) rather than needing a separate terminal case here.
    }
  })().catch(() => {
    // The status() iterator throwing is not expected, but must never
    // crash the process over an OBSERVABILITY concern -- see the
    // identical principle for telemetry/metrics export failures
    // elsewhere in this project.
  });
  return { isConnected: () => connected };
}

export interface StreamIdentity {
  /** The stream's own name (see src/config.ts eventsStream()). */
  name: string;
  /**
   * The stream's ISO-8601 creation timestamp, as reported by NATS JetStream
   * itself (`StreamInfo.created`). Changes every time this stream is
   * DELETED and recreated under the same name -- this project uses it as
   * a lightweight, no-new-subsystem "generation" identifier (see
   * src/messaging/quarantine.ts and docs/OBSERVABILITY.md "quarantine
   * identity and the supported stream lifecycle") specifically so a
   * quarantine row's dedup key can never collide across two DIFFERENT
   * stream incarnations that happen to reuse the same low sequence
   * numbers.
   */
  generation: string;
}

/** Idempotently ensures the Phase-1 events stream exists, and returns its identity (see StreamIdentity). */
export async function ensureEventsStream(jsm: JetStreamManager): Promise<StreamIdentity> {
  const stream = eventsStream();
  try {
    const info = await jsm.streams.info(stream);
    return { name: stream, generation: info.created };
  } catch {
    const info = await jsm.streams.add({
      name: stream,
      subjects: [eventsSubject()],
      retention: RetentionPolicy.Limits,
      max_age: 7 * 24 * 60 * 60 * 1_000_000_000, // 7 days, in ns
    });
    return { name: stream, generation: info.created };
  }
}

/** Idempotently ensures the Phase-1 durable pull consumer exists -- re-running this after a reconnect is SAFE and does NOT reset delivery position (see the file header comment: that state lives on the server). */
export async function ensureDurableConsumer(jsm: JetStreamManager): Promise<void> {
  const stream = eventsStream();
  const durable = eventsDurable();
  try {
    await jsm.consumers.info(stream, durable);
  } catch {
    await jsm.consumers.add(stream, {
      durable_name: durable,
      ack_policy: AckPolicy.Explicit,
      max_deliver: 10,
      // Governs worst-case recovery latency for a message that was
      // delivered to a worker that crashed before acking it: the broker
      // will not redeliver until this elapses (server-side disconnect
      // detection can expedite it, but is not guaranteed). 8s is a
      // Phase-1 default, not a measured production value -- see
      // docs/ROADMAP.md.
      ack_wait: 8 * 1_000_000_000, // 8s, in ns
    });
  }
}
