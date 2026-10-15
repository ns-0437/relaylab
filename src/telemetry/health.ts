// Phase 3A Part B: a minimal, dependency-free HTTP server exposing three
// DISTINCT Kubernetes probe endpoints. "Distinct" is the explicit
// requirement (see CLAUDE.md "Health and shutdown behavior") -- a single
// shared health check would conflate "the process is alive" with "the
// process can currently reach NATS," which is exactly the gap
// publisher-main.ts/consumer-main.ts's own existing comments already call
// out ("a healthy /metrics scrape does NOT prove this process can reach
// NATS"). This module gives that existing distinction an HTTP shape a
// kubelet probe can act on, without changing what is actually tracked.
//
// Phase 3A.1: `state.isReady()` is now composed by the CALLER from
// several signals (NATS connectivity, a bounded/cached MySQL ping via
// `startDependencyPing` below, and `!stopping`) -- this module still
// only ever reads one boolean per probe; it does not know what feeds it.
import http from "node:http";
import type { Server } from "node:http";

export interface HealthState {
  /** Composed by the caller (see publisher-main.ts/consumer-main.ts): NATS connectivity AND cached MySQL reachability AND "not currently shutting down." */
  isReady: () => boolean;
}

export interface HealthServer {
  /** Called once the process's one-time initial connect sequence has completed. Never unmark it. */
  markStarted: () => void;
  stop: () => Promise<void>;
}

/**
 * - /healthz/startup: 503 until `markStarted()` is called once, then 200
 *   forever after -- "has this process ever finished initializing,"
 *   distinct from whether it can process work RIGHT NOW.
 * - /healthz/live: always 200 as long as this server is answering at all.
 *   By construction it never inspects NATS/MySQL, so a transient broker
 *   or database outage can never trigger a liveness restart loop.
 * - /healthz/ready: reflects `state.isReady()` at the moment of the
 *   request. This is the ONE endpoint allowed to fail on a connectivity
 *   gap or during shutdown, and is exactly what distinguishes "alive"
 *   from "able to work."
 */
export function startHealthServer(port: number, state: HealthState): HealthServer {
  let started = false;
  const server: Server = http.createServer((req, res) => {
    const url = req.url ?? "/";
    if (url === "/healthz/startup") {
      res.writeHead(started ? 200 : 503, { "content-type": "text/plain" }).end(started ? "started\n" : "starting\n");
    } else if (url === "/healthz/live") {
      res.writeHead(200, { "content-type": "text/plain" }).end("live\n");
    } else if (url === "/healthz/ready") {
      const ready = state.isReady();
      res.writeHead(ready ? 200 : 503, { "content-type": "text/plain" }).end(ready ? "ready\n" : "not ready\n");
    } else {
      res.writeHead(404, { "content-type": "text/plain" }).end("not found\n");
    }
  });
  // Phase 3A.1 regression, found and fixed while validating this patch:
  // `http.Server` has NO default listener for its own 'error' event --
  // an unhandled EventEmitter 'error' event throws, which crashes the
  // WHOLE process. This was never observable in production (every
  // Kubernetes pod has its own network namespace, so a bind collision
  // on :9470 cannot happen there) but DID crash a real publisher
  // process outright in this project's existing test suite, which
  // deliberately spawns two concurrent publisher-main.ts processes on
  // ONE host to test leader failover -- both tried to bind the SAME
  // default port and the second's crash let the first trivially "win"
  // leadership, masking the real bug behind an apparently-passing test.
  // Logging instead of crashing is the fix; kubelet's own probe timeout
  // already treats an unreachable health port as a probe failure, which
  // is the correct, non-silent outcome for a genuine bind problem.
  server.on("error", (err) => {
    console.error(`[health] HTTP server error (port ${port}): ${err instanceof Error ? err.message : String(err)} -- health probes will be unreachable, but the process is NOT crashing over this.`);
  });
  server.listen(port, "0.0.0.0", () => {
    const addr = server.address();
    const boundPort = typeof addr === "object" && addr ? addr.port : port;
    console.log(`[health] listening on :${boundPort} (/healthz/startup, /healthz/live, /healthz/ready)`);
  });

  return {
    markStarted: () => {
      started = true;
    },
    stop: () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
      }),
  };
}

export interface DependencyPing {
  /** The latest cached result -- reading this never performs I/O. */
  isUp: () => boolean;
  stop: () => void;
}

/**
 * Phase 3A.1: a bounded, periodic, CACHED dependency check -- never a
 * query run synchronously inside a probe handler. kubelet's own probe
 * cadence (and any number of concurrent probers) would otherwise turn
 * into an unbounded loop of real database queries with no timeout of
 * its own; this instead runs on ONE fixed interval, each attempt bounded
 * by `timeoutMs`, and callers (readiness) only ever read the latest
 * cached boolean. `ping` is caller-supplied (e.g.
 * `() => pool.query("SELECT 1").then(() => {})`) so this module stays
 * dependency-free.
 */
export function startDependencyPing(ping: () => Promise<void>, intervalMs = 2000, timeoutMs = 1500): DependencyPing {
  let up = false;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout>;

  const tick = () => {
    void (async () => {
      try {
        await Promise.race([
          ping(),
          new Promise<never>((_, reject) => setTimeout(() => reject(new Error("dependency ping timed out")), timeoutMs)),
        ]);
        up = true;
      } catch {
        up = false;
      } finally {
        if (!stopped) timer = setTimeout(tick, intervalMs);
      }
    })();
  };
  timer = setTimeout(tick, 0);

  return {
    isUp: () => up,
    stop: () => {
      stopped = true;
      clearTimeout(timer);
    },
  };
}
