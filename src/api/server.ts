// Demo UI milestone: a small local HTTP API, plain `node:http` (no new
// web framework dependency). Binds to loopback ONLY by default -- this
// is a local portfolio demo, never intended to be reachable from
// another host. Serves the built UI (ui/dist/) and four JSON endpoints.
//
// Accepts only `mode` from a fixed allowlist ("deterministic",
// "ai-simulated", "ai-live") -- never an arbitrary repository path, a
// shell command, or a provider URL; the fixture repository and
// Docker-based verification command are entirely fixed by the existing
// pipeline this file calls into, not supplied by the caller.
import { createServer, type IncomingMessage, type ServerResponse, type Server } from "node:http";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type mysql from "mysql2/promise";
import { createPool } from "../db/pool.js";
import { loadMysqlConfig } from "../config.js";
import { applyMigrations } from "../db/migrate.js";
import {
  startRun,
  cancelRun,
  activeRunId,
  awaitActiveRun,
  blockAdmission,
  cancelActiveRunForLockLoss,
  degradedRunNote,
  RunConflictError,
  LiveModeDisabledError,
  RunStartError,
  AdmissionBlockedError,
} from "./executor.js";
import { getRun, listRuns, reconcileOnStartup, type RunMode } from "./runs-store.js";
import { acquireSingletonLock, type SingletonLock, type SingletonLockTimings } from "./singleton-lock.js";

/**
 * Bounded, explicit -- shutdown never waits indefinitely, and never
 * claims to have finished cleanup if this elapses first. Read fresh
 * INSIDE createApiServer (not cached as a module-level constant at
 * import time) -- same "read fresh on every call" pattern already used
 * for RELAYLAB_LIVE_MODE below, so a test can set this env var right
 * before calling createApiServer() and have it actually take effect,
 * regardless of ESM import hoisting order.
 */
function shutdownTimeoutMs(): number {
  const value = Number(process.env.RELAYLAB_SHUTDOWN_TIMEOUT_MS ?? 15_000);
  if (!Number.isSafeInteger(value) || value <= 0 || value > 120_000) throw new Error("invalid shutdown timeout");
  return value;
}

const HERE = path.dirname(fileURLToPath(import.meta.url));
const UI_DIR = path.join(HERE, "..", "..", "ui", "dist");
const PORT = Number(process.env.RELAYLAB_API_PORT ?? 4555);
const HOST = process.env.RELAYLAB_API_HOST ?? "127.0.0.1";
const MAX_BODY_BYTES = 16 * 1024;
const ALLOWED_MODES: readonly RunMode[] = ["deterministic", "ai-simulated", "ai-live"];

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { "content-type": "application/json; charset=utf-8", "content-length": Buffer.byteLength(payload) });
  res.end(payload);
}

function sendError(res: ServerResponse, status: number, code: string, message: string): void {
  sendJson(res, status, { error: { code, message } });
}

async function readJsonBody(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += (chunk as Buffer).length;
    if (size > MAX_BODY_BYTES) throw new Error("request body too large");
    chunks.push(chunk as Buffer);
  }
  if (chunks.length === 0) return {};
  const text = Buffer.concat(chunks).toString("utf8");
  try {
    return JSON.parse(text);
  } catch {
    throw new Error("request body is not valid JSON");
  }
}

/**
 * Applied to EVERY mutating request (POST /api/runs AND POST
 * /api/runs/:id/cancel) -- consistent validation, not special-cased per
 * route. A request with no body at all is accepted (cancel needs none);
 * a request that DOES send a body must declare and actually be JSON.
 */
async function requireJsonMutation(req: IncomingMessage, res: ServerResponse): Promise<unknown | undefined> {
  const hasBody = req.headers["content-length"] !== "0" && req.headers["content-length"] !== undefined;
  const contentType = req.headers["content-type"] ?? "";
  if (hasBody && !contentType.includes("application/json")) {
    sendError(res, 415, "unsupported_media_type", "this request requires Content-Type: application/json when a body is sent");
    return undefined;
  }
  try {
    return await readJsonBody(req);
  } catch (err) {
    sendError(res, 400, "invalid_body", err instanceof Error ? err.message : "invalid request body");
    return undefined;
  }
}

/**
 * Rejects an untrusted browser Origin outright. A request with NO
 * Origin header (curl, server-to-server, same-origin top-level
 * navigation) is allowed; a request WITH one must match this server's
 * own loopback origin exactly.
 */
function originIsTrusted(req: IncomingMessage): boolean {
  const origin = req.headers.origin;
  if (!origin) return true;
  const allowed = new Set([`http://127.0.0.1:${PORT}`, `http://localhost:${PORT}`]);
  return allowed.has(origin);
}

const STATIC_TYPES: Record<string, string> = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".map": "application/json; charset=utf-8" };

async function serveStatic(req: IncomingMessage, res: ServerResponse, pathname: string): Promise<boolean> {
  const rel = pathname === "/" ? "index.html" : pathname.slice(1);
  // No path traversal: resolve and re-confirm the result is still
  // directly inside UI_DIR before ever reading it.
  const resolved = path.resolve(UI_DIR, rel);
  if (!resolved.startsWith(UI_DIR + path.sep) && resolved !== UI_DIR) return false;
  try {
    const content = await readFile(resolved);
    const ext = path.extname(resolved);
    res.writeHead(200, { "content-type": STATIC_TYPES[ext] ?? "application/octet-stream" });
    res.end(content);
    return true;
  } catch {
    return false;
  }
}

/** `timedOut: true` means SHUTDOWN_TIMEOUT_MS elapsed before cleanup finished -- resources may still be open. Never conflated with a genuine `timedOut: false` completion. */
export interface ShutdownResult {
  timedOut: boolean;
}

export interface ApiServerHandle {
  server: Server;
  pool: mysql.Pool;
  /** Bounded graceful shutdown: admission is blocked and any active run is cancelled IMMEDIATELY; stopping the HTTP server, awaiting the run's own cleanup, releasing the lock, and closing the pool are then raced against SHUTDOWN_TIMEOUT_MS. See ShutdownResult. */
  shutdown: () => Promise<ShutdownResult>;
}

export async function createApiServer(options?: { lockTimings?: SingletonLockTimings }): Promise<ApiServerHandle> {
  const SHUTDOWN_TIMEOUT_MS = shutdownTimeoutMs();
  const pool = createPool(loadMysqlConfig());
  let lock: SingletonLock | null = null;
  try {
    await applyMigrations(loadMysqlConfig());

    // Singleton lock FIRST, reconciliation only AFTER it is confirmed
    // held -- a second instance against the SAME database must never
    // reconcile (or otherwise touch) runs the first instance still owns.
    const lockOwnerId = `demo-api-${process.pid}-${randomUUID()}`;
    lock = await acquireSingletonLock(pool, lockOwnerId, () => {
      // Confirmed lock loss (or the conservative expiry watchdog)
      // fires exactly once: block all further admission AND cancel any
      // execution this now-stale instance still has running, so it can
      // never overwrite a successor instance's view of demo_runs.
      console.error("[api] singleton lock lost -- another instance appears to have taken over, or this lease could not be confirmed renewed in time. Blocking further run starts and cancelling any active run.");
      blockAdmission();
      cancelActiveRunForLockLoss();
    }, options?.lockTimings);
    if (!lock) {
      throw new Error("another relaylab demo API instance already holds the singleton lock against this database -- refusing to start a second one");
    }

    const interrupted = await reconcileOnStartup(pool);
    if (interrupted > 0) {
      console.log(`[api] reconciled ${interrupted} run(s) left in an in-flight status by a prior process -- marked "interrupted", never displayed as running.`);
    }
  } catch (err) {
    // Startup failure AFTER the pool (and possibly the lock) was
    // already acquired must still release both -- never leave a lock
    // lease or a pool connection orphaned just because a LATER startup
    // step (reconciliation, migrations) failed.
    await lock?.stop().catch(() => {});
    await pool.end().catch(() => {});
    throw err;
  }

  let shuttingDown = false;

  const server = createServer(async (req, res) => {
    try {
      if (!originIsTrusted(req)) {
        sendError(res, 403, "untrusted_origin", "this request's Origin header is not this server's own loopback origin");
        return;
      }
      if (shuttingDown) {
        sendError(res, 503, "shutting_down", "this server is shutting down");
        return;
      }

      const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
      const pathname = url.pathname;

      if (req.method === "GET" && !pathname.startsWith("/api/")) {
        if (await serveStatic(req, res, pathname)) return;
        sendError(res, 404, "not_found", `no static asset at ${pathname}`);
        return;
      }

      if (req.method === "GET" && pathname === "/api/status") {
        // Never includes credentials -- only whether live mode is
        // server-side enabled, for the UI's badge.
        sendJson(res, 200, { liveModeEnabled: process.env.RELAYLAB_LIVE_MODE === "1", allowedModes: ALLOWED_MODES, lockHeld: !lock.isLost() });
        return;
      }

      if (req.method === "GET" && pathname === "/api/runs") {
        const rows = await listRuns(pool);
        sendJson(res, 200, { runs: rows.map((run) => {
          const degraded = degradedRunNote(run.runId);
          return degraded ? { ...run, status: "interrupted", detail: { ...run.detail, error: degraded } } : run;
        }), activeRunId: activeRunId() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/runs") {
        const body = await requireJsonMutation(req, res);
        if (body === undefined) return; // requireJsonMutation already sent the error response

        if (lock.isLost()) {
          sendError(res, 503, "singleton_lock_lost", "this server instance no longer holds the singleton lock -- refusing to start a new run");
          return;
        }

        const { mode, confirmPaidCall } = (body ?? {}) as { mode?: unknown; confirmPaidCall?: unknown };
        if (typeof mode !== "string" || !ALLOWED_MODES.includes(mode as RunMode)) {
          sendError(res, 400, "invalid_mode", `"mode" must be one of: ${ALLOWED_MODES.join(", ")}`);
          return;
        }
        if (confirmPaidCall !== undefined && typeof confirmPaidCall !== "boolean") {
          sendError(res, 400, "invalid_field", '"confirmPaidCall" must be a boolean if present');
          return;
        }
        try {
          const { runId } = await startRun(pool, mode as RunMode, confirmPaidCall === true);
          sendJson(res, 201, { runId });
        } catch (err) {
          if (err instanceof RunConflictError) {
            sendError(res, 409, "run_conflict", err.message);
          } else if (err instanceof LiveModeDisabledError) {
            sendError(res, 403, "live_mode_disabled", err.message);
          } else if (err instanceof AdmissionBlockedError) {
            sendError(res, 503, "admission_blocked", err.message);
          } else if (err instanceof RunStartError) {
            sendError(res, 500, err.code, err.message);
          } else {
            sendError(res, 500, "internal_error", "an internal error occurred starting this run");
          }
        }
        return;
      }

      const detailMatch = pathname.match(/^\/api\/runs\/([^/]+)$/);
      if (req.method === "GET" && detailMatch) {
        const runId = decodeURIComponent(detailMatch[1]!);
        const run = await getRun(pool, runId);
        if (!run) {
          sendError(res, 404, "not_found", "no run with that id");
          return;
        }
        // A sanitized, same-process-only degraded note overlaid when
        // this run's TRUE terminal status could not be durably
        // recorded -- the durable row (possibly stale) is still
        // returned alongside it, never replaced by a guess.
        const degraded = degradedRunNote(runId);
        sendJson(res, 200, degraded ? { run, degraded } : { run });
        return;
      }

      const cancelMatch = pathname.match(/^\/api\/runs\/([^/]+)\/cancel$/);
      if (req.method === "POST" && cancelMatch) {
        const body = await requireJsonMutation(req, res);
        if (body === undefined) return;
        const runId = decodeURIComponent(cancelMatch[1]!);
        try {
          cancelRun(runId);
          sendJson(res, 202, { cancelling: true });
        } catch {
          sendError(res, 409, "not_active", "this run is not currently active (already finished, or never existed)");
        }
        return;
      }

      sendError(res, 404, "not_found", `no route for ${req.method} ${pathname}`);
    } catch {
      sendError(res, 500, "internal_error", "an internal error occurred handling this request");
    }
  });

  let shutdownOnce: Promise<ShutdownResult> | null = null;
  /**
   * Bounded and prompt: admission is blocked and any active run is
   * cancelled IMMEDIATELY (synchronously, before anything is awaited),
   * not after waiting for `server.close()`'s own in-flight-connection
   * drain to finish. Everything else (closing the HTTP server, awaiting
   * the active run's own cleanup, stopping lock renewal, closing the
   * pool) runs against an explicit `SHUTDOWN_TIMEOUT_MS` deadline. If
   * that deadline elapses first, this returns `{ timedOut: true }` --
   * it NEVER claims `completed: true` merely because the wait ended;
   * the caller (the CLI entry point below) reports that outcome
   * honestly rather than assuming cleanup finished.
   */
  const shutdown = (): Promise<ShutdownResult> => {
    if (shutdownOnce) return shutdownOnce;
    shuttingDown = true;
    blockAdmission();
    const runningIdAtShutdown = activeRunId();
    if (runningIdAtShutdown) {
      console.log(`[api] shutdown: cancelling the active run (${runningIdAtShutdown}) immediately...`);
      try {
        cancelRun(runningIdAtShutdown);
      } catch {
        /* already finished between the check and the cancel call -- fine. */
      }
    }

    const cleanup = (async (): Promise<void> => {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await awaitActiveRun();
      await lock.stop();
      await pool.end();
    })();

    let deadlineTimer: ReturnType<typeof setTimeout>;
    shutdownOnce = Promise.race([
      cleanup.then((): ShutdownResult => ({ timedOut: false })),
      new Promise<ShutdownResult>((resolve) => { deadlineTimer = setTimeout(() => resolve({ timedOut: true }), SHUTDOWN_TIMEOUT_MS); }),
    ]).finally(() => clearTimeout(deadlineTimer));
    return shutdownOnce;
  };

  return { server, pool, shutdown };
}

if (fileURLToPath(import.meta.url) === process.argv[1]) {
  createApiServer()
    .then((handle) => {
      handle.server.listen(PORT, HOST, () => {
        console.log(`[api] listening on http://${HOST}:${PORT} (loopback only unless RELAYLAB_API_HOST is overridden)`);
        console.log(`[api] live mode: ${process.env.RELAYLAB_LIVE_MODE === "1" ? "ENABLED server-side (still requires per-request confirmPaidCall)" : "disabled"}`);
      });
      const onSignal = () => {
        console.log("[api] shutdown signal received -- blocking admission, cancelling any active run immediately...");
        handle
          .shutdown()
          .then((result) => {
            if (result.timedOut) {
              // Honest, not a success: the bounded deadline elapsed
              // before cleanup (HTTP close / active-run await / lock
              // release / pool close) actually finished -- never
              // reported as a clean exit.
              console.error(`[api] shutdown TIMED OUT after ${shutdownTimeoutMs()}ms -- cleanup may be incomplete.`);
              process.exit(1);
            } else {
              console.log("[api] shutdown complete.");
              process.exit(0);
            }
          })
          .catch((err) => {
            console.error("[api] error during shutdown:", err);
            process.exit(1);
          });
      };
      process.on("SIGINT", onSignal);
      process.on("SIGTERM", onSignal);
    })
    .catch((err) => {
      console.error("[api] fatal:", err instanceof Error ? err.message : err);
      process.exitCode = 1;
    });
}
