// Demo UI milestone: a small local HTTP API, plain `node:http` (no new
// web framework dependency). Binds to loopback ONLY by default -- this
// is a local portfolio demo, never intended to be reachable from
// another host. Serves the static UI (ui/) and four JSON endpoints.
//
// Accepts only `mode` from a fixed allowlist ("deterministic",
// "ai-simulated", "ai-live") -- never an arbitrary repository path, a
// shell command, or a provider URL; the fixture repository and
// Docker-based verification command are entirely fixed by the existing
// pipeline this file calls into, not supplied by the caller.
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createPool } from "../db/pool.js";
import { loadMysqlConfig } from "../config.js";
import { applyMigrations } from "../db/migrate.js";
import { startRun, cancelRun, activeRunId, RunConflictError, LiveModeDisabledError } from "./executor.js";
import { getRun, listRuns, reconcileOnStartup, type RunMode } from "./runs-store.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const UI_DIR = path.join(HERE, "..", "..", "ui");
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

const STATIC_TYPES: Record<string, string> = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8" };

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

export async function createApiServer() {
  const pool = createPool(loadMysqlConfig());
  await applyMigrations(loadMysqlConfig());
  const interrupted = await reconcileOnStartup(pool);
  if (interrupted > 0) {
    console.log(`[api] reconciled ${interrupted} run(s) left in an in-flight status by a prior process -- marked "interrupted", never displayed as running.`);
  }

  const server = createServer(async (req, res) => {
    try {
      if (!originIsTrusted(req)) {
        sendError(res, 403, "untrusted_origin", "this request's Origin header is not this server's own loopback origin");
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
        sendJson(res, 200, { liveModeEnabled: process.env.RELAYLAB_LIVE_MODE === "1", allowedModes: ALLOWED_MODES });
        return;
      }

      if (req.method === "GET" && pathname === "/api/runs") {
        const rows = await listRuns(pool);
        sendJson(res, 200, { runs: rows, activeRunId: activeRunId() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/runs") {
        // Mutations must be JSON -- never accept form-encoded or
        // arbitrary content types for a state-changing request.
        const contentType = req.headers["content-type"] ?? "";
        if (!contentType.includes("application/json")) {
          sendError(res, 415, "unsupported_media_type", "POST /api/runs requires Content-Type: application/json");
          return;
        }
        let body: unknown;
        try {
          body = await readJsonBody(req);
        } catch (err) {
          sendError(res, 400, "invalid_body", err instanceof Error ? err.message : "invalid request body");
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
          } else {
            sendError(res, 500, "start_failed", err instanceof Error ? err.message.slice(0, 300) : "failed to start run");
          }
        }
        return;
      }

      const detailMatch = pathname.match(/^\/api\/runs\/([^/]+)$/);
      if (req.method === "GET" && detailMatch) {
        const run = await getRun(pool, decodeURIComponent(detailMatch[1]!));
        if (!run) {
          sendError(res, 404, "not_found", "no run with that id");
          return;
        }
        sendJson(res, 200, { run });
        return;
      }

      const cancelMatch = pathname.match(/^\/api\/runs\/([^/]+)\/cancel$/);
      if (req.method === "POST" && cancelMatch) {
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
    } catch (err) {
      sendError(res, 500, "internal_error", err instanceof Error ? err.message.slice(0, 300) : "internal error");
    }
  });

  return { server, pool };
}

if (fileURLToPath(import.meta.url) === process.argv[1]) {
  createApiServer()
    .then(({ server }) => {
      server.listen(PORT, HOST, () => {
        console.log(`[api] listening on http://${HOST}:${PORT} (loopback only unless RELAYLAB_API_HOST is overridden)`);
        console.log(`[api] live mode: ${process.env.RELAYLAB_LIVE_MODE === "1" ? "ENABLED server-side (still requires per-request confirmPaidCall)" : "disabled"}`);
      });
    })
    .catch((err) => {
      console.error("[api] fatal:", err);
      process.exitCode = 1;
    });
}
