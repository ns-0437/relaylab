// Phase 1.2.1 Part A.1 fix: graceful shutdown must not call process.exit()
// on the successful path. A hard `process.exit(0)` right after shutdown
// work made it IMPOSSIBLE to tell "this process released every resource
// on its own" from "this process was forced to end regardless of
// whether it actually had" -- the two look identical from outside once
// `process.exit()` is in the picture. Removing it means the process can
// only actually end once Node's event loop has nothing left keeping it
// alive (every timer/socket/handle released), which is the thing this
// project's shutdown code must actually guarantee, not merely intend.
//
// The externally enforced hard-stop deadline (SIGKILL after a bounded
// wait) still exists -- see tests/integration/process-harness.ts
// `stopOne`'s `gracefulTimeoutMs` -- it is just OWNED by the test
// harness/supervisor, not faked from inside the process being shut down.
//
// This helper is diagnostic only: it NEVER forces an exit and NEVER
// cancels anything. It schedules one unref'd check (so the check itself
// can never be the reason the process stays alive) that reports,
// honestly, whether Node still considers any handle active after the
// graceful-shutdown steps have all completed. If nothing is active, the
// process has already exited naturally by the time this fires and
// nothing is printed.
export function reportLingeringHandlesAfter(label: string, delayMs = 2000): void {
  const timer = setTimeout(() => {
    const proc = process as unknown as { _getActiveHandles?: () => unknown[] };
    const handles = proc._getActiveHandles?.() ?? [];
    if (handles.length > 0) {
      console.error(
        `[${label}] ${handles.length} handle(s) still active ${delayMs}ms after completing graceful ` +
          `shutdown steps (NATS drain, MySQL pool end, telemetry shutdown all returned) -- this process ` +
          `has NOT exited naturally and something is holding a resource open. This is reported, not masked: ` +
          `no process.exit() call will paper over it. See docs/OBSERVABILITY.md "graceful shutdown".`,
      );
    }
  }, delayMs);
  timer.unref();
}
