// Phase 1.2 audit, Part A.1: a shared, robust child-process manager for
// integration tests/demos that spawn real publisher-main.ts/consumer-main.ts
// processes. Replaces four slightly-different ad hoc `spawned: ChildProcess[]`
// + manual kill arrays that used to be duplicated across
// crash-windows.ts, recovery-scenario.ts, publisher-leadership.test.ts,
// and shutdown.test.ts -- which is exactly how a real orphan happened:
// one of those ad hoc arrays' cleanup never ran because the whole
// harness process itself was detached from its controlling shell before
// it reached its own `after()`/`finally`, and nothing OUTSIDE that
// process was tracking its children independently.
//
// This module does not solve "my own process got detached" (that is an
// orchestration-layer problem, not a solve this code can own), but it
// does guarantee that WITHIN one run of a test/demo, every child this
// harness spawned is registered the instant spawn() returns (covering a
// process that fails before reporting readiness), is asked to shut down
// gracefully first, is bounded-waited, is force-killed only if still
// alive after that wait, and has its actual exit awaited and its
// listeners/timers cleaned up -- on EVERY exit path: success, assertion
// failure, startup failure, or timeout.

import { fork, type ChildProcess } from "node:child_process";
import path from "node:path";

export interface ManagedChildOptions {
  /** Human-readable label used in console output, e.g. "pub-A". */
  label: string;
  /** Absolute path to the *-main.ts entrypoint to fork. */
  entry: string;
  /** cwd for the forked process (normally the repo root). */
  cwd: string;
  env: NodeJS.ProcessEnv;
}

export interface ManagedChild {
  readonly process: ChildProcess;
  readonly label: string;
  readonly lines: string[];
  /** Resolves once this specific child has actually exited (code/signal). */
  readonly exited: Promise<{ code: number | null; signal: string | null }>;
}

/**
 * Tracks every child this harness spawns, from the moment `spawn()` is
 * called (not from the moment it signals readiness), so a process that
 * fails to start is still in scope for cleanup. Call `stopAll()` in a
 * `finally` block around the entire test/scenario body -- not only in an
 * `after()` hook, so an exception thrown mid-scenario (assertion
 * failure, timeout, startup failure) still triggers cleanup before the
 * error propagates.
 */
export class ProcessHarness {
  private readonly children = new Set<ManagedChild>();
  private stopped = false;

  spawn(opts: ManagedChildOptions): ManagedChild {
    const child = fork(opts.entry, [], {
      cwd: opts.cwd,
      env: opts.env,
      execArgv: ["--import", "tsx"],
      stdio: ["ignore", "pipe", "pipe", "ipc"],
    });
    const lines: string[] = [];
    const onStdout = (d: Buffer) => {
      const text = String(d);
      lines.push(text);
      process.stdout.write(`[${opts.label}] ${text}`);
    };
    const onStderr = (d: Buffer) => process.stderr.write(`[${opts.label}] ${d}`);
    child.stdout?.on("data", onStdout);
    child.stderr?.on("data", onStderr);

    const exited = new Promise<{ code: number | null; signal: string | null }>((resolve) => {
      child.once("exit", (code, signal) => {
        child.stdout?.off("data", onStdout);
        child.stderr?.off("data", onStderr);
        resolve({ code, signal });
      });
    });

    const managed: ManagedChild = { process: child, label: opts.label, lines, exited };
    // Registered BEFORE this function returns -- covers a child that
    // crashes or exits before ever reporting readiness over IPC.
    this.children.add(managed);
    child.once("exit", () => this.children.delete(managed));
    return managed;
  }

  /** Current count of children this harness still considers live-tracked (not yet exited). */
  get liveCount(): number {
    return this.children.size;
  }

  /**
   * Stops every tracked child: graceful IPC "shutdown" request first (if
   * the child still has an open channel), bounded wait for it to exit on
   * its own, then SIGKILL anything still alive, and awaits REAL exit for
   * each -- never just "sent a signal and moved on". Idempotent and safe
   * to call multiple times or on an empty set (e.g. a scenario that threw
   * before spawning anything). Never throws itself, so it's safe to put
   * directly in a `finally` without a second try/catch around it.
   */
  async stopAll(gracefulTimeoutMs = 3000): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    const snapshot = [...this.children];
    await Promise.all(snapshot.map((c) => this.stopOne(c, gracefulTimeoutMs)));
  }

  private async stopOne(child: ManagedChild, gracefulTimeoutMs: number): Promise<void> {
    const proc = child.process;
    if (proc.exitCode !== null || proc.signalCode !== null) return; // already exited

    // Graceful attempt: IPC message if the channel is still open (the
    // real mechanism publisher-main.ts/consumer-main.ts listen for -- see
    // their process.on("message", ...) handlers, added specifically
    // because child.kill("SIGTERM") does not deliver a catchable signal
    // on this Windows machine; see docs/decisions/0009-harness-child-cleanup.md).
    if (proc.connected) {
      try {
        proc.send("shutdown");
      } catch {
        // channel closed between the check and the send; fall through to force-kill below
      }
    }

    const gracefulExit = await raceWithTimeout(child.exited, gracefulTimeoutMs);
    if (gracefulExit !== "timeout") return; // exited on its own within the bound

    if (proc.exitCode === null && proc.signalCode === null) {
      proc.kill("SIGKILL");
    }
    await child.exited; // unconditionally await real exit -- never assume kill() was instantaneous
  }
}

function raceWithTimeout<T>(promise: Promise<T>, ms: number): Promise<T | "timeout"> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve("timeout"), ms);
    promise.then((v) => {
      clearTimeout(timer);
      resolve(v);
    });
  });
}

/** Builds the standard env for a forked RelayLab process pointed at the disposable test db/namespace. */
export function testChildEnv(testDatabase: string, extra: Record<string, string> = {}): NodeJS.ProcessEnv {
  return {
    ...process.env,
    RELAYLAB_NAMESPACE: "relaylab-test",
    RELAYLAB_MYSQL_DATABASE: testDatabase,
    // Disabled by default: these harness-spawned processes are used by
    // tests that are NOT about telemetry (ownership, idempotency, gap
    // handling, shutdown, crash windows, ...), and must not depend on a
    // Collector being reachable or pay its shutdown-flush timeout. Tests
    // that ARE about telemetry (tests/integration/telemetry*.test.ts)
    // override this explicitly via `extra`. See docs/OBSERVABILITY.md
    // "disabled mode".
    RELAYLAB_OTEL_DISABLED: "1",
    ...extra,
  };
}

export function repoRootFrom(testFileDir: string): string {
  return path.join(testFileDir, "..", "..");
}
