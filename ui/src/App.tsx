// Demo UI milestone: the local demo's only React component tree.
// Built with TypeScript + esbuild, locally bundled -- no CDN, no
// runtime dependency on network access to load React. Polling is
// COMPLETION-BASED (schedule the next poll only after the current one
// resolves) specifically so a slow request can never overlap with
// another in-flight one or leave a stale response racing a newer one.
import React, { useCallback, useEffect, useRef, useState } from "react";
import { getStatus, listRuns, startRun, cancelRun, ApiError, type RunRow, type RunMode } from "./api.js";

const POLL_DELAY_MS = 2000;

const STAGES: { key: RunRow["detail"]["stage"]; label: string }[] = [
  { key: "generation", label: "Generation" },
  { key: "verification", label: "Verification" },
  { key: "checkpoint", label: "Checkpoint" },
  { key: "transfer", label: "Transfer" },
  { key: "restore_ack", label: "Restore / Ack" },
];

const TERMINAL_STATUSES = new Set(["completed", "failed", "cancelled", "interrupted"]);

function stageState(run: RunRow | null, stageKey: RunRow["detail"]["stage"]): "pending" | "active" | "done" | "failed" | "skipped" {
  if (!run) return "pending";
  const order = STAGES.map((s) => s.key);
  const currentIdx = order.indexOf(run.detail.stage);
  const stageIdx = order.indexOf(stageKey);
  if (TERMINAL_STATUSES.has(run.status)) {
    if (run.status === "completed") return "done";
    if (stageIdx < currentIdx) return "done";
    if (stageIdx === currentIdx) return "failed";
    // A terminal (non-completed) run never reaches a LATER stage --
    // this is "skipped", a distinct, honest label, never "pending"
    // (which implies it still might happen).
    return "skipped";
  }
  if (stageIdx < currentIdx) return "done";
  if (stageIdx === currentIdx) return "active";
  return "pending";
}

function Badge({ mode }: { mode: RunMode }): React.ReactElement {
  const className = mode === "ai-live" ? "badge live" : mode === "ai-simulated" ? "badge simulated" : "badge deterministic";
  const text = mode === "ai-live" ? "LIVE (real API call)" : mode === "ai-simulated" ? "SIMULATED provider" : "deterministic (no AI)";
  return <span className={className}>{text}</span>;
}

function StartForm({ onStarted, disabled, liveModeEnabled }: { onStarted: (runId: string) => void; disabled: boolean; liveModeEnabled: boolean }): React.ReactElement {
  const [mode, setMode] = useState<RunMode>("ai-simulated");
  const [confirmed, setConfirmed] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setStarting(true);
    try {
      const result = await startRun(mode, mode === "ai-live" ? confirmed : undefined);
      onStarted(result.runId);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "failed to start run");
    } finally {
      setStarting(false);
    }
  }

  return (
    <form onSubmit={submit}>
      <div className="field">
        <label htmlFor="mode-select">Repair mode</label>
        <select id="mode-select" value={mode} onChange={(e) => setMode(e.target.value as RunMode)} disabled={disabled || starting}>
          <option value="ai-simulated">AI repair -- SIMULATED provider (no network call, no cost)</option>
          <option value="deterministic">Deterministic scripted repair (no AI at all)</option>
          <option value="ai-live" disabled={!liveModeEnabled}>
            {liveModeEnabled ? "AI repair -- LIVE provider (real, paid API call)" : "AI repair -- LIVE (disabled on this server)"}
          </option>
        </select>
      </div>
      {mode === "ai-live" && (
        <div className="field">
          <label>
            <input type="checkbox" checked={confirmed} onChange={(e) => setConfirmed(e.target.checked)} />
            {" I understand starting this will make a real Anthropic API call and incur real charges"}
          </label>
        </div>
      )}
      <button type="submit" disabled={disabled || starting || (mode === "ai-live" && !confirmed)}>
        {starting ? "Starting..." : "Start run"}
      </button>
      {error && (
        <p className="error-box" role="alert">
          {error}
        </p>
      )}
    </form>
  );
}

function RunList({ runs, selectedId, onSelect }: { runs: RunRow[]; selectedId: string | null; onSelect: (id: string) => void }): React.ReactElement {
  if (runs.length === 0) return <p className="empty">No runs yet -- start one above.</p>;
  return (
    <table>
      <thead>
        <tr>
          <th>Run</th>
          <th>Mode</th>
          <th>Status</th>
          <th>Updated</th>
        </tr>
      </thead>
      <tbody>
        {runs.map((r) => (
          <tr
            key={r.runId}
            className={r.runId === selectedId ? "selected" : ""}
            tabIndex={0}
            role="button"
            aria-pressed={r.runId === selectedId}
            onClick={() => onSelect(r.runId)}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") onSelect(r.runId);
            }}
          >
            <td className="mono">{r.runId.slice(0, 18)}</td>
            <td>
              <Badge mode={r.mode} />
            </td>
            <td>{r.status}</td>
            <td>{new Date(r.updatedAt).toLocaleTimeString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function AttemptsTable({ attempts }: { attempts?: RunRow["detail"]["attempts"] }): React.ReactElement | null {
  if (!attempts || attempts.length === 0) return null;
  return (
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Model</th>
          <th>Outcome</th>
          <th>Verification</th>
          <th>Tokens in/out</th>
        </tr>
      </thead>
      <tbody>
        {attempts.map((a, i) => (
          <tr key={i}>
            <td>{a.attempt}</td>
            <td className="mono">{a.modelUsed || "-"}</td>
            <td>{a.outcome}</td>
            <td>{a.verificationOutcome || "-"}</td>
            <td>{a.usage ? `${a.usage.inputTokens} / ${a.usage.outputTokens}` : "-"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function RunDetail({ run, onCancel, isActive }: { run: RunRow | null; onCancel: (runId: string) => void; isActive: boolean }): React.ReactElement {
  if (!run) return <p className="empty">Select a run to see its detail.</p>;
  const d = run.detail;
  return (
    <div>
      <div className="stage-row">
        {STAGES.map((s) => (
          <span key={s.key} className={`stage ${stageState(run, s.key)}`}>
            {s.label}
          </span>
        ))}
      </div>
      <p>
        Run: <span className="mono">{run.runId}</span> -- <Badge mode={run.mode} /> -- status: {run.status}
      </p>
      {isActive && (
        <button className="secondary" onClick={() => onCancel(run.runId)}>
          Cancel run
        </button>
      )}
      <AttemptsTable attempts={d.attempts} />
      {(d.baseCommit || d.resultCommit) && (
        <div className="field">
          <h2>Identity</h2>
          {d.baseCommit && <p className="mono">base: {d.baseCommit}</p>}
          {d.resultCommit && <p className="mono">result commit: {d.resultCommit}</p>}
          {d.resultTree && <p className="mono">result tree: {d.resultTree}</p>}
          {d.restoredCommit && (
            <p className="mono">
              worker B restored commit: {d.restoredCommit} {d.restoredCommit === d.resultCommit ? "(MATCHES)" : "(MISMATCH)"}
            </p>
          )}
          {d.acknowledged && <p>Restore acknowledgement: confirmed by durable backend state</p>}
        </div>
      )}
      {d.error && (
        <div className="error-box" role="alert">
          {d.error}
        </div>
      )}
    </div>
  );
}

export function App(): React.ReactElement {
  const [runs, setRuns] = useState<RunRow[]>([]);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [liveModeEnabled, setLiveModeEnabled] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const stoppedRef = useRef(false);

  useEffect(() => {
    getStatus()
      .then((s) => setLiveModeEnabled(s.liveModeEnabled))
      .catch(() => {});
  }, []);

  // Completion-based polling: the NEXT poll is scheduled only once the
  // current one has fully resolved (success or failure), via setTimeout
  // -- never setInterval, which would let a slow request overlap with
  // the next tick and risk a stale response landing after a newer one.
  useEffect(() => {
    stoppedRef.current = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    async function tick() {
      try {
        const result = await listRuns();
        if (stoppedRef.current) return;
        setRuns(result.runs);
        setActiveRunId(result.activeRunId);
        setLoadError(null);
        setLoaded(true);
      } catch (err) {
        if (!stoppedRef.current) setLoadError(err instanceof ApiError ? err.message : "could not reach the API");
      } finally {
        if (!stoppedRef.current) timer = setTimeout(tick, POLL_DELAY_MS);
      }
    }
    void tick();

    return () => {
      stoppedRef.current = true;
      if (timer) clearTimeout(timer);
    };
  }, []);

  const selectedRun = runs.find((r) => r.runId === selectedId) ?? null;

  const onCancel = useCallback(async (runId: string) => {
    try {
      await cancelRun(runId);
    } catch (err) {
      setLoadError(err instanceof ApiError ? err.message : "failed to cancel");
    }
  }, []);

  return (
    <React.Fragment>
      <section aria-live="polite">
        <span className={liveModeEnabled ? "badge live" : "badge simulated"}>{liveModeEnabled ? "LIVE MODE ENABLED on this server" : "live mode disabled on this server"}</span>
      </section>
      <section>
        <h2>Start a run</h2>
        <StartForm onStarted={setSelectedId} disabled={!!activeRunId} liveModeEnabled={liveModeEnabled} />
        {activeRunId && <p className="hint">A run is already active -- only one run is supported at a time in this demo.</p>}
      </section>
      <section>
        <h2>Runs</h2>
        {!loaded && !loadError && <p className="empty">Loading...</p>}
        {loadError && (
          <p className="error-box" role="alert">
            Could not reach the API: {loadError}
          </p>
        )}
        {loaded && <RunList runs={runs} selectedId={selectedId} onSelect={setSelectedId} />}
      </section>
      <section>
        <h2>Run detail</h2>
        <RunDetail run={selectedRun} onCancel={onCancel} isActive={!!selectedRun && selectedRun.runId === activeRunId} />
      </section>
    </React.Fragment>
  );
}
