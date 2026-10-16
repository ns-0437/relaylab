// Demo UI milestone: plain React (no JSX, no build step -- React is
// loaded as a UMD script in index.html) talking to the local API via
// fetch + simple polling (2s). No WebSockets. Every "success" state
// shown here is read directly from GET /api/runs|/api/runs/:id, i.e.
// durable backend state (demo_runs), never a local timer pretending a
// stage finished.
const h = React.createElement;
const POLL_MS = 2000;

const STAGES = [
  { key: "generation", label: "Generation" },
  { key: "verification", label: "Verification" },
  { key: "checkpoint", label: "Checkpoint" },
  { key: "transfer", label: "Transfer" },
  { key: "restore_ack", label: "Restore / Ack" },
];

function stageState(run, stageKey) {
  if (!run) return "pending";
  const order = STAGES.map((s) => s.key);
  const currentIdx = order.indexOf(run.detail.stage);
  const stageIdx = order.indexOf(stageKey);
  const terminal = new Set(["failed", "cancelled", "interrupted"]);
  if (terminal.has(run.status)) {
    if (stageIdx < currentIdx) return "done";
    if (stageIdx === currentIdx) return "failed";
    return "pending";
  }
  if (run.status === "completed") return "done";
  if (stageIdx < currentIdx) return "done";
  if (stageIdx === currentIdx) return "active";
  return "pending";
}

async function api(path, options) {
  const res = await fetch(path, options);
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const message = body && body.error ? body.error.message : `HTTP ${res.status}`;
    throw new Error(message);
  }
  return body;
}

function Badge({ mode }) {
  return h("span", { className: `badge ${mode === "ai-live" ? "live" : mode === "ai-simulated" ? "simulated" : "deterministic"}` }, mode === "ai-live" ? "LIVE (real API call)" : mode === "ai-simulated" ? "SIMULATED provider" : "deterministic (no AI)");
}

function StartForm({ onStarted, disabled, liveModeEnabled }) {
  const [mode, setMode] = React.useState("ai-simulated");
  const [confirmed, setConfirmed] = React.useState(false);
  const [error, setError] = React.useState(null);
  const [starting, setStarting] = React.useState(false);

  async function submit(e) {
    e.preventDefault();
    setError(null);
    setStarting(true);
    try {
      const body = { mode };
      if (mode === "ai-live") body.confirmPaidCall = confirmed;
      const result = await api("/api/runs", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
      onStarted(result.runId);
    } catch (err) {
      setError(err.message);
    } finally {
      setStarting(false);
    }
  }

  return h(
    "form",
    { onSubmit: submit },
    h(
      "div",
      { className: "field" },
      h("label", { htmlFor: "mode-select" }, "Repair mode"),
      h(
        "select",
        { id: "mode-select", value: mode, onChange: (e) => setMode(e.target.value), disabled: disabled || starting },
        h("option", { value: "ai-simulated" }, "AI repair -- SIMULATED provider (no network call, no cost)"),
        h("option", { value: "deterministic" }, "Deterministic scripted repair (no AI at all)"),
        h("option", { value: "ai-live", disabled: !liveModeEnabled }, liveModeEnabled ? "AI repair -- LIVE provider (real, paid API call)" : "AI repair -- LIVE (disabled on this server)"),
      ),
    ),
    mode === "ai-live" &&
      h(
        "div",
        { className: "field" },
        h(
          "label",
          null,
          h("input", { type: "checkbox", checked: confirmed, onChange: (e) => setConfirmed(e.target.checked) }),
          " I understand starting this will make a real Anthropic API call and incur real charges",
        ),
      ),
    h("button", { type: "submit", disabled: disabled || starting || (mode === "ai-live" && !confirmed) }, starting ? "Starting..." : "Start run"),
    error && h("p", { className: "error-box", role: "alert" }, error),
  );
}

function RunList({ runs, selectedId, onSelect }) {
  if (runs.length === 0) return h("p", { className: "empty" }, "No runs yet -- start one above.");
  return h(
    "table",
    null,
    h(
      "thead",
      null,
      h("tr", null, h("th", null, "Run"), h("th", null, "Mode"), h("th", null, "Status"), h("th", null, "Updated")),
    ),
    h(
      "tbody",
      null,
      runs.map((r) =>
        h(
          "tr",
          {
            key: r.runId,
            className: r.runId === selectedId ? "selected" : "",
            tabIndex: 0,
            role: "button",
            "aria-pressed": r.runId === selectedId,
            onClick: () => onSelect(r.runId),
            onKeyDown: (e) => {
              if (e.key === "Enter" || e.key === " ") onSelect(r.runId);
            },
          },
          h("td", { className: "mono" }, r.runId.slice(0, 18)),
          h("td", null, h(Badge, { mode: r.mode })),
          h("td", null, r.status),
          h("td", null, new Date(r.updatedAt).toLocaleTimeString()),
        ),
      ),
    ),
  );
}

function AttemptsTable({ attempts }) {
  if (!attempts || attempts.length === 0) return null;
  return h(
    "table",
    null,
    h("thead", null, h("tr", null, h("th", null, "#"), h("th", null, "Model"), h("th", null, "Outcome"), h("th", null, "Verification"), h("th", null, "Tokens in/out"))),
    h(
      "tbody",
      null,
      attempts.map((a, i) =>
        h(
          "tr",
          { key: i },
          h("td", null, a.attempt),
          h("td", { className: "mono" }, a.modelUsed || "-"),
          h("td", null, a.outcome),
          h("td", null, a.verificationOutcome || "-"),
          h("td", null, a.usage ? `${a.usage.inputTokens} / ${a.usage.outputTokens}` : "-"),
        ),
      ),
    ),
  );
}

function RunDetail({ run, onCancel, isActive }) {
  if (!run) return h("p", { className: "empty" }, "Select a run to see its detail.");
  const d = run.detail || {};
  return h(
    "div",
    null,
    h(
      "div",
      { className: "stage-row" },
      STAGES.map((s) => h("span", { key: s.key, className: `stage ${stageState(run, s.key)}` }, s.label)),
    ),
    h("p", null, "Run: ", h("span", { className: "mono" }, run.runId), " -- ", h(Badge, { mode: run.mode }), " -- status: ", run.status),
    isActive && h("button", { className: "secondary", onClick: () => onCancel(run.runId) }, "Cancel run"),
    h(AttemptsTable, { attempts: d.attempts }),
    (d.baseCommit || d.resultCommit) &&
      h(
        "div",
        { className: "field" },
        h("h2", null, "Identity"),
        d.baseCommit && h("p", { className: "mono" }, "base: ", d.baseCommit),
        d.resultCommit && h("p", { className: "mono" }, "result commit: ", d.resultCommit),
        d.resultTree && h("p", { className: "mono" }, "result tree: ", d.resultTree),
        d.restoredCommit && h("p", { className: "mono" }, "worker B restored commit: ", d.restoredCommit, d.restoredCommit === d.resultCommit ? " (MATCHES)" : " (MISMATCH)"),
        d.acknowledged && h("p", null, "Restore acknowledgement: confirmed by durable backend state"),
      ),
    d.error && h("div", { className: "error-box", role: "alert" }, d.error),
  );
}

function App() {
  const [runs, setRuns] = React.useState([]);
  const [activeRunId, setActiveRunId] = React.useState(null);
  const [selectedId, setSelectedId] = React.useState(null);
  const [liveModeEnabled, setLiveModeEnabled] = React.useState(false);
  const [loadError, setLoadError] = React.useState(null);
  const [loaded, setLoaded] = React.useState(false);

  React.useEffect(() => {
    api("/api/status")
      .then((s) => {
        setLiveModeEnabled(s.liveModeEnabled);
        const badge = document.getElementById("live-mode-badge");
        if (badge) {
          badge.textContent = s.liveModeEnabled ? "LIVE MODE ENABLED on this server" : "live mode disabled on this server";
          badge.className = s.liveModeEnabled ? "badge live" : "badge simulated";
        }
      })
      .catch(() => {});
  }, []);

  React.useEffect(() => {
    let cancelled = false;
    async function poll() {
      try {
        const { runs, activeRunId } = await api("/api/runs");
        if (cancelled) return;
        setRuns(runs);
        setActiveRunId(activeRunId);
        setLoadError(null);
        setLoaded(true);
      } catch (err) {
        if (!cancelled) setLoadError(err.message);
      }
    }
    poll();
    const id = setInterval(poll, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const selectedRun = runs.find((r) => r.runId === selectedId) || null;

  async function onCancel(runId) {
    try {
      await api(`/api/runs/${encodeURIComponent(runId)}/cancel`, { method: "POST" });
    } catch (err) {
      setLoadError(err.message);
    }
  }

  return h(
    React.Fragment,
    null,
    h(
      "section",
      null,
      h("h2", null, "Start a run"),
      h(StartForm, { onStarted: setSelectedId, disabled: !!activeRunId, liveModeEnabled }),
      activeRunId && h("p", { className: "hint" }, "A run is already active -- only one run is supported at a time in this demo."),
    ),
    h(
      "section",
      null,
      h("h2", null, "Runs"),
      !loaded && !loadError && h("p", { className: "empty" }, "Loading..."),
      loadError && h("p", { className: "error-box", role: "alert" }, "Could not reach the API: ", loadError),
      loaded && h(RunList, { runs, selectedId, onSelect: setSelectedId }),
    ),
    h(
      "section",
      null,
      h("h2", null, "Run detail"),
      h(RunDetail, { run: selectedRun, onCancel, isActive: selectedRun && selectedRun.runId === activeRunId }),
    ),
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(h(App));
