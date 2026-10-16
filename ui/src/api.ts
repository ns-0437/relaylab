// Demo UI milestone: the typed fetch wrapper the UI uses to talk to the
// local API. Every "success" value returned here comes directly from a
// GET response body -- the UI never derives a stage/status from a local
// timer.
export interface AttemptView {
  attempt: number;
  modelUsed: string | null;
  outcome: string;
  verificationOutcome?: string;
  usage?: { inputTokens: number; outputTokens: number };
  reason?: string;
}

export interface RunDetailView {
  stage: "generation" | "verification" | "checkpoint" | "transfer" | "restore_ack";
  attempts?: AttemptView[];
  baseCommit?: string;
  resultCommit?: string;
  resultTree?: string;
  verificationOutcome?: string;
  checkpointDigest?: string;
  artifactDigest?: string;
  restoredCommit?: string;
  restoredTree?: string;
  restoreVerificationOutcome?: string;
  acknowledged?: boolean;
  error?: string;
}

export type RunMode = "deterministic" | "ai-simulated" | "ai-live";

export interface RunRow {
  runId: string;
  taskId: string;
  mode: RunMode;
  status: string;
  ownerId: string;
  createdAt: string;
  updatedAt: string;
  detail: RunDetailView;
}

export interface ApiErrorBody {
  error: { code: string; message: string };
}

export class ApiError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  const body = (await res.json().catch(() => ({}))) as T & Partial<ApiErrorBody>;
  if (!res.ok) {
    const err = body as Partial<ApiErrorBody>;
    throw new ApiError(err.error?.code ?? "unknown_error", err.error?.message ?? `HTTP ${res.status}`);
  }
  return body as T;
}

export function getStatus(): Promise<{ liveModeEnabled: boolean; allowedModes: RunMode[]; lockHeld: boolean }> {
  return request("/api/status");
}

export function listRuns(): Promise<{ runs: RunRow[]; activeRunId: string | null }> {
  return request("/api/runs");
}

export function getRun(runId: string): Promise<{ run: RunRow }> {
  return request(`/api/runs/${encodeURIComponent(runId)}`);
}

export function startRun(mode: RunMode, confirmPaidCall?: boolean): Promise<{ runId: string }> {
  const payload: { mode: RunMode; confirmPaidCall?: boolean } = { mode };
  if (confirmPaidCall !== undefined) payload.confirmPaidCall = confirmPaidCall;
  return request("/api/runs", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(payload) });
}

export function cancelRun(runId: string): Promise<{ cancelling: true }> {
  return request(`/api/runs/${encodeURIComponent(runId)}/cancel`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" });
}
