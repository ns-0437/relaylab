// Demo UI milestone: plain mysql2 queries against the one small
// `demo_runs` table (infra/migrations/0013_demo_runs.sql). Deliberately
// NOT routed through TaskStore/Effect -- this is UI-facing lifecycle
// bookkeeping for the demo API itself, not a durable task-ownership
// primitive; the actual task ownership/fencing/checkpoint/handoff
// guarantees still come entirely from the existing TaskStore-backed
// pipeline this table merely records a summary of.
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";

export type RunMode = "deterministic" | "ai-simulated" | "ai-live";
export type RunStatus =
  | "starting"
  | "running"
  | "checkpointed"
  | "transferring"
  | "restoring"
  | "completed"
  | "failed"
  | "cancelled"
  | "interrupted";

export interface RunDetail {
  stage: "generation" | "verification" | "checkpoint" | "transfer" | "restore_ack";
  attempts?:
    | {
        attempt: number;
        modelUsed: string | null;
        outcome: string;
        verificationOutcome?: string | undefined;
        usage?: { inputTokens: number; outputTokens: number } | undefined;
        reason?: string | undefined;
      }[]
    | undefined;
  baseCommit?: string | undefined;
  resultCommit?: string | undefined;
  resultTree?: string | undefined;
  verificationOutcome?: string | undefined;
  checkpointDigest?: string | undefined;
  artifactDigest?: string | undefined;
  restoredCommit?: string | undefined;
  restoredTree?: string | undefined;
  restoreVerificationOutcome?: string | undefined;
  acknowledged?: boolean | undefined;
  /** Sanitized only -- never a raw SDK error body, prompt, or credential. See src/ai/anthropic-client.ts's own sanitization. */
  error?: string | undefined;
}

export interface RunRow {
  runId: string;
  taskId: string;
  mode: RunMode;
  status: RunStatus;
  ownerId: string;
  createdAt: string;
  updatedAt: string;
  detail: RunDetail;
}

function fromRow(row: RowDataPacket): RunRow {
  return {
    runId: row.run_id,
    taskId: row.task_id,
    mode: row.mode,
    status: row.status,
    ownerId: row.owner_id,
    createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : String(row.created_at),
    updatedAt: row.updated_at instanceof Date ? row.updated_at.toISOString() : String(row.updated_at),
    detail: typeof row.detail_json === "string" ? JSON.parse(row.detail_json) : row.detail_json,
  };
}

export async function insertRun(pool: mysql.Pool, run: { runId: string; taskId: string; mode: RunMode; status: RunStatus; ownerId: string; detail: RunDetail }): Promise<void> {
  await pool.query(`INSERT INTO demo_runs (run_id, task_id, mode, status, owner_id, detail_json) VALUES (?, ?, ?, ?, ?, ?)`, [
    run.runId,
    run.taskId,
    run.mode,
    run.status,
    run.ownerId,
    JSON.stringify(run.detail),
  ]);
}

export async function updateRunStatus(pool: mysql.Pool, runId: string, status: RunStatus, detailPatch: Partial<RunDetail>): Promise<void> {
  const [rows] = await pool.query<RowDataPacket[]>(`SELECT detail_json FROM demo_runs WHERE run_id = ?`, [runId]);
  const existing = rows[0] ? (typeof rows[0].detail_json === "string" ? JSON.parse(rows[0].detail_json) : rows[0].detail_json) : {};
  const merged = { ...existing, ...detailPatch };
  await pool.query(`UPDATE demo_runs SET status = ?, detail_json = ? WHERE run_id = ?`, [status, JSON.stringify(merged), runId]);
}

export async function getRun(pool: mysql.Pool, runId: string): Promise<RunRow | null> {
  const [rows] = await pool.query<RowDataPacket[]>(`SELECT * FROM demo_runs WHERE run_id = ?`, [runId]);
  return rows[0] ? fromRow(rows[0]) : null;
}

export async function listRuns(pool: mysql.Pool, limit = 50): Promise<RunRow[]> {
  const [rows] = await pool.query<RowDataPacket[]>(`SELECT * FROM demo_runs ORDER BY created_at DESC LIMIT ?`, [limit]);
  return rows.map(fromRow);
}

/**
 * Called once at server startup, BEFORE accepting any request. Any row
 * still in an in-flight status necessarily belonged to a PRIOR process
 * (this process starts with no active run at all) -- there is no way
 * for an in-memory JS execution to have survived the restart, so such a
 * row is reclassified as "interrupted" rather than ever being displayed
 * as "running" again.
 */
export async function reconcileOnStartup(pool: mysql.Pool): Promise<number> {
  const [result] = await pool.query(`UPDATE demo_runs SET status = 'interrupted' WHERE status IN ('starting','running','checkpointed','transferring','restoring')`);
  return (result as { affectedRows: number }).affectedRows;
}
