// Phase 1.2.1 patch, Part A.2: "a basic inspection command; a UI is
// unnecessary." Lists recent consumer_quarantine rows, newest first.
// `payload_excerpt` (raw application bytes, up to 2000 bytes -- see
// infra/migrations/0004_consumer_quarantine.sql's data-handling policy
// comment) is printed here deliberately: this is an operator-invoked
// inspection tool reading directly from the database, not an
// application log line or a metric -- the distinction this patch's
// redaction policy actually cares about (see docs/OBSERVABILITY.md
// "dead-letter quarantine"). Usage:
//   npx tsx src/db/quarantine-cli.ts [consumerName] [limit]
import { createPool } from "./pool.js";
import { loadMysqlConfig } from "../config.js";
import type { RowDataPacket } from "mysql2/promise";

interface QuarantineRow extends RowDataPacket {
  consumer_name: string;
  nats_stream_seq: number;
  reason: string;
  event_id: string | null;
  task_id: string | null;
  delivery_attempt: number;
  payload_excerpt: Buffer | null;
  payload_truncated: number;
  quarantined_at: string;
}

async function main() {
  const consumerNameFilter = process.argv[2];
  const limit = Number(process.argv[3] ?? 20);

  const pool = createPool(loadMysqlConfig());
  try {
    const [rows] = consumerNameFilter
      ? await pool.query<QuarantineRow[]>(
          `SELECT * FROM consumer_quarantine WHERE consumer_name = ? ORDER BY quarantined_at DESC LIMIT ?`,
          [consumerNameFilter, limit],
        )
      : await pool.query<QuarantineRow[]>(`SELECT * FROM consumer_quarantine ORDER BY quarantined_at DESC LIMIT ?`, [limit]);

    if (rows.length === 0) {
      console.log("No quarantined messages.");
      return;
    }

    for (const row of rows) {
      console.log("---");
      console.log(`consumer_name:     ${row.consumer_name}`);
      console.log(`nats_stream_seq:   ${row.nats_stream_seq}`);
      console.log(`reason:            ${row.reason}`);
      console.log(`event_id:          ${row.event_id ?? "(unknown -- not decodable)"}`);
      console.log(`task_id:           ${row.task_id ?? "(unknown -- not decodable)"}`);
      console.log(`delivery_attempt:  ${row.delivery_attempt}`);
      console.log(`quarantined_at:    ${row.quarantined_at}`);
      console.log(
        `payload_excerpt:   ${row.payload_excerpt ? row.payload_excerpt.toString("utf8") : "(none)"}` +
          (row.payload_truncated ? " ...[truncated at 2000 bytes]" : ""),
      );
    }
    console.log(`---\n${rows.length} row(s) shown (limit ${limit}).`);
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error("[quarantine-cli] fatal:", err instanceof Error ? err.message : err);
  process.exitCode = 1;
});
